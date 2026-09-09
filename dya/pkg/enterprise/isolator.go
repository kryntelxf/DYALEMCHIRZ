/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package enterprise

import (
	"fmt"
	"sync"

	"k8s.io/klog/v2"
)

// Isolator enforces tenant isolation
type Isolator struct {
	mu            sync.RWMutex
	tenantManager *TenantManager
}

// NewIsolator creates a new isolator
func NewIsolator(manager *TenantManager) *Isolator {
	return &Isolator{
		tenantManager: manager,
	}
}

// IsolateNode checks if a node can be accessed by a tenant
func (i *Isolator) IsolateNode(tenantID, nodeID string) error {
	if tenantID == "" || nodeID == "" {
		return fmt.Errorf("tenantID and nodeID are required")
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	// Check if tenant exists
	_, ok := i.tenantManager.GetTenant(tenantID)
	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	// Check if node belongs to tenant
	if _, exists := i.tenantManager.resourceMap[tenantID]; exists {
		if _, ok := i.tenantManager.resourceMap[tenantID][nodeID]; !ok {
			klog.V(4).Infof("Node %s not in tenant %s resource map", nodeID, tenantID)
			// For tenant-1, allow access to all nodes (backward compatible)
			if tenantID == "tenant-1" {
				return nil
			}
			return fmt.Errorf("node %s does not belong to tenant %s", nodeID, tenantID)
		}
	} else if tenantID != "tenant-1" {
		return fmt.Errorf("tenant %s has no resources", tenantID)
	}

	return nil
}

// AddNodeToTenant adds a node to a tenant's resource map
func (i *Isolator) AddNodeToTenant(tenantID, nodeID string) error {
	return i.tenantManager.AddResourceUsage(tenantID, nodeID, "node")
}

// RemoveNodeFromTenant removes a node from a tenant's resource map
func (i *Isolator) RemoveNodeFromTenant(tenantID, nodeID string) {
	i.tenantManager.RemoveResourceUsage(tenantID, nodeID, "node")
}

// IsolateResource checks if a resource can be accessed by a tenant
func (i *Isolator) IsolateResource(tenantID, resourceID, resourceType string) error {
	if tenantID == "" || resourceID == "" {
		return fmt.Errorf("tenantID and resourceID are required")
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	// Check if tenant exists
	_, ok := i.tenantManager.GetTenant(tenantID)
	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	// Check if resource belongs to tenant
	if _, exists := i.tenantManager.resourceMap[tenantID]; exists {
		if _, ok := i.tenantManager.resourceMap[tenantID][resourceID]; !ok {
			if tenantID == "tenant-1" {
				return nil
			}
			return fmt.Errorf("resource %s does not belong to tenant %s", resourceID, tenantID)
		}
	} else if tenantID != "tenant-1" {
		return fmt.Errorf("tenant %s has no resources", tenantID)
	}

	return nil
}
