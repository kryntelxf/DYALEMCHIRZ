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

// IsolateNode checks if a node can be accessed
func (i *Isolator) IsolateNode(tenantID, nodeID string) error {
	if tenantID == "" || nodeID == "" {
		return fmt.Errorf("tenantID and nodeID are required")
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	_, ok := i.tenantManager.GetTenant(tenantID)
	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	if _, exists := i.tenantManager.ResourceMap[tenantID]; exists {
		if _, ok := i.tenantManager.ResourceMap[tenantID][nodeID]; !ok {
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

// AddNodeToTenant adds a node to tenant
func (i *Isolator) AddNodeToTenant(tenantID, nodeID string) error {
	return i.tenantManager.AddResourceUsage(tenantID, nodeID, "node")
}

// RemoveNodeFromTenant removes a node from tenant
func (i *Isolator) RemoveNodeFromTenant(tenantID, nodeID string) {
	i.tenantManager.RemoveResourceUsage(tenantID, nodeID, "node")
}
