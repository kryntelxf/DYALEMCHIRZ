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
	"time"

	"k8s.io/klog/v2"
)

// Quota represents resource limits
type Quota struct {
	MaxNodes       int `json:"maxNodes"`
	MaxAssets      int `json:"maxAssets"`
	MaxEvents      int `json:"maxEvents"`
	MaxAIProcesses int `json:"maxAIProcesses"`
}

// QuotaUsed represents current usage
type QuotaUsed struct {
	Nodes       int `json:"nodes"`
	Assets      int `json:"assets"`
	Events      int `json:"events"`
	AIProcesses int `json:"aiProcesses"`
}

// TenantWithQuota extends Tenant with quota
type TenantWithQuota struct {
	Tenant
	Quota     *Quota     `json:"quota"`
	Used      *QuotaUsed `json:"used"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

// TenantManager manages tenants
type TenantManager struct {
	mu          sync.RWMutex
	tenants     map[string]*TenantWithQuota
	ResourceMap map[string]map[string]bool `json:"resourceMap"`
}

// NewTenantManager creates a new tenant manager
func NewTenantManager() *TenantManager {
	return &TenantManager{
		tenants:     make(map[string]*TenantWithQuota),
		ResourceMap: make(map[string]map[string]bool),
	}
}

// CreateTenant creates a new tenant
func (tm *TenantManager) CreateTenant(id, name, description string, quota *Quota) (*TenantWithQuota, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tenants[id]; exists {
		return nil, fmt.Errorf("tenant %s already exists", id)
	}

	if quota == nil {
		quota = &Quota{
			MaxNodes:       100,
			MaxAssets:      1000,
			MaxEvents:      10000,
			MaxAIProcesses: 10,
		}
	}

	tenant := &TenantWithQuota{
		Tenant: Tenant{
			ID:          id,
			Name:        name,
			Description: description,
		},
		Quota: quota,
		Used: &QuotaUsed{
			Nodes:       0,
			Assets:      0,
			Events:      0,
			AIProcesses: 0,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	tm.tenants[id] = tenant
	tm.ResourceMap[id] = make(map[string]bool)

	klog.Infof("Created tenant: %s (ID: %s)", name, id)
	return tenant, nil
}

// GetTenant retrieves a tenant
func (tm *TenantManager) GetTenant(id string) (*TenantWithQuota, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tenant, ok := tm.tenants[id]
	return tenant, ok
}

// GetAllTenants returns all tenants
func (tm *TenantManager) GetAllTenants() []*TenantWithQuota {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make([]*TenantWithQuota, 0, len(tm.tenants))
	for _, t := range tm.tenants {
		result = append(result, t)
	}
	return result
}

// DeleteTenant deletes a tenant
func (tm *TenantManager) DeleteTenant(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tenants[id]; !exists {
		return fmt.Errorf("tenant %s not found", id)
	}

	delete(tm.tenants, id)
	delete(tm.ResourceMap, id)

	klog.Infof("Deleted tenant: %s", id)
	return nil
}

// CanAddResource checks if tenant can add resource
func (tm *TenantManager) CanAddResource(tenantID, resourceType string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return false
	}

	switch resourceType {
	case "node":
		return tenant.Used.Nodes < tenant.Quota.MaxNodes
	case "asset":
		return tenant.Used.Assets < tenant.Quota.MaxAssets
	case "event":
		return tenant.Used.Events < tenant.Quota.MaxEvents
	case "ai":
		return tenant.Used.AIProcesses < tenant.Quota.MaxAIProcesses
	default:
		return true
	}
}

// AddResourceUsage adds resource usage
func (tm *TenantManager) AddResourceUsage(tenantID, resourceID, resourceType string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	if !tm.canAddResourceLocked(tenantID, resourceType) {
		return fmt.Errorf("quota exceeded for %s", resourceType)
	}

	if tm.ResourceMap[tenantID] == nil {
		tm.ResourceMap[tenantID] = make(map[string]bool)
	}
	tm.ResourceMap[tenantID][resourceID] = true

	switch resourceType {
	case "node":
		tenant.Used.Nodes++
	case "asset":
		tenant.Used.Assets++
	case "event":
		tenant.Used.Events++
	case "ai":
		tenant.Used.AIProcesses++
	}

	tenant.UpdatedAt = time.Now()
	return nil
}

// RemoveResourceUsage removes resource usage
func (tm *TenantManager) RemoveResourceUsage(tenantID, resourceID, resourceType string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return
	}

	if tm.ResourceMap[tenantID] != nil {
		delete(tm.ResourceMap[tenantID], resourceID)
	}

	switch resourceType {
	case "node":
		if tenant.Used.Nodes > 0 {
			tenant.Used.Nodes--
		}
	case "asset":
		if tenant.Used.Assets > 0 {
			tenant.Used.Assets--
		}
	case "event":
		if tenant.Used.Events > 0 {
			tenant.Used.Events--
		}
	case "ai":
		if tenant.Used.AIProcesses > 0 {
			tenant.Used.AIProcesses--
		}
	}

	tenant.UpdatedAt = time.Now()
}

func (tm *TenantManager) canAddResourceLocked(tenantID, resourceType string) bool {
	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return false
	}

	switch resourceType {
	case "node":
		return tenant.Used.Nodes < tenant.Quota.MaxNodes
	case "asset":
		return tenant.Used.Assets < tenant.Quota.MaxAssets
	case "event":
		return tenant.Used.Events < tenant.Quota.MaxEvents
	case "ai":
		return tenant.Used.AIProcesses < tenant.Quota.MaxAIProcesses
	default:
		return true
	}
}

// GetTenantUsage returns usage
func (tm *TenantManager) GetTenantUsage(tenantID string) (*QuotaUsed, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("tenant %s not found", tenantID)
	}

	return &QuotaUsed{
		Nodes:       tenant.Used.Nodes,
		Assets:      tenant.Used.Assets,
		Events:      tenant.Used.Events,
		AIProcesses: tenant.Used.AIProcesses,
	}, nil
}
