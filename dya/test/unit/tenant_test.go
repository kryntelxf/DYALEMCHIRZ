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

package unit

import (
	"testing"

	"k8s.io/kubernetes/dya/pkg/enterprise"
)

func TestTenantManager(t *testing.T) {
	manager := enterprise.NewTenantManager()
	
	// Test creating tenant
	quota := &enterprise.Quota{
		MaxNodes:  50,
		MaxAssets: 500,
	}
	
	tenant, err := manager.CreateTenant("test-tenant", "Test Tenant", "Testing", quota)
	if err != nil {
		t.Fatalf("Failed to create tenant: %v", err)
	}
	
	if tenant.ID != "test-tenant" {
		t.Errorf("Expected tenant ID 'test-tenant', got '%s'", tenant.ID)
	}
	
	if tenant.Quota.MaxNodes != 50 {
		t.Errorf("Expected max nodes 50, got %d", tenant.Quota.MaxNodes)
	}
	
	// Test duplicate tenant
	_, err = manager.CreateTenant("test-tenant", "Duplicate", "", nil)
	if err == nil {
		t.Error("Expected error when creating duplicate tenant")
	}
	
	// Test adding resources
	if err := manager.AddResourceUsage("test-tenant", "node-1", "node"); err != nil {
		t.Errorf("Failed to add resource: %v", err)
	}
	
	usage, err := manager.GetTenantUsage("test-tenant")
	if err != nil {
		t.Errorf("Failed to get usage: %v", err)
	}
	
	if usage.Nodes != 1 {
		t.Errorf("Expected nodes 1, got %d", usage.Nodes)
	}
	
	// Test quota enforcement
	for i := 0; i < 50; i++ {
		if err := manager.AddResourceUsage("test-tenant", "node-extra", "node"); err != nil {
			// Should fail after 50
			break
		}
	}
	
	usage, _ = manager.GetTenantUsage("test-tenant")
	if usage.Nodes > 50 {
		t.Errorf("Expected nodes <= 50, got %d", usage.Nodes)
	}
	
	// Test deleting tenant
	if err := manager.DeleteTenant("test-tenant"); err != nil {
		t.Errorf("Failed to delete tenant: %v", err)
	}
	
	_, ok := manager.GetTenant("test-tenant")
	if ok {
		t.Error("Tenant should not exist after deletion")
	}
}

func TestQuotaValidation(t *testing.T) {
	manager := enterprise.NewTenantManager()
	
	quota := &enterprise.Quota{
		MaxNodes:       10,
		MaxAssets:      20,
		MaxEvents:      30,
		MaxAIProcesses: 5,
	}
	
	tenant, err := manager.CreateTenant("quota-test", "Quota Test", "", quota)
	if err != nil {
		t.Fatalf("Failed to create tenant: %v", err)
	}
	
	// Test can add resource
	if !manager.CanAddResource("quota-test", "node") {
		t.Error("Should be able to add node")
	}
	
	// Fill up nodes
	for i := 0; i < 10; i++ {
		manager.AddResourceUsage("quota-test", "node-test", "node")
	}
	
	if manager.CanAddResource("quota-test", "node") {
		t.Error("Should not be able to add more nodes")
	}
	
	// Test different resource types
	if !manager.CanAddResource("quota-test", "asset") {
		t.Error("Should be able to add asset")
	}
	
	for i := 0; i < 20; i++ {
		manager.AddResourceUsage("quota-test", "asset-test", "asset")
	}
	
	if manager.CanAddResource("quota-test", "asset") {
		t.Error("Should not be able to add more assets")
	}
	
	// Cleanup
	manager.DeleteTenant("quota-test")
}
