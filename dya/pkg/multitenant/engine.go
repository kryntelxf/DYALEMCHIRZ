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

package multitenant

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	tenants     []Tenant
	quotas      []Quota
	policies    []Policy
	validators  []Validator
	running     bool
}

type Tenant struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Namespace   string    `json:"namespace"`
	Status      string    `json:"status"` // active, suspended, deleted
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Quota struct {
	TenantID    string            `json:"tenantId"`
	Resource    string            `json:"resource"`
	Limit       int64             `json:"limit"`
	Used        int64             `json:"used"`
	CreatedAt   time.Time         `json:"createdAt"`
}

type Policy struct {
	TenantID    string                 `json:"tenantId"`
	Name        string                 `json:"name"`
	Rules       map[string]interface{} `json:"rules"`
	CreatedAt   time.Time              `json:"createdAt"`
}

type Validator interface {
	Validate(tenant *Tenant) (*ValidationResult, error)
	Name() string
}

type ValidationResult struct {
	TenantID string   `json:"tenantId"`
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

func NewEngine() *Engine {
	return &Engine{
		tenants:    make([]Tenant, 0),
		quotas:     make([]Quota, 0),
		policies:   make([]Policy, 0),
		validators: make([]Validator, 0),
		running:    false,
	}
}

func (e *Engine) RegisterTenant(tenant Tenant) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tenants = append(e.tenants, tenant)
	klog.Infof("Registered tenant: %s", tenant.Name)
}

func (e *Engine) RegisterQuota(quota Quota) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.quotas = append(e.quotas, quota)
	klog.Infof("Registered quota for tenant: %s", quota.TenantID)
}

func (e *Engine) RegisterPolicy(policy Policy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies = append(e.policies, policy)
	klog.Infof("Registered policy for tenant: %s", policy.TenantID)
}

func (e *Engine) RegisterValidator(validator Validator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.validators = append(e.validators, validator)
	klog.Infof("Registered validator: %s", validator.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Multi-Tenant Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Multi-Tenant Engine stopped")
}

func (e *Engine) GetTenants() []Tenant {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tenants
}

func (e *Engine) GetTenant(id string) *Tenant {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, t := range e.tenants {
		if t.ID == id {
			return &t
		}
	}
	return nil
}

func (e *Engine) GetQuotas(tenantID string) []Quota {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]Quota, 0)
	for _, q := range e.quotas {
		if q.TenantID == tenantID {
			results = append(results, q)
		}
	}
	return results
}

func (e *Engine) ValidateTenant(tenant *Tenant) []*ValidationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*ValidationResult, 0)
	for _, validator := range e.validators {
		result, err := validator.Validate(tenant)
		if err != nil {
			klog.Errorf("Validator %s failed: %v", validator.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
