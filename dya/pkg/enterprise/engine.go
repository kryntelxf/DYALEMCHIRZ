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
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu           sync.RWMutex
	tenants      []Tenant
	roles        []Role
	organizations []Organization
	auditors     []Auditor
	apiHandlers  []APIHandler
	running      bool
}

type Tenant struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Role struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Organization struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	TenantID    string    `json:"tenantId"`
	Members     []string  `json:"members"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Auditor interface {
	Audit(event interface{}) error
	Name() string
}

type APIHandler interface {
	Handle(request interface{}) (*APIResponse, error)
	Name() string
}

type APIResponse struct {
	Success   bool        `json:"success"`
	Data      interface{} `json:"data"`
	Error     string      `json:"error"`
	Timestamp time.Time   `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		tenants:      make([]Tenant, 0),
		roles:        make([]Role, 0),
		organizations: make([]Organization, 0),
		auditors:     make([]Auditor, 0),
		apiHandlers:  make([]APIHandler, 0),
		running:      false,
	}
}

func (e *Engine) RegisterTenant(tenant Tenant) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tenants = append(e.tenants, tenant)
	klog.Infof("Registered tenant: %s", tenant.Name)
}

func (e *Engine) RegisterRole(role Role) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.roles = append(e.roles, role)
	klog.Infof("Registered role: %s", role.Name)
}

func (e *Engine) RegisterOrganization(org Organization) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.organizations = append(e.organizations, org)
	klog.Infof("Registered organization: %s", org.Name)
}

func (e *Engine) RegisterAuditor(auditor Auditor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.auditors = append(e.auditors, auditor)
	klog.Infof("Registered enterprise auditor: %s", auditor.Name())
}

func (e *Engine) RegisterAPIHandler(handler APIHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.apiHandlers = append(e.apiHandlers, handler)
	klog.Infof("Registered API handler: %s", handler.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Enterprise Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Enterprise Engine stopped")
}

func (e *Engine) GetTenants() []Tenant {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tenants
}

func (e *Engine) GetRoles() []Role {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.roles
}

func (e *Engine) GetOrganizations() []Organization {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.organizations
}

func (e *Engine) Audit(event interface{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, auditor := range e.auditors {
		if err := auditor.Audit(event); err != nil {
			klog.Errorf("Enterprise auditor %s failed: %v", auditor.Name(), err)
		}
	}
}

func (e *Engine) HandleAPI(request interface{}) []*APIResponse {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*APIResponse, 0)
	for _, handler := range e.apiHandlers {
		resp, err := handler.Handle(request)
		if err != nil {
			klog.Errorf("API handler %s failed: %v", handler.Name(), err)
			continue
		}
		if resp != nil {
			results = append(results, resp)
		}
	}
	return results
}
