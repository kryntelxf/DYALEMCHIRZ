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

package commercial

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu                   sync.RWMutex
	licenses             []License
	supportPlans         []SupportPlan
	managedServices      []ManagedService
	professionalServices []ProfessionalService
	partners             []Partner
	commercialAPIs       []CommercialAPI
	running              bool
}

type License struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Features    []string  `json:"features"`
	Price       string    `json:"price"`
	CreatedAt   time.Time `json:"createdAt"`
}

type SupportPlan struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Level        string    `json:"level"`
	ResponseTime string    `json:"responseTime"`
	Hours        string    `json:"hours"`
	CreatedAt    time.Time `json:"createdAt"`
}

type ManagedService struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Price       string    `json:"price"`
	CreatedAt   time.Time `json:"createdAt"`
}

type ProfessionalService struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Description string    `json:"description"`
	Price       string    `json:"price"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Partner struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Website     string    `json:"website"`
	CreatedAt   time.Time `json:"createdAt"`
}

type CommercialAPI struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Endpoint    string    `json:"endpoint"`
	Description string    `json:"description"`
	Pricing     string    `json:"pricing"`
	CreatedAt   time.Time `json:"createdAt"`
}

func NewEngine() *Engine {
	return &Engine{
		licenses:             make([]License, 0),
		supportPlans:         make([]SupportPlan, 0),
		managedServices:      make([]ManagedService, 0),
		professionalServices: make([]ProfessionalService, 0),
		partners:             make([]Partner, 0),
		commercialAPIs:       make([]CommercialAPI, 0),
		running:              false,
	}
}

func (e *Engine) RegisterLicense(license License) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.licenses = append(e.licenses, license)
	klog.Infof("Registered commercial license: %s", license.Name)
}

func (e *Engine) RegisterSupportPlan(plan SupportPlan) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.supportPlans = append(e.supportPlans, plan)
	klog.Infof("Registered support plan: %s", plan.Name)
}

func (e *Engine) RegisterManagedService(service ManagedService) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.managedServices = append(e.managedServices, service)
	klog.Infof("Registered managed service: %s", service.Name)
}

func (e *Engine) RegisterProfessionalService(service ProfessionalService) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.professionalServices = append(e.professionalServices, service)
	klog.Infof("Registered professional service: %s", service.Name)
}

func (e *Engine) RegisterPartner(partner Partner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.partners = append(e.partners, partner)
	klog.Infof("Registered commercial partner: %s", partner.Name)
}

func (e *Engine) RegisterCommercialAPI(api CommercialAPI) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.commercialAPIs = append(e.commercialAPIs, api)
	klog.Infof("Registered commercial API: %s", api.Name)
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Commercial Platform Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Commercial Platform Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) GetLicenses() []License {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.licenses
}

func (e *Engine) GetSupportPlans() []SupportPlan {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.supportPlans
}

func (e *Engine) GetManagedServices() []ManagedService {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.managedServices
}

func (e *Engine) GetProfessionalServices() []ProfessionalService {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.professionalServices
}

func (e *Engine) GetPartners() []Partner {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.partners
}

func (e *Engine) GetCommercialAPIs() []CommercialAPI {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.commercialAPIs
}
