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

package ecosystem

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu            sync.RWMutex
	sdks          []SDK
	plugins       []Plugin
	integrations  []Integration
	examples      []Example
	guides        []Guide
	partners      []Partner
	running       bool
}

type SDK struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Language    string    `json:"language"`
	Version     string    `json:"version"`
	Repository  string    `json:"repository"`
	Documentation string  `json:"documentation"`
	Status      string    `json:"status"` // stable, beta, experimental
	CreatedAt   time.Time `json:"createdAt"`
}

type Plugin struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Author      string    `json:"author"`
	Version     string    `json:"version"`
	Repository  string    `json:"repository"`
	Downloads   int       `json:"downloads"`
	Rating      float64   `json:"rating"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Integration struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Partner     string    `json:"partner"`
	Type        string    `json:"type"`
	Description string    `json:"description"`
	Documentation string  `json:"documentation"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Example struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Language    string    `json:"language"`
	Path        string    `json:"path"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Guide struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Category    string    `json:"category"`
	Path        string    `json:"path"`
	Description string    `json:"description"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Partner struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Website     string    `json:"website"`
	Description string    `json:"description"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

func NewEngine() *Engine {
	return &Engine{
		sdks:         make([]SDK, 0),
		plugins:      make([]Plugin, 0),
		integrations: make([]Integration, 0),
		examples:     make([]Example, 0),
		guides:       make([]Guide, 0),
		partners:     make([]Partner, 0),
		running:      false,
	}
}

func (e *Engine) RegisterSDK(sdk SDK) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sdks = append(e.sdks, sdk)
	klog.Infof("Registered ecosystem SDK: %s (%s)", sdk.Name, sdk.Language)
}

func (e *Engine) RegisterPlugin(plugin Plugin) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.plugins = append(e.plugins, plugin)
	klog.Infof("Registered ecosystem plugin: %s", plugin.Name)
}

func (e *Engine) RegisterIntegration(integration Integration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.integrations = append(e.integrations, integration)
	klog.Infof("Registered integration: %s", integration.Name)
}

func (e *Engine) RegisterExample(example Example) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.examples = append(e.examples, example)
	klog.Infof("Registered example: %s", example.Name)
}

func (e *Engine) RegisterGuide(guide Guide) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.guides = append(e.guides, guide)
	klog.Infof("Registered guide: %s", guide.Title)
}

func (e *Engine) RegisterPartner(partner Partner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.partners = append(e.partners, partner)
	klog.Infof("Registered partner: %s", partner.Name)
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Ecosystem Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Ecosystem Engine stopped")
}

func (e *Engine) GetSDKs() []SDK {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.sdks
}

func (e *Engine) GetPlugins() []Plugin {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.plugins
}

func (e *Engine) GetIntegrations() []Integration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.integrations
}

func (e *Engine) GetExamples() []Example {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.examples
}

func (e *Engine) GetGuides() []Guide {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.guides
}

func (e *Engine) GetPartners() []Partner {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.partners
}
