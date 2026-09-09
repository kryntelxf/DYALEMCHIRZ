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

package developer

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu        sync.RWMutex
	sdks      []SDK
	plugins   []Plugin
	templates []Template
	tools     []Tool
	docs      []Doc
	running   bool
}

type SDK struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	Language    string    `json:"language"`
	Repository  string    `json:"repository"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Plugin struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	Type        string    `json:"type"`
	Author      string    `json:"author"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Template struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Path        string    `json:"path"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Tool struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Command     string    `json:"command"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Doc struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Path        string    `json:"path"`
	Description string    `json:"description"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func NewEngine() *Engine {
	return &Engine{
		sdks:      make([]SDK, 0),
		plugins:   make([]Plugin, 0),
		templates: make([]Template, 0),
		tools:     make([]Tool, 0),
		docs:      make([]Doc, 0),
		running:   false,
	}
}

func (e *Engine) RegisterSDK(sdk SDK) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sdks = append(e.sdks, sdk)
	klog.Infof("Registered SDK: %s (%s)", sdk.Name, sdk.Language)
}

func (e *Engine) RegisterPlugin(plugin Plugin) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.plugins = append(e.plugins, plugin)
	klog.Infof("Registered plugin: %s", plugin.Name)
}

func (e *Engine) RegisterTemplate(template Template) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.templates = append(e.templates, template)
	klog.Infof("Registered template: %s", template.Name)
}

func (e *Engine) RegisterTool(tool Tool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tools = append(e.tools, tool)
	klog.Infof("Registered tool: %s", tool.Name)
}

func (e *Engine) RegisterDoc(doc Doc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.docs = append(e.docs, doc)
	klog.Infof("Registered doc: %s", doc.Title)
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Developer Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Developer Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
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

func (e *Engine) GetTemplates() []Template {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.templates
}

func (e *Engine) GetTools() []Tool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tools
}

func (e *Engine) GetDocs() []Doc {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.docs
}
