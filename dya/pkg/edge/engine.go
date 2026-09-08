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

package edge

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	handlers    []Handler
	syncers     []Syncer
	enforcers   []Enforcer
	buffers     []Buffer
	running     bool
}

type Handler interface {
	Handle(event interface{}) error
	Name() string
}

type Syncer interface {
	Sync() error
	Name() string
}

type Enforcer interface {
	Enforce(policy interface{}) error
	Name() string
}

type Buffer interface {
	Store(data interface{}) error
	Flush() ([]interface{}, error)
	Name() string
}

type EdgeStatus struct {
	Mode          string    `json:"mode"`
	LastSync      time.Time `json:"lastSync"`
	PendingEvents int       `json:"pendingEvents"`
	Connected     bool      `json:"connected"`
}

func NewEngine() *Engine {
	return &Engine{
		handlers:  make([]Handler, 0),
		syncers:   make([]Syncer, 0),
		enforcers: make([]Enforcer, 0),
		buffers:   make([]Buffer, 0),
		running:   false,
	}
}

func (e *Engine) RegisterHandler(handler Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	klog.Infof("Registered edge handler: %s", handler.Name())
}

func (e *Engine) RegisterSyncer(syncer Syncer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.syncers = append(e.syncers, syncer)
	klog.Infof("Registered edge syncer: %s", syncer.Name())
}

func (e *Engine) RegisterEnforcer(enforcer Enforcer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enforcers = append(e.enforcers, enforcer)
	klog.Infof("Registered edge enforcer: %s", enforcer.Name())
}

func (e *Engine) RegisterBuffer(buffer Buffer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.buffers = append(e.buffers, buffer)
	klog.Infof("Registered edge buffer: %s", buffer.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Edge Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Edge Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) HandleLocal(event interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, handler := range e.handlers {
		if err := handler.Handle(event); err != nil {
			klog.Errorf("Edge handler %s failed: %v", handler.Name(), err)
			return err
		}
	}
	return nil
}

func (e *Engine) Sync() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, syncer := range e.syncers {
		if err := syncer.Sync(); err != nil {
			klog.Errorf("Edge syncer %s failed: %v", syncer.Name(), err)
		}
	}
}

func (e *Engine) EnforceLocal(policy interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, enforcer := range e.enforcers {
		if err := enforcer.Enforce(policy); err != nil {
			klog.Errorf("Edge enforcer %s failed: %v", enforcer.Name(), err)
			return err
		}
	}
	return nil
}

func (e *Engine) StoreBuffer(data interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, buffer := range e.buffers {
		if err := buffer.Store(data); err != nil {
			klog.Errorf("Edge buffer %s failed: %v", buffer.Name(), err)
			return err
		}
	}
	return nil
}

func (e *Engine) FlushBuffers() [][]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([][]interface{}, 0)
	for _, buffer := range e.buffers {
		data, err := buffer.Flush()
		if err != nil {
			klog.Errorf("Edge buffer %s failed: %v", buffer.Name(), err)
			continue
		}
		if len(data) > 0 {
			results = append(results, data)
		}
	}
	return results
}
