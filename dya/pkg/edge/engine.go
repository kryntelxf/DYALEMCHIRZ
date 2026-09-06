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
	mu            sync.RWMutex
	handlers      []LocalHandler
	syncers       []Syncer
	enforcers     []LocalEnforcer
	buffers       []Buffer
	running       bool
}

type LocalHandler interface {
	Handle(event interface{}) error
	Name() string
}

type Syncer interface {
	Sync() error
	Name() string
}

type LocalEnforcer interface {
	Enforce(policy interface{}) error
	Name() string
}

type Buffer interface {
	Store(data interface{}) error
	Flush() ([]interface{}, error)
	Name() string
}

type EdgeStatus struct {
	Mode          string    `json:"mode"` // online, offline, degraded
	LastSync      time.Time `json:"lastSync"`
	PendingEvents int       `json:"pendingEvents"`
	Connected     bool      `json:"connected"`
}

func NewEngine() *Engine {
	return &Engine{
		handlers:  make([]LocalHandler, 0),
		syncers:   make([]Syncer, 0),
		enforcers: make([]LocalEnforcer, 0),
		buffers:   make([]Buffer, 0),
		running:   false,
	}
}

func (e *Engine) RegisterHandler(handler LocalHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	klog.Infof("Registered local handler: %s", handler.Name())
}

func (e *Engine) RegisterSyncer(syncer Syncer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.syncers = append(e.syncers, syncer)
	klog.Infof("Registered syncer: %s", syncer.Name())
}

func (e *Engine) RegisterEnforcer(enforcer LocalEnforcer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enforcers = append(e.enforcers, enforcer)
	klog.Infof("Registered local enforcer: %s", enforcer.Name())
}

func (e *Engine) RegisterBuffer(buffer Buffer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.buffers = append(e.buffers, buffer)
	klog.Infof("Registered buffer: %s", buffer.Name())
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

func (e *Engine) HandleLocalEvent(event interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, handler := range e.handlers {
		if err := handler.Handle(event); err != nil {
			klog.Errorf("Local handler %s failed: %v", handler.Name(), err)
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
			klog.Errorf("Syncer %s failed: %v", syncer.Name(), err)
		}
	}
}

func (e *Engine) EnforceLocalPolicy(policy interface{}) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, enforcer := range e.enforcers {
		if err := enforcer.Enforce(policy); err != nil {
			klog.Errorf("Local enforcer %s failed: %v", enforcer.Name(), err)
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
			klog.Errorf("Buffer %s failed to store: %v", buffer.Name(), err)
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
			klog.Errorf("Buffer %s failed to flush: %v", buffer.Name(), err)
			continue
		}
		if len(data) > 0 {
			results = append(results, data)
		}
	}
	return results
}
