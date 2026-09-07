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

package event

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// Event represents a normalized event
type Event struct {
	ID          string                 `json:"id"`
	Source      string                 `json:"source"`
	Type        string                 `json:"type"` // create, update, delete
	AssetID     string                 `json:"assetId"`
	Timestamp   time.Time              `json:"timestamp"`
	Data        map[string]interface{} `json:"data"`
}

// Pipeline processes events
type Pipeline struct {
	mu          sync.RWMutex
	processors  []Processor
	handlers    []Handler
	events      []*Event
	metrics     *Metrics
}

// Processor processes an event
type Processor interface {
	Process(event *Event) (*Event, error)
	Name() string
}

// Handler handles an event
type Handler interface {
	Handle(event *Event) error
	Name() string
}

// Metrics tracks event statistics
type Metrics struct {
	mu          sync.RWMutex
	Total       int64
	Processed   int64
	Failed      int64
	LastEvent   time.Time
}

// NewPipeline creates a new event pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{
		processors: make([]Processor, 0),
		handlers:   make([]Handler, 0),
		events:     make([]*Event, 0),
		metrics:    &Metrics{},
	}
}

// RegisterProcessor adds a processor
func (p *Pipeline) RegisterProcessor(processor Processor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processors = append(p.processors, processor)
	klog.Infof("Registered event processor: %s", processor.Name())
}

// RegisterHandler adds a handler
func (p *Pipeline) RegisterHandler(handler Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers = append(p.handlers, handler)
	klog.Infof("Registered event handler: %s", handler.Name())
}

// Process processes an event through the pipeline
func (p *Pipeline) Process(ctx context.Context, event *Event) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	p.metrics.mu.Lock()
	p.metrics.Total++
	p.metrics.LastEvent = time.Now()
	p.metrics.mu.Unlock()

	// Process through processors
	current := event
	for _, processor := range p.processors {
		var err error
		current, err = processor.Process(current)
		if err != nil {
			p.metrics.mu.Lock()
			p.metrics.Failed++
			p.metrics.mu.Unlock()
			return fmt.Errorf("processor %s failed: %w", processor.Name(), err)
		}
	}

	// Handle the event
	for _, handler := range p.handlers {
		if err := handler.Handle(current); err != nil {
			klog.Errorf("Handler %s failed: %v", handler.Name(), err)
		}
	}

	p.metrics.mu.Lock()
	p.metrics.Processed++
	p.metrics.mu.Unlock()

	p.events = append(p.events, current)
	klog.V(4).Infof("Processed event: %s for asset %s", event.Type, event.AssetID)
	return nil
}

// GetMetrics returns event metrics
func (p *Pipeline) GetMetrics() *Metrics {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()
	return &Metrics{
		Total:     p.metrics.Total,
		Processed: p.metrics.Processed,
		Failed:    p.metrics.Failed,
		LastEvent: p.metrics.LastEvent,
	}
}
