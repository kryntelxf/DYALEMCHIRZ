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

package knowledge

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	extractors  []Extractor
	analyzers   []Analyzer
	queriers    []Querier
	running     bool
}

type Extractor interface {
	Extract(data interface{}) (*Knowledge, error)
	Name() string
}

type Analyzer interface {
	Analyze(knowledge *Knowledge) (*Analysis, error)
	Name() string
}

type Querier interface {
	Query(query string) (*QueryResult, error)
	Name() string
}

type Knowledge struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Content     map[string]interface{} `json:"content"`
	Relations   []Relation             `json:"relations"`
	Timestamp   time.Time              `json:"timestamp"`
}

type Relation struct {
	Source      string `json:"source"`
	Target      string `json:"target"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

type Analysis struct {
	KnowledgeID string   `json:"knowledgeId"`
	Patterns    []string `json:"patterns"`
	Insights    []string `json:"insights"`
	Confidence  float64  `json:"confidence"`
	Timestamp   time.Time `json:"timestamp"`
}

type QueryResult struct {
	Query       string        `json:"query"`
	Results     []interface{} `json:"results"`
	Count       int           `json:"count"`
	Duration    string        `json:"duration"`
	Timestamp   time.Time     `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		extractors: make([]Extractor, 0),
		analyzers:  make([]Analyzer, 0),
		queriers:   make([]Querier, 0),
		running:    false,
	}
}

func (e *Engine) RegisterExtractor(extractor Extractor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.extractors = append(e.extractors, extractor)
	klog.Infof("Registered knowledge extractor: %s", extractor.Name())
}

func (e *Engine) RegisterAnalyzer(analyzer Analyzer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzers = append(e.analyzers, analyzer)
	klog.Infof("Registered knowledge analyzer: %s", analyzer.Name())
}

func (e *Engine) RegisterQuerier(querier Querier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queriers = append(e.queriers, querier)
	klog.Infof("Registered knowledge querier: %s", querier.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Knowledge Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Knowledge Engine stopped")
}

func (e *Engine) ExtractKnowledge(data interface{}) []*Knowledge {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Knowledge, 0)
	for _, extractor := range e.extractors {
		result, err := extractor.Extract(data)
		if err != nil {
			klog.Errorf("Extractor %s failed: %v", extractor.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) AnalyzeKnowledge(knowledge *Knowledge) []*Analysis {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Analysis, 0)
	for _, analyzer := range e.analyzers {
		result, err := analyzer.Analyze(knowledge)
		if err != nil {
			klog.Errorf("Analyzer %s failed: %v", analyzer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) Query(query string) []*QueryResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*QueryResult, 0)
	for _, querier := range e.queriers {
		result, err := querier.Query(query)
		if err != nil {
			klog.Errorf("Querier %s failed: %v", querier.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
