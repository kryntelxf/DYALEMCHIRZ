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

package digitaltwin

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// Engine is the Digital Twin engine
type Engine struct {
	mu          sync.RWMutex
	simulators  []Simulator
	analyzers   []Analyzer
	running     bool
}

// Simulator interface
type Simulator interface {
	Simulate(scenario *Scenario) (*SimulationResult, error)
	Name() string
}

// Analyzer interface
type Analyzer interface {
	Analyze(result *SimulationResult) (*Analysis, error)
	Name() string
}

// Scenario represents a simulation scenario
type Scenario struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Changes     map[string]interface{} `json:"changes"`
	Parameters  map[string]string      `json:"parameters"`
}

// SimulationResult represents simulation result
type SimulationResult struct {
	ScenarioID      string                 `json:"scenarioId"`
	Status          string                 `json:"status"`
	Impact          string                 `json:"impact"`
	AffectedAssets  []string               `json:"affectedAssets"`
	Metrics         map[string]interface{} `json:"metrics"`
	Recommendations []string               `json:"recommendations"`
	Timestamp       time.Time              `json:"timestamp"`
}

// Analysis represents analysis result
type Analysis struct {
	ScenarioID string   `json:"scenarioId"`
	RiskLevel  string   `json:"riskLevel"`
	Insights   []string `json:"insights"`
	Confidence float64  `json:"confidence"`
	Timestamp  time.Time `json:"timestamp"`
}

// NewEngine creates a new Digital Twin engine
func NewEngine() *Engine {
	return &Engine{
		simulators: make([]Simulator, 0),
		analyzers:  make([]Analyzer, 0),
		running:    false,
	}
}

// RegisterSimulator registers a simulator
func (e *Engine) RegisterSimulator(simulator Simulator) {
	if e == nil || simulator == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.simulators = append(e.simulators, simulator)
	klog.Infof("Registered simulator: %s", simulator.Name())
}

// RegisterAnalyzer registers an analyzer
func (e *Engine) RegisterAnalyzer(analyzer Analyzer) {
	if e == nil || analyzer == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzers = append(e.analyzers, analyzer)
	klog.Infof("Registered analyzer: %s", analyzer.Name())
}

// Start starts the engine
func (e *Engine) Start() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Digital Twin Engine started")
}

// Stop stops the engine
func (e *Engine) Stop() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Digital Twin Engine stopped")
}

// IsRunning returns whether the engine is running
func (e *Engine) IsRunning() bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

// Simulate runs simulation
func (e *Engine) Simulate(scenario *Scenario) []*SimulationResult {
	if e == nil || scenario == nil {
		return []*SimulationResult{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make([]*SimulationResult, 0)
	for _, simulator := range e.simulators {
		if simulator == nil {
			continue
		}
		result, err := simulator.Simulate(scenario)
		if err != nil {
			klog.Errorf("Simulator %s failed: %v", simulator.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

// Analyze runs analysis
func (e *Engine) Analyze(results []*SimulationResult) []*Analysis {
	if e == nil || len(results) == 0 {
		return []*Analysis{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	analyses := make([]*Analysis, 0)
	for _, analyzer := range e.analyzers {
		if analyzer == nil {
			continue
		}
		for _, result := range results {
			if result == nil {
				continue
			}
			analysis, err := analyzer.Analyze(result)
			if err != nil {
				klog.Errorf("Analyzer %s failed: %v", analyzer.Name(), err)
				continue
			}
			if analysis != nil {
				analyses = append(analyses, analysis)
			}
		}
	}
	return analyses
}
