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

type Engine struct {
	mu            sync.RWMutex
	simulators    []Simulator
	analyzers     []ImpactAnalyzer
	running       bool
}

type Simulator interface {
	Simulate(scenario *Scenario) (*SimulationResult, error)
	Name() string
}

type ImpactAnalyzer interface {
	Analyze(asset interface{}) (*ImpactResult, error)
	Name() string
}

type Scenario struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Assets      []string          `json:"assets"`
	Parameters  map[string]string `json:"parameters"`
}

type SimulationResult struct {
	ScenarioID      string    `json:"scenarioId"`
	Status          string    `json:"status"`
	Impact          string    `json:"impact"`
	RecoveryTime    string    `json:"recoveryTime"`
	AffectedAssets  []string  `json:"affectedAssets"`
	Recommendations []string  `json:"recommendations"`
	Timestamp       time.Time `json:"timestamp"`
}

type ImpactResult struct {
	AssetID         string   `json:"assetId"`
	ImpactLevel     string   `json:"impactLevel"`
	Dependencies    []string `json:"dependencies"`
	Criticality     string   `json:"criticality"`
}

func NewEngine() *Engine {
	return &Engine{
		simulators: make([]Simulator, 0),
		analyzers:  make([]ImpactAnalyzer, 0),
		running:    false,
	}
}

func (e *Engine) RegisterSimulator(simulator Simulator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.simulators = append(e.simulators, simulator)
	klog.Infof("Registered simulator: %s", simulator.Name())
}

func (e *Engine) RegisterAnalyzer(analyzer ImpactAnalyzer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzers = append(e.analyzers, analyzer)
	klog.Infof("Registered impact analyzer: %s", analyzer.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Digital Twin Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Digital Twin Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) Simulate(scenario *Scenario) []*SimulationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*SimulationResult, 0)
	for _, simulator := range e.simulators {
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

func (e *Engine) AnalyzeImpact(asset interface{}) []*ImpactResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*ImpactResult, 0)
	for _, analyzer := range e.analyzers {
		result, err := analyzer.Analyze(asset)
		if err != nil {
			klog.Errorf("Impact analyzer %s failed: %v", analyzer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
