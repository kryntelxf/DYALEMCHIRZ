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

package simulation

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	runners     []Runner
	validators  []Validator
	analyzers   []Analyzer
	running     bool
}

type Runner interface {
	Run(scenario *Scenario) (*SimulationResult, error)
	Name() string
}

type Validator interface {
	Validate(result *SimulationResult) (*ValidationResult, error)
	Name() string
}

type Analyzer interface {
	Analyze(results []*SimulationResult) (*AnalysisResult, error)
	Name() string
}

type Scenario struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Nodes       int                    `json:"nodes"`
	Pods        int                    `json:"pods"`
	Services    int                    `json:"services"`
	Parameters  map[string]interface{} `json:"parameters"`
}

type SimulationResult struct {
	ScenarioID     string                 `json:"scenarioId"`
	Status         string                 `json:"status"`
	Duration       string                 `json:"duration"`
	SuccessRate    float64                `json:"successRate"`
	Metrics        map[string]interface{} `json:"metrics"`
	Failures       []string               `json:"failures"`
	Recommendations []string              `json:"recommendations"`
	Timestamp      time.Time              `json:"timestamp"`
}

type ValidationResult struct {
	ScenarioID string   `json:"scenarioId"`
	Valid      bool     `json:"valid"`
	Errors     []string `json:"errors"`
	Warnings   []string `json:"warnings"`
	Timestamp  time.Time `json:"timestamp"`
}

type AnalysisResult struct {
	TotalScenarios int                    `json:"totalScenarios"`
	SuccessRate    float64                `json:"successRate"`
	AverageDuration string                `json:"averageDuration"`
	Insights       []string               `json:"insights"`
	Timestamp      time.Time              `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		runners:    make([]Runner, 0),
		validators: make([]Validator, 0),
		analyzers:  make([]Analyzer, 0),
		running:    false,
	}
}

func (e *Engine) RegisterRunner(runner Runner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.runners = append(e.runners, runner)
	klog.Infof("Registered simulation runner: %s", runner.Name())
}

func (e *Engine) RegisterValidator(validator Validator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.validators = append(e.validators, validator)
	klog.Infof("Registered simulation validator: %s", validator.Name())
}

func (e *Engine) RegisterAnalyzer(analyzer Analyzer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzers = append(e.analyzers, analyzer)
	klog.Infof("Registered simulation analyzer: %s", analyzer.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Simulation Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Simulation Engine stopped")
}

func (e *Engine) RunSimulation(scenario *Scenario) []*SimulationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*SimulationResult, 0)
	for _, runner := range e.runners {
		result, err := runner.Run(scenario)
		if err != nil {
			klog.Errorf("Simulation runner %s failed: %v", runner.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) ValidateResults(results []*SimulationResult) []*ValidationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	validationResults := make([]*ValidationResult, 0)
	for _, result := range results {
		for _, validator := range e.validators {
			vr, err := validator.Validate(result)
			if err != nil {
				klog.Errorf("Validator %s failed: %v", validator.Name(), err)
				continue
			}
			if vr != nil {
				validationResults = append(validationResults, vr)
			}
		}
	}
	return validationResults
}

func (e *Engine) AnalyzeResults(results []*SimulationResult) []*AnalysisResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	analysisResults := make([]*AnalysisResult, 0)
	for _, analyzer := range e.analyzers {
		result, err := analyzer.Analyze(results)
		if err != nil {
			klog.Errorf("Analyzer %s failed: %v", analyzer.Name(), err)
			continue
		}
		if result != nil {
			analysisResults = append(analysisResults, result)
		}
	}
	return analysisResults
}
