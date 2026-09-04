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

package resilience

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu               sync.RWMutex
	healthCheckers   []HealthChecker
	failureDetectors []FailureDetector
	recoveryPlanners []RecoveryPlanner
	running          bool
}

type HealthChecker interface {
	Check(asset interface{}) (*HealthStatus, error)
	Name() string
}

type FailureDetector interface {
	Detect(asset interface{}) (*Failure, error)
	Name() string
}

type RecoveryPlanner interface {
	Plan(asset interface{}, failure *Failure) (*RecoveryPlan, error)
	Name() string
}

type HealthStatus struct {
	AssetID   string    `json:"assetId"`
	Status    string    `json:"status"`
	Score     float64   `json:"score"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type Failure struct {
	AssetID     string    `json:"assetId"`
	Type        string    `json:"type"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
}

type RecoveryPlan struct {
	AssetID          string   `json:"assetId"`
	Steps            []string `json:"steps"`
	EstimatedTime    string   `json:"estimatedTime"`
	Priority         int      `json:"priority"`
	RequiresApproval bool     `json:"requiresApproval"`
}

func NewEngine() *Engine {
	return &Engine{
		healthCheckers:   make([]HealthChecker, 0),
		failureDetectors: make([]FailureDetector, 0),
		recoveryPlanners: make([]RecoveryPlanner, 0),
		running:          false,
	}
}

func (e *Engine) RegisterHealthChecker(checker HealthChecker) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.healthCheckers = append(e.healthCheckers, checker)
	klog.Infof("Registered health checker: %s", checker.Name())
}

func (e *Engine) RegisterFailureDetector(detector FailureDetector) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failureDetectors = append(e.failureDetectors, detector)
	klog.Infof("Registered failure detector: %s", detector.Name())
}

func (e *Engine) RegisterRecoveryPlanner(planner RecoveryPlanner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recoveryPlanners = append(e.recoveryPlanners, planner)
	klog.Infof("Registered recovery planner: %s", planner.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Resilience Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Resilience Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) CheckHealth(asset interface{}) []*HealthStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*HealthStatus, 0)
	for _, checker := range e.healthCheckers {
		result, err := checker.Check(asset)
		if err != nil {
			klog.Errorf("Health checker %s failed: %v", checker.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) DetectFailures(asset interface{}) []*Failure {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Failure, 0)
	for _, detector := range e.failureDetectors {
		result, err := detector.Detect(asset)
		if err != nil {
			klog.Errorf("Failure detector %s failed: %v", detector.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) PlanRecovery(asset interface{}, failure *Failure) []*RecoveryPlan {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*RecoveryPlan, 0)
	for _, planner := range e.recoveryPlanners {
		result, err := planner.Plan(asset, failure)
		if err != nil {
			klog.Errorf("Recovery planner %s failed: %v", planner.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
