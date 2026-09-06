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

package policy

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu         sync.RWMutex
	policies   []Policy
	evaluators []Evaluator
	enforcers  []Enforcer
	auditors   []Auditor
	running    bool
}

type Policy struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Type        string            `json:"type"`
	Rules       []Rule            `json:"rules"`
	Severity    string            `json:"severity"`
	Enabled     bool              `json:"enabled"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

type Rule struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Condition  string            `json:"condition"`
	Action     string            `json:"action"`
	Parameters map[string]string `json:"parameters"`
}

type Evaluator interface {
	Evaluate(policy *Policy, context interface{}) (*EvaluationResult, error)
	Name() string
}

type Enforcer interface {
	Enforce(result *EvaluationResult) (*EnforcementResult, error)
	Name() string
}

type Auditor interface {
	Audit(result *EvaluationResult) error
	Name() string
}

type EvaluationResult struct {
	PolicyID  string    `json:"policyId"`
	Compliant bool      `json:"compliant"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type EnforcementResult struct {
	PolicyID  string    `json:"policyId"`
	Action    string    `json:"action"`
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		policies:   make([]Policy, 0),
		evaluators: make([]Evaluator, 0),
		enforcers:  make([]Enforcer, 0),
		auditors:   make([]Auditor, 0),
		running:    false,
	}
}

func (e *Engine) RegisterPolicy(policy Policy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies = append(e.policies, policy)
	klog.Infof("Registered policy: %s", policy.Name)
}

func (e *Engine) RegisterEvaluator(evaluator Evaluator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evaluators = append(e.evaluators, evaluator)
	klog.Infof("Registered evaluator: %s", evaluator.Name())
}

func (e *Engine) RegisterEnforcer(enforcer Enforcer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enforcers = append(e.enforcers, enforcer)
	klog.Infof("Registered enforcer: %s", enforcer.Name())
}

func (e *Engine) RegisterAuditor(auditor Auditor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.auditors = append(e.auditors, auditor)
	klog.Infof("Registered auditor: %s", auditor.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Policy Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Policy Engine stopped")
}

func (e *Engine) EvaluatePolicies(context interface{}) []*EvaluationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*EvaluationResult, 0)
	for _, policy := range e.policies {
		if !policy.Enabled {
			continue
		}
		for _, evaluator := range e.evaluators {
			result, err := evaluator.Evaluate(&policy, context)
			if err != nil {
				klog.Errorf("Evaluator %s failed: %v", evaluator.Name(), err)
				continue
			}
			if result != nil {
				results = append(results, result)
			}
		}
	}
	return results
}

func (e *Engine) EnforcePolicies(results []*EvaluationResult) []*EnforcementResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	enforcementResults := make([]*EnforcementResult, 0)
	for _, result := range results {
		if !result.Compliant {
			for _, enforcer := range e.enforcers {
				enforcementResult, err := enforcer.Enforce(result)
				if err != nil {
					klog.Errorf("Enforcer %s failed: %v", enforcer.Name(), err)
					continue
				}
				if enforcementResult != nil {
					enforcementResults = append(enforcementResults, enforcementResult)
				}
			}
		}
	}
	return enforcementResults
}
