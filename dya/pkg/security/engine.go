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

package security

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	verifiers   []Verifier
	enforcers   []Enforcer
	auditors    []Auditor
	detectors   []Detector
	running     bool
}

type Verifier interface {
	Verify(identity interface{}) (*VerificationResult, error)
	Name() string
}

type Enforcer interface {
	Enforce(policy interface{}, context interface{}) (*EnforcementResult, error)
	Name() string
}

type Auditor interface {
	Audit(event interface{}) error
	Name() string
}

type Detector interface {
	Detect(activity interface{}) (*SecurityAnomaly, error)
	Name() string
}

type VerificationResult struct {
	IdentityID string    `json:"identityId"`
	Valid      bool      `json:"valid"`
	Reason     string    `json:"reason"`
	Timestamp  time.Time `json:"timestamp"`
}

type EnforcementResult struct {
	PolicyID  string    `json:"policyId"`
	Action    string    `json:"action"`
	Allowed   bool      `json:"allowed"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

type SecurityAnomaly struct {
	Type        string    `json:"type"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		verifiers: make([]Verifier, 0),
		enforcers: make([]Enforcer, 0),
		auditors:  make([]Auditor, 0),
		detectors: make([]Detector, 0),
		running:   false,
	}
}

func (e *Engine) RegisterVerifier(verifier Verifier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.verifiers = append(e.verifiers, verifier)
	klog.Infof("Registered verifier: %s", verifier.Name())
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

func (e *Engine) RegisterDetector(detector Detector) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.detectors = append(e.detectors, detector)
	klog.Infof("Registered detector: %s", detector.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Security Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Security Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) Verify(identity interface{}) []*VerificationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*VerificationResult, 0)
	for _, verifier := range e.verifiers {
		result, err := verifier.Verify(identity)
		if err != nil {
			klog.Errorf("Verifier %s failed: %v", verifier.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) Enforce(policy interface{}, context interface{}) []*EnforcementResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*EnforcementResult, 0)
	for _, enforcer := range e.enforcers {
		result, err := enforcer.Enforce(policy, context)
		if err != nil {
			klog.Errorf("Enforcer %s failed: %v", enforcer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) Audit(event interface{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, auditor := range e.auditors {
		if err := auditor.Audit(event); err != nil {
			klog.Errorf("Auditor %s failed: %v", auditor.Name(), err)
		}
	}
}

func (e *Engine) Detect(activity interface{}) []*SecurityAnomaly {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*SecurityAnomaly, 0)
	for _, detector := range e.detectors {
		result, err := detector.Detect(activity)
		if err != nil {
			klog.Errorf("Detector %s failed: %v", detector.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
