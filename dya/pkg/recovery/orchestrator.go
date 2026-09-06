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

package recovery

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Orchestrator struct {
	mu        sync.RWMutex
	executors []Executor
	verifiers []Verifier
	notifiers []Notifier
	running   bool
}

type Executor interface {
	Execute(step *Step) (*ExecutionResult, error)
	Name() string
}

type Verifier interface {
	Verify(assetID string) (*VerificationResult, error)
	Name() string
}

type Notifier interface {
	Notify(message string, level string) error
	Name() string
}

type Step struct {
	ID          string
	Name        string
	Action      string
	Description string
	Timeout     time.Duration
}

type ExecutionResult struct {
	StepID    string    `json:"stepId"`
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	Duration  string    `json:"duration"`
	Timestamp time.Time `json:"timestamp"`
}

type VerificationResult struct {
	AssetID   string    `json:"assetId"`
	Healthy   bool      `json:"healthy"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type RecoveryStatus struct {
	PlanID      string    `json:"planId"`
	Status      string    `json:"status"`
	CurrentStep int       `json:"currentStep"`
	TotalSteps  int       `json:"totalSteps"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
	Error       string    `json:"error"`
}

func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		executors: make([]Executor, 0),
		verifiers: make([]Verifier, 0),
		notifiers: make([]Notifier, 0),
		running:   false,
	}
}

func (o *Orchestrator) RegisterExecutor(executor Executor) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.executors = append(o.executors, executor)
	klog.Infof("Registered executor: %s", executor.Name())
}

func (o *Orchestrator) RegisterVerifier(verifier Verifier) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.verifiers = append(o.verifiers, verifier)
	klog.Infof("Registered verifier: %s", verifier.Name())
}

func (o *Orchestrator) RegisterNotifier(notifier Notifier) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.notifiers = append(o.notifiers, notifier)
	klog.Infof("Registered notifier: %s", notifier.Name())
}

func (o *Orchestrator) Start() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running {
		return
	}
	o.running = true
	klog.Info("Recovery Orchestrator started")
}

func (o *Orchestrator) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.running {
		return
	}
	o.running = false
	klog.Info("Recovery Orchestrator stopped")
}

func (o *Orchestrator) ExecuteSteps(steps []Step) *RecoveryStatus {
	klog.Info("Executing recovery steps...")

	status := &RecoveryStatus{
		PlanID:      "recovery",
		Status:      "running",
		CurrentStep: 0,
		TotalSteps:  len(steps),
		StartedAt:   time.Now(),
	}

	for i, step := range steps {
		status.CurrentStep = i + 1
		klog.Infof("Executing step %d/%d: %s", i+1, len(steps), step.Name)

		executed := false
		for _, executor := range o.executors {
			result, err := executor.Execute(&step)
			if err != nil {
				klog.Errorf("Executor %s failed: %v", executor.Name(), err)
				continue
			}
			if result != nil && result.Success {
				executed = true
				break
			}
		}

		if !executed {
			status.Status = "failed"
			status.Error = "Step execution failed"
			klog.Errorf("Recovery failed at step %s", step.Name)
			o.notify("Recovery failed: "+step.Name, "critical")
			return status
		}

		// Verify after each step
		for _, verifier := range o.verifiers {
			result, err := verifier.Verify("asset")
			if err != nil {
				klog.Errorf("Verifier %s failed: %v", verifier.Name(), err)
				continue
			}
			if result != nil && !result.Healthy {
				status.Status = "failed"
				status.Error = "Verification failed"
				klog.Errorf("Recovery verification failed")
				o.notify("Recovery verification failed", "high")
				return status
			}
		}
	}

	status.Status = "completed"
	status.CompletedAt = time.Now()
	klog.Info("Recovery completed successfully")
	o.notify("Recovery completed successfully", "info")
	return status
}

func (o *Orchestrator) notify(message string, level string) {
	for _, notifier := range o.notifiers {
		if err := notifier.Notify(message, level); err != nil {
			klog.Errorf("Notifier %s failed: %v", notifier.Name(), err)
		}
	}
}
