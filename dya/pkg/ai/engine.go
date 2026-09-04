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

package ai

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	detectors   []AnomalyDetector
	correlators []EventCorrelator
	scorers     []RiskScorer
	predictors  []Predictor
	running     bool
}

type AnomalyDetector interface {
	Detect(data interface{}) (*AnomalyResult, error)
	Name() string
}

type EventCorrelator interface {
	Correlate(events []interface{}) (*CorrelationResult, error)
	Name() string
}

type RiskScorer interface {
	Score(asset interface{}) (*RiskScore, error)
	Name() string
}

type Predictor interface {
	Predict(data interface{}) (*PredictionResult, error)
	Name() string
}

type AnomalyResult struct {
	Detected    bool      `json:"detected"`
	Score       float64   `json:"score"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
}

type CorrelationResult struct {
	IncidentID string        `json:"incidentId"`
	Events     []interface{} `json:"events"`
	RootCause  string        `json:"rootCause"`
	Confidence float64       `json:"confidence"`
}

type RiskScore struct {
	AssetID   string             `json:"assetId"`
	Score     float64            `json:"score"`
	Factors   map[string]float64 `json:"factors"`
	Timestamp time.Time          `json:"timestamp"`
}

type PredictionResult struct {
	PredictedState  string   `json:"predictedState"`
	Confidence      float64  `json:"confidence"`
	Recommendations []string `json:"recommendations"`
}

func NewEngine() *Engine {
	return &Engine{
		detectors:   make([]AnomalyDetector, 0),
		correlators: make([]EventCorrelator, 0),
		scorers:     make([]RiskScorer, 0),
		predictors:  make([]Predictor, 0),
		running:     false,
	}
}

func (e *Engine) RegisterDetector(detector AnomalyDetector) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.detectors = append(e.detectors, detector)
	klog.Infof("Registered anomaly detector: %s", detector.Name())
}

func (e *Engine) RegisterCorrelator(correlator EventCorrelator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.correlators = append(e.correlators, correlator)
	klog.Infof("Registered event correlator: %s", correlator.Name())
}

func (e *Engine) RegisterScorer(scorer RiskScorer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scorers = append(e.scorers, scorer)
	klog.Infof("Registered risk scorer: %s", scorer.Name())
}

func (e *Engine) RegisterPredictor(predictor Predictor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.predictors = append(e.predictors, predictor)
	klog.Infof("Registered predictor: %s", predictor.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("AI Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("AI Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) DetectAnomalies(data interface{}) []*AnomalyResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*AnomalyResult, 0)
	for _, detector := range e.detectors {
		result, err := detector.Detect(data)
		if err != nil {
			klog.Errorf("Anomaly detector %s failed: %v", detector.Name(), err)
			continue
		}
		if result != nil && result.Detected {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) CorrelateEvents(events []interface{}) []*CorrelationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*CorrelationResult, 0)
	for _, correlator := range e.correlators {
		result, err := correlator.Correlate(events)
		if err != nil {
			klog.Errorf("Event correlator %s failed: %v", correlator.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) ScoreRisk(asset interface{}) []*RiskScore {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*RiskScore, 0)
	for _, scorer := range e.scorers {
		result, err := scorer.Score(asset)
		if err != nil {
			klog.Errorf("Risk scorer %s failed: %v", scorer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) Predict(data interface{}) []*PredictionResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*PredictionResult, 0)
	for _, predictor := range e.predictors {
		result, err := predictor.Predict(data)
		if err != nil {
			klog.Errorf("Predictor %s failed: %v", predictor.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
