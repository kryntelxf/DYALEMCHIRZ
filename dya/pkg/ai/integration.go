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

// Engine is the AI engine
type Engine struct {
	mu          sync.RWMutex
	detectors   []AnomalyDetector
	scorers     []RiskScorer
	predictors  []Predictor
	running     bool
}

// AnomalyDetector interface
type AnomalyDetector interface {
	Detect(data interface{}) (*AnomalyResult, error)
	Name() string
}

// RiskScorer interface
type RiskScorer interface {
	Score(asset interface{}) (*RiskScore, error)
	Name() string
}

// Predictor interface
type Predictor interface {
	Predict(data interface{}) (*PredictionResult, error)
	Name() string
}

// AnomalyResult represents anomaly detection result
type AnomalyResult struct {
	Detected    bool      `json:"detected"`
	Score       float64   `json:"score"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
}

// RiskScore represents risk score
type RiskScore struct {
	AssetID   string             `json:"assetId"`
	Score     float64            `json:"score"`
	Factors   map[string]float64 `json:"factors"`
	Timestamp time.Time          `json:"timestamp"`
}

// PredictionResult represents prediction result
type PredictionResult struct {
	PredictedState  string   `json:"predictedState"`
	Confidence      float64  `json:"confidence"`
	Recommendations []string `json:"recommendations"`
}

// NewEngine creates a new AI engine
func NewEngine() *Engine {
	return &Engine{
		detectors:  make([]AnomalyDetector, 0),
		scorers:    make([]RiskScorer, 0),
		predictors: make([]Predictor, 0),
		running:    false,
	}
}

// RegisterDetector registers a detector
func (e *Engine) RegisterDetector(detector AnomalyDetector) {
	if e == nil || detector == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.detectors = append(e.detectors, detector)
	klog.Infof("Registered detector: %s", detector.Name())
}

// RegisterScorer registers a scorer
func (e *Engine) RegisterScorer(scorer RiskScorer) {
	if e == nil || scorer == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scorers = append(e.scorers, scorer)
	klog.Infof("Registered scorer: %s", scorer.Name())
}

// RegisterPredictor registers a predictor
func (e *Engine) RegisterPredictor(predictor Predictor) {
	if e == nil || predictor == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.predictors = append(e.predictors, predictor)
	klog.Infof("Registered predictor: %s", predictor.Name())
}

// Start starts the AI engine
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
	klog.Info("AI Engine started")
}

// Stop stops the AI engine
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
	klog.Info("AI Engine stopped")
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

// Detect runs anomaly detection
func (e *Engine) Detect(data interface{}) []*AnomalyResult {
	if e == nil {
		return []*AnomalyResult{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make([]*AnomalyResult, 0)
	for _, detector := range e.detectors {
		if detector == nil {
			continue
		}
		result, err := detector.Detect(data)
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

// Score runs risk scoring
func (e *Engine) Score(asset interface{}) []*RiskScore {
	if e == nil {
		return []*RiskScore{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make([]*RiskScore, 0)
	for _, scorer := range e.scorers {
		if scorer == nil {
			continue
		}
		result, err := scorer.Score(asset)
		if err != nil {
			klog.Errorf("Scorer %s failed: %v", scorer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

// Predict runs prediction
func (e *Engine) Predict(data interface{}) []*PredictionResult {
	if e == nil {
		return []*PredictionResult{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make([]*PredictionResult, 0)
	for _, predictor := range e.predictors {
		if predictor == nil {
			continue
		}
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
