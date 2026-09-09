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

package predictive

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu           sync.RWMutex
	predictors   []Predictor
	forecasters  []Forecaster
	analyzers    []RiskAnalyzer
	recommenders []Recommender
	running      bool
}

type Predictor interface {
	Predict(data interface{}) (*Prediction, error)
	Name() string
}

type Forecaster interface {
	Forecast(data interface{}) (*Forecast, error)
	Name() string
}

type RiskAnalyzer interface {
	Analyze(data interface{}) (*RiskAnalysis, error)
	Name() string
}

type Recommender interface {
	Recommend(data interface{}) (*Recommendation, error)
	Name() string
}

type Prediction struct {
	AssetID     string    `json:"assetId"`
	EventType   string    `json:"eventType"`
	Probability float64   `json:"probability"`
	TimeFrame   string    `json:"timeFrame"`
	Confidence  float64   `json:"confidence"`
	Timestamp   time.Time `json:"timestamp"`
}

type Forecast struct {
	AssetID    string      `json:"assetId"`
	Metric     string      `json:"metric"`
	Values     []float64   `json:"values"`
	Timestamps []time.Time `json:"timestamps"`
	Confidence float64     `json:"confidence"`
	Timestamp  time.Time   `json:"timestamp"`
}

type RiskAnalysis struct {
	AssetID   string             `json:"assetId"`
	RiskScore float64            `json:"riskScore"`
	Factors   map[string]float64 `json:"factors"`
	Level     string             `json:"level"`
	Timestamp time.Time          `json:"timestamp"`
}

type Recommendation struct {
	AssetID     string    `json:"assetId"`
	Action      string    `json:"action"`
	Description string    `json:"description"`
	Priority    string    `json:"priority"`
	Timestamp   time.Time `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		predictors:   make([]Predictor, 0),
		forecasters:  make([]Forecaster, 0),
		analyzers:    make([]RiskAnalyzer, 0),
		recommenders: make([]Recommender, 0),
		running:      false,
	}
}

func (e *Engine) RegisterPredictor(predictor Predictor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.predictors = append(e.predictors, predictor)
	klog.Infof("Registered predictor: %s", predictor.Name())
}

func (e *Engine) RegisterForecaster(forecaster Forecaster) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.forecasters = append(e.forecasters, forecaster)
	klog.Infof("Registered forecaster: %s", forecaster.Name())
}

func (e *Engine) RegisterRiskAnalyzer(analyzer RiskAnalyzer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzers = append(e.analyzers, analyzer)
	klog.Infof("Registered risk analyzer: %s", analyzer.Name())
}

func (e *Engine) RegisterRecommender(recommender Recommender) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recommenders = append(e.recommenders, recommender)
	klog.Infof("Registered recommender: %s", recommender.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Predictive Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Predictive Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) Predict(data interface{}) []*Prediction {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Prediction, 0)
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

func (e *Engine) Forecast(data interface{}) []*Forecast {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Forecast, 0)
	for _, forecaster := range e.forecasters {
		result, err := forecaster.Forecast(data)
		if err != nil {
			klog.Errorf("Forecaster %s failed: %v", forecaster.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) AnalyzeRisk(data interface{}) []*RiskAnalysis {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*RiskAnalysis, 0)
	for _, analyzer := range e.analyzers {
		result, err := analyzer.Analyze(data)
		if err != nil {
			klog.Errorf("Risk analyzer %s failed: %v", analyzer.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) Recommend(data interface{}) []*Recommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*Recommendation, 0)
	for _, recommender := range e.recommenders {
		result, err := recommender.Recommend(data)
		if err != nil {
			klog.Errorf("Recommender %s failed: %v", recommender.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}
