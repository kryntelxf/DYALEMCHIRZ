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

package predictors

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/ai"
)

// SimplePredictor makes simple predictions
type SimplePredictor struct {
	// confidence threshold
	confidence float64
}

// NewSimplePredictor creates a new simple predictor
func NewSimplePredictor() *SimplePredictor {
	return &SimplePredictor{
		confidence: 85.0,
	}
}

// Name returns the name of the predictor
func (p *SimplePredictor) Name() string {
	return "simple-predictor"
}

// Predict predicts future state
func (p *SimplePredictor) Predict(data interface{}) (*ai.PredictionResult, error) {
	if data == nil {
		return &ai.PredictionResult{
			PredictedState: "unknown",
			Confidence:     0,
			Recommendations: []string{"No data available"},
		}, nil
	}

	klog.V(4).Info("SimplePredictor making prediction")

	return &ai.PredictionResult{
		PredictedState: "stable",
		Confidence:     85.0,
		Recommendations: []string{
			"Continue monitoring",
			"Check resource usage",
		},
	}, nil
}
