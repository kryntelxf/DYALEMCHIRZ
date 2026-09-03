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

package scorers

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/ai"
)

// RiskScorer calculates risk scores
type RiskScorer struct{}

// Name returns the name of the scorer
func (s *RiskScorer) Name() string {
	return "risk-scorer"
}

// Score calculates risk score for an asset
func (s *RiskScorer) Score(asset interface{}) (*ai.RiskScore, error) {
	klog.V(4).Info("RiskScorer running")
	return &ai.RiskScore{
		AssetID:   "unknown",
		Score:     0,
		Factors:   map[string]float64{},
		Timestamp: time.Now(),
	}, nil
}
