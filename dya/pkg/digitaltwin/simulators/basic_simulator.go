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

package simulators

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/digitaltwin"
)

// BasicSimulator is a basic simulator
type BasicSimulator struct{}

// NewBasicSimulator creates a new basic simulator
func NewBasicSimulator() *BasicSimulator {
	return &BasicSimulator{}
}

// Name returns the name
func (s *BasicSimulator) Name() string {
	return "basic-simulator"
}

// Simulate runs simulation
func (s *BasicSimulator) Simulate(scenario *digitaltwin.Scenario) (*digitaltwin.SimulationResult, error) {
	if scenario == nil {
		return nil, nil
	}
	klog.V(4).Infof("Simulating scenario: %s", scenario.Name)
	return &digitaltwin.SimulationResult{
		ScenarioID:     scenario.ID,
		Status:         "completed",
		Impact:         "low",
		AffectedAssets: []string{"asset-1", "asset-2"},
		Metrics: map[string]interface{}{
			"nodes":    10,
			"pods":     50,
			"services": 5,
		},
		Recommendations: []string{"No action needed", "Monitor for 24 hours"},
		Timestamp:       time.Now(),
	}, nil
}
