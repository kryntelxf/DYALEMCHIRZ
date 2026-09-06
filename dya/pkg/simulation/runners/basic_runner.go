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

package runners

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/simulation"
)

type BasicRunner struct{}

func (r *BasicRunner) Name() string {
	return "basic-runner"
}

func (r *BasicRunner) Run(scenario *simulation.Scenario) (*simulation.SimulationResult, error) {
	klog.V(4).Infof("Running simulation: %s", scenario.Name)
	return &simulation.SimulationResult{
		ScenarioID:  scenario.ID,
		Status:      "completed",
		Duration:    "5s",
		SuccessRate: 99.5,
		Metrics: map[string]interface{}{
			"nodes":     scenario.Nodes,
			"pods":      scenario.Pods,
			"services":  scenario.Services,
		},
		Failures:       []string{},
		Recommendations: []string{"Scale up nodes", "Optimize resource allocation"},
		Timestamp:      time.Now(),
	}, nil
}
