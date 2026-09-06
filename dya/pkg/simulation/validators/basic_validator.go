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

package validators

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/simulation"
)

type BasicValidator struct{}

func (v *BasicValidator) Name() string {
	return "basic-validator"
}

func (v *BasicValidator) Validate(result *simulation.SimulationResult) (*simulation.ValidationResult, error) {
	klog.V(4).Infof("Validating simulation: %s", result.ScenarioID)
	return &simulation.ValidationResult{
		ScenarioID: result.ScenarioID,
		Valid:      true,
		Errors:     []string{},
		Warnings:   []string{},
		Timestamp:  time.Now(),
	}, nil
}
