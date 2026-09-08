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

package planners

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/resilience"
)

type BasicPlanner struct{}

func (p *BasicPlanner) Name() string {
	return "basic-planner"
}

func (p *BasicPlanner) Plan(asset interface{}, failure *resilience.Failure) (*resilience.RecoveryPlan, error) {
	klog.V(4).Info("BasicPlanner planning recovery")
	if failure == nil {
		return &resilience.RecoveryPlan{
			AssetID:          "unknown",
			Steps:            []string{"No failure detected"},
			EstimatedTime:    "0s",
			Priority:         0,
			RequiresApproval: false,
		}, nil
	}
	return &resilience.RecoveryPlan{
		AssetID:          failure.AssetID,
		Steps:            []string{"Investigate failure", "Restart service", "Verify recovery"},
		EstimatedTime:    "5 minutes",
		Priority:         1,
		RequiresApproval: false,
	}, nil
}
