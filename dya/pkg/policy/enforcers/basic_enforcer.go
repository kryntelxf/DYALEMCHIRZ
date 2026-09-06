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

package enforcers

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/policy"
)

type BasicEnforcer struct{}

func (e *BasicEnforcer) Name() string {
	return "basic-enforcer"
}

func (e *BasicEnforcer) Enforce(result *policy.EvaluationResult) (*policy.EnforcementResult, error) {
	klog.V(4).Infof("Enforcing policy: %s", result.PolicyID)
	return &policy.EnforcementResult{
		PolicyID:  result.PolicyID,
		Action:    "enforce",
		Success:   true,
		Message:   "Policy enforced successfully",
		Timestamp: time.Now(),
	}, nil
}
