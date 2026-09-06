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

package evaluators

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/policy"
)

type BasicEvaluator struct{}

func (e *BasicEvaluator) Name() string {
	return "basic-evaluator"
}

func (e *BasicEvaluator) Evaluate(p *policy.Policy, context interface{}) (*policy.EvaluationResult, error) {
	klog.V(4).Infof("Evaluating policy: %s", p.Name)
	return &policy.EvaluationResult{
		PolicyID:  p.ID,
		Compliant: true,
		Message:   "Policy compliant",
		Timestamp: time.Now(),
	}, nil
}
