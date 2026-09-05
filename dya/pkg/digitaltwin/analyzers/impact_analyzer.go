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

package analyzers

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/digitaltwin"
)

type ImpactAnalyzer struct{}

func (a *ImpactAnalyzer) Name() string {
	return "impact-analyzer"
}

func (a *ImpactAnalyzer) Analyze(asset interface{}) (*digitaltwin.ImpactResult, error) {
	klog.V(4).Info("ImpactAnalyzer running")
	return &digitaltwin.ImpactResult{
		AssetID:      "unknown",
		ImpactLevel:  "low",
		Dependencies: []string{},
		Criticality:  "medium",
	}, nil
}
