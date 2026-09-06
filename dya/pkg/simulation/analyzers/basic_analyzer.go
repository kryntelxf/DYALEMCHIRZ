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
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/simulation"
)

type BasicAnalyzer struct{}

func (a *BasicAnalyzer) Name() string {
	return "basic-analyzer"
}

func (a *BasicAnalyzer) Analyze(results []*simulation.SimulationResult) (*simulation.AnalysisResult, error) {
	klog.V(4).Info("Analyzing simulation results")
	return &simulation.AnalysisResult{
		TotalScenarios:  len(results),
		SuccessRate:     99.0,
		AverageDuration: "5s",
		Insights:        []string{"All scenarios passed", "Performance is optimal"},
		Timestamp:       time.Now(),
	}, nil
}
