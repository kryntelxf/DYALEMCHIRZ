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

package impact

import (
	"k8s.io/kubernetes/dya/pkg/graph"
)

// Analyzer analyzes impact of failures
type Analyzer struct {
	graph *graph.Graph
}

// NewAnalyzer creates a new impact analyzer
func NewAnalyzer(g *graph.Graph) *Analyzer {
	return &Analyzer{graph: g}
}

// ImpactResult represents the result of impact analysis
type ImpactResult struct {
	AssetID          string   `json:"assetId"`
	DirectDependents []string `json:"directDependents"`
	AllDependents    []string `json:"allDependents"`
	ImpactLevel      string   `json:"impactLevel"`
	AffectedCount    int      `json:"affectedCount"`
}

// Analyze analyzes the impact of an asset failure
func (a *Analyzer) Analyze(assetID string) *ImpactResult {
	if a.graph == nil {
		return &ImpactResult{
			AssetID:       assetID,
			ImpactLevel:   "unknown",
			AffectedCount: 0,
		}
	}

	directDeps := a.graph.GetDependents(assetID)
	allDeps := a.getAllDependents(assetID)

	level := "low"
	if len(allDeps) > 10 {
		level = "critical"
	} else if len(allDeps) > 5 {
		level = "high"
	} else if len(allDeps) > 2 {
		level = "medium"
	}

	return &ImpactResult{
		AssetID:          assetID,
		DirectDependents: directDeps,
		AllDependents:    allDeps,
		ImpactLevel:      level,
		AffectedCount:    len(allDeps),
	}
}

func (a *Analyzer) getAllDependents(assetID string) []string {
	if a.graph == nil {
		return []string{}
	}
	visited := make(map[string]bool)
	result := []string{}
	a.collectDependents(assetID, visited, &result)
	return result
}

func (a *Analyzer) collectDependents(assetID string, visited map[string]bool, result *[]string) {
	if visited[assetID] {
		return
	}
	visited[assetID] = true

	deps := a.graph.GetDependents(assetID)
	for _, dep := range deps {
		if !visited[dep] {
			*result = append(*result, dep)
			a.collectDependents(dep, visited, result)
		}
	}
}
