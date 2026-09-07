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

package metrics

import (
	"sync"

	"k8s.io/klog/v2"
)

var (
	mu                   sync.RWMutex
	graphNodes           float64
	graphEdges           float64
	reconciliations      float64
	reconciliationErrors float64
	eventsTotal          float64
	eventsProcessed      float64
	eventsFailed         float64
)

// SetGraphMetrics updates graph metrics
func SetGraphMetrics(nodes, edges float64) {
	mu.Lock()
	defer mu.Unlock()
	graphNodes = nodes
	graphEdges = edges
	klog.V(4).Infof("Graph metrics: nodes=%.0f, edges=%.0f", nodes, edges)
}

// SetReconciliationMetrics updates reconciliation metrics
func SetReconciliationMetrics(count, errors float64) {
	mu.Lock()
	defer mu.Unlock()
	reconciliations = count
	reconciliationErrors = errors
}

// SetEventMetrics updates event metrics
func SetEventMetrics(total, processed, failed float64) {
	mu.Lock()
	defer mu.Unlock()
	eventsTotal = total
	eventsProcessed = processed
	eventsFailed = failed
}

// GetMetrics returns all metrics
func GetMetrics() map[string]float64 {
	mu.RLock()
	defer mu.RUnlock()
	return map[string]float64{
		"graph_nodes":            graphNodes,
		"graph_edges":            graphEdges,
		"reconciliations":        reconciliations,
		"reconciliation_errors":  reconciliationErrors,
		"events_total":           eventsTotal,
		"events_processed":       eventsProcessed,
		"events_failed":          eventsFailed,
	}
}
