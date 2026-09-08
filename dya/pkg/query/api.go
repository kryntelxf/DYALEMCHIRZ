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

package query

import (
	"k8s.io/kubernetes/dya/pkg/graph"
)

// API provides graph query methods
type API struct {
	graph *graph.Graph
}

// NewAPI creates a new query API
func NewAPI(g *graph.Graph) *API {
	return &API{graph: g}
}

// GetNode returns a node by ID
func (q *API) GetNode(id string) (*graph.Node, bool) {
	return q.graph.GetNode(id)
}

// GetDependencies returns dependencies of a node
func (q *API) GetDependencies(id string) []string {
	return q.graph.GetDependencies(id)
}

// GetDependents returns dependents of a node
func (q *API) GetDependents(id string) []string {
	return q.graph.GetDependents(id)
}

// GetAllNodes returns all nodes
func (q *API) GetAllNodes() []*graph.Node {
	return q.graph.GetAllNodes()
}

// GetNodesByKind returns nodes of a specific kind
func (q *API) GetNodesByKind(kind string) []*graph.Node {
	result := []*graph.Node{}
	for _, node := range q.graph.GetAllNodes() {
		if node.Kind == kind {
			result = append(result, node)
		}
	}
	return result
}

// GetNodesByLabel returns nodes with a specific label
func (q *API) GetNodesByLabel(key, value string) []*graph.Node {
	result := []*graph.Node{}
	for _, node := range q.graph.GetAllNodes() {
		if node.Labels != nil && node.Labels[key] == value {
			result = append(result, node)
		}
	}
	return result
}
