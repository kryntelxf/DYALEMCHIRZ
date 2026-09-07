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

package graph

import (
	"fmt"
	"sync"

	"k8s.io/klog/v2"
)

// Node represents an asset in the graph
type Node struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace,omitempty"`
	Kind       string            `json:"kind"`
	Labels     map[string]string `json:"labels,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// Edge represents a relationship between nodes
type Edge struct {
	Source      string `json:"source"`
	Target      string `json:"target"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// Graph is the in-memory asset graph
type Graph struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	edges map[string][]*Edge
}

// NewGraph creates a new empty graph
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*Node),
		edges: make(map[string][]*Edge),
	}
}

// AddNode adds a node to the graph
func (g *Graph) AddNode(node *Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if node == nil {
		return fmt.Errorf("node is nil")
	}
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	g.nodes[node.ID] = node
	klog.V(4).Infof("Added node: %s (%s)", node.ID, node.Kind)
	return nil
}

// GetNode retrieves a node by ID
func (g *Graph) GetNode(id string) (*Node, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	node, ok := g.nodes[id]
	return node, ok
}

// UpdateNode updates an existing node
func (g *Graph) UpdateNode(node *Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if node == nil {
		return fmt.Errorf("node is nil")
	}
	if _, ok := g.nodes[node.ID]; !ok {
		return fmt.Errorf("node %s not found", node.ID)
	}

	g.nodes[node.ID] = node
	klog.V(4).Infof("Updated node: %s", node.ID)
	return nil
}

// RemoveNode removes a node and its edges
func (g *Graph) RemoveNode(id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.nodes[id]; !ok {
		return fmt.Errorf("node %s not found", id)
	}

	delete(g.nodes, id)
	delete(g.edges, id)

	// Remove edges that target this node
	for source, edges := range g.edges {
		newEdges := make([]*Edge, 0)
		for _, edge := range edges {
			if edge.Target != id {
				newEdges = append(newEdges, edge)
			}
		}
		g.edges[source] = newEdges
	}

	klog.V(4).Infof("Removed node: %s", id)
	return nil
}

// AddEdge adds a relationship between nodes
func (g *Graph) AddEdge(source, target, edgeType, description string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.nodes[source]; !ok {
		return fmt.Errorf("source node %s not found", source)
	}
	if _, ok := g.nodes[target]; !ok {
		return fmt.Errorf("target node %s not found", target)
	}

	edge := &Edge{
		Source:      source,
		Target:      target,
		Type:        edgeType,
		Description: description,
	}

	g.edges[source] = append(g.edges[source], edge)
	klog.V(4).Infof("Added edge: %s -> %s (%s)", source, target, edgeType)
	return nil
}

// GetDependencies returns all dependencies of a node
func (g *Graph) GetDependencies(id string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	edges, ok := g.edges[id]
	if !ok {
		return []string{}
	}

	result := make([]string, len(edges))
	for i, edge := range edges {
		result[i] = edge.Target
	}
	return result
}

// GetDependents returns all nodes that depend on this node
func (g *Graph) GetDependents(id string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]string, 0)
	for source, edges := range g.edges {
		for _, edge := range edges {
			if edge.Target == id {
				result = append(result, source)
			}
		}
	}
	return result
}

// GetAllNodes returns all nodes
func (g *Graph) GetAllNodes() []*Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]*Node, 0, len(g.nodes))
	for _, node := range g.nodes {
		result = append(result, node)
	}
	return result
}

// GetEdges returns all edges from a source
func (g *Graph) GetEdges(source string) []*Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.edges[source]
}

// Count returns the number of nodes and edges
func (g *Graph) Count() (nodes int, edges int) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	nodes = len(g.nodes)
	for _, e := range g.edges {
		edges += len(e)
	}
	return
}
