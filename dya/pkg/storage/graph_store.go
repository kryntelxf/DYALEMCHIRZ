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

package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/graph"
)

// GraphStore stores and retrieves graph data
type GraphStore struct {
	client kubernetes.Interface
}

// NewGraphStore creates a new graph store
func NewGraphStore(client kubernetes.Interface) *GraphStore {
	return &GraphStore{client: client}
}

// Save saves the graph to storage
func (s *GraphStore) Save(ctx context.Context, g *graph.Graph) error {
	if g == nil {
		return fmt.Errorf("graph is nil")
	}
	data, err := json.Marshal(g.GetAllNodes())
	if err != nil {
		return fmt.Errorf("failed to marshal graph: %w", err)
	}
	klog.V(4).Infof("Graph data size: %d bytes (simulated save)", len(data))
	return nil
}

// Load loads the graph from storage
func (s *GraphStore) Load(ctx context.Context) (*graph.Graph, error) {
	klog.V(4).Info("Loading graph from storage (simulated)")
	return graph.NewGraph(), nil
}
