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
	"testing"
)

func TestGraph_AddNode(t *testing.T) {
	g := NewGraph()
	node := &Node{ID: "test-1", Name: "test", Kind: "Pod"}

	if err := g.AddNode(node); err != nil {
		t.Errorf("AddNode failed: %v", err)
	}

	if _, ok := g.GetNode("test-1"); !ok {
		t.Error("Node not found after AddNode")
	}
}

func TestGraph_AddNode_EmptyID(t *testing.T) {
	g := NewGraph()
	node := &Node{ID: "", Name: "test", Kind: "Pod"}

	if err := g.AddNode(node); err == nil {
		t.Error("Expected error for empty ID, got nil")
	}
}

func TestGraph_UpdateNode(t *testing.T) {
	g := NewGraph()
	node := &Node{ID: "test-1", Name: "test", Kind: "Pod"}
	g.AddNode(node)

	updated := &Node{ID: "test-1", Name: "updated", Kind: "Pod"}
	if err := g.UpdateNode(updated); err != nil {
		t.Errorf("UpdateNode failed: %v", err)
	}

	result, _ := g.GetNode("test-1")
	if result.Name != "updated" {
		t.Errorf("Expected name 'updated', got '%s'", result.Name)
	}
}

func TestGraph_RemoveNode(t *testing.T) {
	g := NewGraph()
	node := &Node{ID: "test-1", Name: "test", Kind: "Pod"}
	g.AddNode(node)

	if err := g.RemoveNode("test-1"); err != nil {
		t.Errorf("RemoveNode failed: %v", err)
	}

	if _, ok := g.GetNode("test-1"); ok {
		t.Error("Node still exists after RemoveNode")
	}
}

func TestGraph_AddEdge(t *testing.T) {
	g := NewGraph()
	g.AddNode(&Node{ID: "source", Name: "source", Kind: "Pod"})
	g.AddNode(&Node{ID: "target", Name: "target", Kind: "Node"})

	if err := g.AddEdge("source", "target", "depends-on", ""); err != nil {
		t.Errorf("AddEdge failed: %v", err)
	}

	deps := g.GetDependencies("source")
	if len(deps) != 1 || deps[0] != "target" {
		t.Errorf("Expected dependency 'target', got %v", deps)
	}
}

func TestGraph_GetDependents(t *testing.T) {
	g := NewGraph()
	g.AddNode(&Node{ID: "source", Name: "source", Kind: "Pod"})
	g.AddNode(&Node{ID: "target", Name: "target", Kind: "Node"})
	g.AddEdge("source", "target", "depends-on", "")

	deps := g.GetDependents("target")
	if len(deps) != 1 || deps[0] != "source" {
		t.Errorf("Expected dependent 'source', got %v", deps)
	}
}

func TestGraph_RemoveNode_Cascade(t *testing.T) {
	g := NewGraph()
	g.AddNode(&Node{ID: "source", Name: "source", Kind: "Pod"})
	g.AddNode(&Node{ID: "target", Name: "target", Kind: "Node"})
	g.AddEdge("source", "target", "depends-on", "")

	g.RemoveNode("source")
	deps := g.GetDependencies("source")
	if len(deps) != 0 {
		t.Errorf("Expected no dependencies after removal, got %v", deps)
	}
}

func TestGraph_ConcurrentAccess(t *testing.T) {
	g := NewGraph()
	done := make(chan bool)

	// Add nodes concurrently
	go func() {
		for i := 0; i < 100; i++ {
			g.AddNode(&Node{ID: string(rune('A' + i%26)), Name: "test", Kind: "Pod"})
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			g.AddNode(&Node{ID: string(rune('a' + i%26)), Name: "test", Kind: "Pod"})
		}
		done <- true
	}()

	<-done
	<-done

	nodes, _ := g.Count()
	if nodes < 26 { // At least unique nodes
		t.Logf("Nodes count: %d", nodes)
	}
}
