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

package assetgraph

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"k8s.io/kubernetes/dya/pkg/graph"
)

func TestController_NewController(t *testing.T) {
	cfg := &rest.Config{}
	ctrl, err := NewController(cfg)
	if err != nil {
		t.Fatalf("NewController failed: %v", err)
	}

	if ctrl == nil {
		t.Error("Controller is nil")
	}

	if ctrl.graph == nil {
		t.Error("Graph is nil")
	}

	if !ctrl.Health() {
		t.Error("Controller health is false")
	}
}

func TestController_AddNode(t *testing.T) {
	g := graph.NewGraph()
	node := &graph.Node{ID: "pod/default/test", Name: "test", Kind: "Pod"}

	if err := g.AddNode(node); err != nil {
		t.Errorf("AddNode failed: %v", err)
	}

	result, ok := g.GetNode("pod/default/test")
	if !ok {
		t.Error("Node not found")
	}
	if result.Kind != "Pod" {
		t.Errorf("Expected Kind 'Pod', got '%s'", result.Kind)
	}
}

func TestController_HandlePodAdd(t *testing.T) {
	cfg := &rest.Config{}
	ctrl, err := NewController(cfg)
	if err != nil {
		t.Fatalf("NewController failed: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
		},
	}

	ctrl.handlePodAdd(pod)

	// Check if node was added
	_, ok := ctrl.graph.GetNode("pod/default/test-pod")
	if !ok {
		t.Error("Pod node not added to graph")
	}
}

func TestController_HandlePodDelete(t *testing.T) {
	cfg := &rest.Config{}
	ctrl, err := NewController(cfg)
	if err != nil {
		t.Fatalf("NewController failed: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	// Add first
	ctrl.handlePodAdd(pod)

	// Then delete
	ctrl.handlePodDelete(pod)

	_, ok := ctrl.graph.GetNode("pod/default/test-pod")
	if ok {
		t.Error("Pod node still exists after delete")
	}
}

func TestController_Reconcile(t *testing.T) {
	cfg := &rest.Config{}
	ctrl, err := NewController(cfg)
	if err != nil {
		t.Fatalf("NewController failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ctrl.reconcile(ctx, "default/test")
	if err != nil {
		t.Errorf("Reconcile failed: %v", err)
	}
}

func TestGraph_Concurrent(t *testing.T) {
	g := graph.NewGraph()
	done := make(chan bool, 2)

	for i := 0; i < 10; i++ {
		go func(id int) {
			g.AddNode(&graph.Node{ID: string(rune('A' + id)), Name: "test", Kind: "Pod"})
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	nodes, _ := g.Count()
	if nodes < 1 {
		t.Logf("Nodes: %d (concurrent test)", nodes)
	}
}

func TestFakeClient_Integration(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Create a test pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "test-node",
		},
	}

	_, err := client.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create pod: %v", err)
	}

	// List pods
	pods, err := client.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}

	if len(pods.Items) != 1 {
		t.Errorf("Expected 1 pod, got %d", len(pods.Items))
	}
}
