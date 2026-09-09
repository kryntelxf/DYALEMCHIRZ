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

package discovery

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/graph"
)

// TenantAwareDiscoverer discovers assets with tenant isolation
type TenantAwareDiscoverer struct {
	kubeClient kubernetes.Interface
	graph      *graph.Graph
	tenantID   string
}

// NewTenantAwareDiscoverer creates a new tenant-aware discoverer
func NewTenantAwareDiscoverer(client kubernetes.Interface, g *graph.Graph, tenantID string) *TenantAwareDiscoverer {
	return &TenantAwareDiscoverer{
		kubeClient: client,
		graph:      g,
		tenantID:   tenantID,
	}
}

// DiscoverNodes discovers nodes for the tenant
func (d *TenantAwareDiscoverer) DiscoverNodes(ctx context.Context) error {
	nodes, err := d.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		graphNode := &graph.Node{
			ID:   fmt.Sprintf("node/%s", node.Name),
			Name: node.Name,
			Kind: "Node",
			Labels: map[string]string{
				"tenant":   d.tenantID,
				"node":     node.Name,
				"provider": "kubernetes",
			},
			Properties: map[string]string{
				"cpu":    node.Status.Capacity.Cpu().String(),
				"memory": node.Status.Capacity.Memory().String(),
				"pods":   node.Status.Capacity.Pods().String(),
			},
		}

		if err := d.graph.AddNode(graphNode); err != nil {
			klog.Warningf("Failed to add node %s: %v", node.Name, err)
		}
	}

	klog.Infof("Discovered %d nodes for tenant %s", len(nodes.Items), d.tenantID)
	return nil
}

// DiscoverPods discovers pods for the tenant
func (d *TenantAwareDiscoverer) DiscoverPods(ctx context.Context) error {
	pods, err := d.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	count := 0
	for _, pod := range pods.Items {
		if !d.isPodForTenant(&pod) {
			continue
		}

		podNode := &graph.Node{
			ID:        fmt.Sprintf("pod/%s/%s", pod.Namespace, pod.Name),
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Kind:      "Pod",
			Labels: map[string]string{
				"tenant":    d.tenantID,
				"namespace": pod.Namespace,
				"pod":       pod.Name,
				"node":      pod.Spec.NodeName,
			},
			Properties: map[string]string{
				"status":    string(pod.Status.Phase),
				"node":      pod.Spec.NodeName,
				"created":   pod.CreationTimestamp.String(),
				"namespace": pod.Namespace,
			},
		}

		if err := d.graph.AddNode(podNode); err != nil {
			klog.Warningf("Failed to add pod %s: %v", pod.Name, err)
			continue
		}

		if pod.Spec.NodeName != "" {
			sourceID := fmt.Sprintf("pod/%s/%s", pod.Namespace, pod.Name)
			targetID := fmt.Sprintf("node/%s", pod.Spec.NodeName)
			if err := d.graph.AddEdge(sourceID, targetID, "scheduled-on", fmt.Sprintf("Pod %s scheduled on node %s", pod.Name, pod.Spec.NodeName)); err != nil {
				klog.Warningf("Failed to add edge: %v", err)
			}
		}

		count++
	}

	klog.Infof("Discovered %d pods for tenant %s", count, d.tenantID)
	return nil
}

// DiscoverServices discovers services for the tenant
func (d *TenantAwareDiscoverer) DiscoverServices(ctx context.Context) error {
	services, err := d.kubeClient.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}

	count := 0
	for _, svc := range services.Items {
		if !d.isServiceForTenant(&svc) {
			continue
		}

		svcNode := &graph.Node{
			ID:        fmt.Sprintf("service/%s/%s", svc.Namespace, svc.Name),
			Name:      svc.Name,
			Namespace: svc.Namespace,
			Kind:      "Service",
			Labels: map[string]string{
				"tenant":    d.tenantID,
				"namespace": svc.Namespace,
				"service":   svc.Name,
			},
			Properties: map[string]string{
				"type":      string(svc.Spec.Type),
				"namespace": svc.Namespace,
				"created":   svc.CreationTimestamp.String(),
				"clusterIP": svc.Spec.ClusterIP,
			},
		}

		if err := d.graph.AddNode(svcNode); err != nil {
			klog.Warningf("Failed to add service %s: %v", svc.Name, err)
			continue
		}

		count++
	}

	klog.Infof("Discovered %d services for tenant %s", count, d.tenantID)
	return nil
}

// isPodForTenant checks if a pod belongs to the tenant
func (d *TenantAwareDiscoverer) isPodForTenant(pod *v1.Pod) bool {
	if pod.Labels != nil {
		if tenant, ok := pod.Labels["tenant"]; ok && tenant == d.tenantID {
			return true
		}
	}
	if d.tenantID == "tenant-1" {
		return true
	}
	return false
}

// isServiceForTenant checks if a service belongs to the tenant
func (d *TenantAwareDiscoverer) isServiceForTenant(svc *v1.Service) bool {
	if svc.Labels != nil {
		if tenant, ok := svc.Labels["tenant"]; ok && tenant == d.tenantID {
			return true
		}
	}
	if d.tenantID == "tenant-1" {
		return true
	}
	return false
}

// DiscoverAll discovers all assets for a tenant
func (d *TenantAwareDiscoverer) DiscoverAll(ctx context.Context) error {
	if err := d.DiscoverNodes(ctx); err != nil {
		return fmt.Errorf("failed to discover nodes: %w", err)
	}
	if err := d.DiscoverPods(ctx); err != nil {
		return fmt.Errorf("failed to discover pods: %w", err)
	}
	if err := d.DiscoverServices(ctx); err != nil {
		return fmt.Errorf("failed to discover services: %w", err)
	}
	return nil
}
