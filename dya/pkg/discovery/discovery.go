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

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/graph"
)

// Discoverer discovers assets from Kubernetes
type Discoverer struct {
	client kubernetes.Interface
	graph  *graph.Graph
}

// NewDiscoverer creates a new discoverer
func NewDiscoverer(client kubernetes.Interface, g *graph.Graph) *Discoverer {
	return &Discoverer{
		client: client,
		graph:  g,
	}
}

// DiscoverNodes discovers Kubernetes Nodes
func (d *Discoverer) DiscoverNodes(ctx context.Context) error {
	nodes, err := d.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	for _, node := range nodes.Items {
		assetID := fmt.Sprintf("node/%s", node.Name)
		nodeNode := &graph.Node{
			ID:    assetID,
			Name:  node.Name,
			Kind:  "Node",
			Labels: node.Labels,
			Properties: map[string]string{
				"status":          string(getNodeStatus(&node)),
				"kubeletVersion": node.Status.NodeInfo.KubeletVersion,
				"os":              node.Status.NodeInfo.OperatingSystem,
				"architecture":    node.Status.NodeInfo.Architecture,
			},
		}

		if err := d.graph.AddNode(nodeNode); err != nil {
			klog.Errorf("Failed to add node %s: %v", node.Name, err)
		}
	}

	klog.Infof("Discovered %d nodes", len(nodes.Items))
	return nil
}

// DiscoverPods discovers Kubernetes Pods
func (d *Discoverer) DiscoverPods(ctx context.Context) error {
	pods, err := d.client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range pods.Items {
		assetID := fmt.Sprintf("pod/%s/%s", pod.Namespace, pod.Name)
		podNode := &graph.Node{
			ID:        assetID,
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Kind:      "Pod",
			Labels:    pod.Labels,
			Properties: map[string]string{
				"phase":   string(pod.Status.Phase),
				"node":    pod.Spec.NodeName,
				"podIP":   pod.Status.PodIP,
				"created": pod.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
			},
		}

		if err := d.graph.AddNode(podNode); err != nil {
			klog.Errorf("Failed to add pod %s/%s: %v", pod.Namespace, pod.Name, err)
			continue
		}

		if pod.Spec.NodeName != "" {
			nodeID := fmt.Sprintf("node/%s", pod.Spec.NodeName)
			if err := d.graph.AddEdge(assetID, nodeID, "scheduled-on", fmt.Sprintf("Pod %s scheduled on node %s", pod.Name, pod.Spec.NodeName)); err != nil {
				klog.V(4).Infof("Could not add edge from %s to %s: %v", assetID, nodeID, err)
			}
		}
	}

	klog.Infof("Discovered %d pods", len(pods.Items))
	return nil
}

// DiscoverServices discovers Kubernetes Services
func (d *Discoverer) DiscoverServices(ctx context.Context) error {
	services, err := d.client.CoreV1().Services(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}

	for _, svc := range services.Items {
		assetID := fmt.Sprintf("service/%s/%s", svc.Namespace, svc.Name)
		svcNode := &graph.Node{
			ID:        assetID,
			Name:      svc.Name,
			Namespace: svc.Namespace,
			Kind:      "Service",
			Labels:    svc.Labels,
			Properties: map[string]string{
				"type":      string(svc.Spec.Type),
				"clusterIP": svc.Spec.ClusterIP,
				"created":   svc.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
			},
		}

		if err := d.graph.AddNode(svcNode); err != nil {
			klog.Errorf("Failed to add service %s/%s: %v", svc.Namespace, svc.Name, err)
		}
	}

	klog.Infof("Discovered %d services", len(services.Items))
	return nil
}

// DiscoverDeployments discovers Kubernetes Deployments
func (d *Discoverer) DiscoverDeployments(ctx context.Context) error {
	deployments, err := d.client.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list deployments: %w", err)
	}

	for _, dep := range deployments.Items {
		assetID := fmt.Sprintf("deployment/%s/%s", dep.Namespace, dep.Name)
		replicas := int32(0)
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		depNode := &graph.Node{
			ID:        assetID,
			Name:      dep.Name,
			Namespace: dep.Namespace,
			Kind:      "Deployment",
			Labels:    dep.Labels,
			Properties: map[string]string{
				"replicas":  fmt.Sprintf("%d", replicas),
				"available": fmt.Sprintf("%d", dep.Status.AvailableReplicas),
				"ready":     fmt.Sprintf("%d", dep.Status.ReadyReplicas),
				"updated":   fmt.Sprintf("%d", dep.Status.UpdatedReplicas),
				"created":   dep.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
			},
		}

		if err := d.graph.AddNode(depNode); err != nil {
			klog.Errorf("Failed to add deployment %s/%s: %v", dep.Namespace, dep.Name, err)
			continue
		}
	}

	klog.Infof("Discovered %d deployments", len(deployments.Items))
	return nil
}

// DiscoverAll discovers all Kubernetes resources
func (d *Discoverer) DiscoverAll(ctx context.Context) error {
	if err := d.DiscoverNodes(ctx); err != nil {
		return err
	}
	if err := d.DiscoverPods(ctx); err != nil {
		return err
	}
	if err := d.DiscoverServices(ctx); err != nil {
		return err
	}
	if err := d.DiscoverDeployments(ctx); err != nil {
		return err
	}
	return nil
}

func getNodeStatus(node *v1.Node) v1.ConditionStatus {
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady {
			return cond.Status
		}
	}
	return v1.ConditionUnknown
}
