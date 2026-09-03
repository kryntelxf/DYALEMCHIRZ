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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// handlePod handles Pod events and creates/updates Asset
func (c *Controller) handlePod(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	// Only process pods that are scheduled
	if pod.Spec.NodeName == "" {
		return
	}

	klog.V(4).Infof("Processing Pod: %s/%s", pod.Namespace, pod.Name)

	// Create Asset from Pod
	asset := c.podToAsset(pod)

	// Apply to cluster
	if err := c.applyAsset(context.Background(), pod.Namespace, asset); err != nil {
		klog.Errorf("Failed to apply asset for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
}

// handleService handles Service events
func (c *Controller) handleService(obj interface{}) {
	service, ok := obj.(*corev1.Service)
	if !ok {
		return
	}

	klog.V(4).Infof("Processing Service: %s/%s", service.Namespace, service.Name)

	// Create Asset from Service
	asset := c.serviceToAsset(service)

	// Apply to cluster
	if err := c.applyAsset(context.Background(), service.Namespace, asset); err != nil {
		klog.Errorf("Failed to apply asset for service %s/%s: %v", service.Namespace, service.Name, err)
	}
}

// handleNode handles Node events
func (c *Controller) handleNode(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}

	klog.V(4).Infof("Processing Node: %s", node.Name)

	// Create Asset from Node
	asset := c.nodeToAsset(node)

	// Apply to cluster
	if err := c.applyAsset(context.Background(), "", asset); err != nil {
		klog.Errorf("Failed to apply asset for node %s: %v", node.Name, err)
	}
}

// podToAsset converts a Pod to an Asset
func (c *Controller) podToAsset(pod *corev1.Pod) *unstructured.Unstructured {
	asset := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dya.kryntelxf.com/v1alpha1",
			"kind":       "Asset",
			"metadata": map[string]interface{}{
				"name":      fmt.Sprintf("%s-%s", pod.Namespace, pod.Name),
				"namespace": pod.Namespace,
				"labels": map[string]interface{}{
					"asset-type": "Pod",
					"namespace":  pod.Namespace,
					"node":       pod.Spec.NodeName,
				},
			},
			"spec": map[string]interface{}{
				"displayName": pod.Name,
				"assetType":   "Pod",
				"parent":      pod.Spec.NodeName,
				"labels": map[string]interface{}{
					"namespace": pod.Namespace,
					"node":      pod.Spec.NodeName,
				},
				"properties": map[string]interface{}{
					"podIP":   pod.Status.PodIP,
					"phase":   string(pod.Status.Phase),
					"node":    pod.Spec.NodeName,
					"created": pod.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
				},
				"health": map[string]interface{}{
					"status": string(pod.Status.Phase),
				},
			},
		},
	}
	return asset
}

// serviceToAsset converts a Service to an Asset
func (c *Controller) serviceToAsset(service *corev1.Service) *unstructured.Unstructured {
	asset := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dya.kryntelxf.com/v1alpha1",
			"kind":       "Asset",
			"metadata": map[string]interface{}{
				"name":      fmt.Sprintf("%s-%s", service.Namespace, service.Name),
				"namespace": service.Namespace,
				"labels": map[string]interface{}{
					"asset-type": "Service",
					"namespace":  service.Namespace,
				},
			},
			"spec": map[string]interface{}{
				"displayName": service.Name,
				"assetType":   "Service",
				"labels": map[string]interface{}{
					"namespace": service.Namespace,
				},
				"properties": map[string]interface{}{
					"clusterIP": service.Spec.ClusterIP,
					"type":      string(service.Spec.Type),
					"ports":     service.Spec.Ports,
					"created":   service.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
				},
			},
		},
	}
	return asset
}

// nodeToAsset converts a Node to an Asset
func (c *Controller) nodeToAsset(node *corev1.Node) *unstructured.Unstructured {
	asset := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "dya.kryntelxf.com/v1alpha1",
			"kind":       "Asset",
			"metadata": map[string]interface{}{
				"name": node.Name,
				"labels": map[string]interface{}{
					"asset-type": "Node",
				},
			},
			"spec": map[string]interface{}{
				"displayName": node.Name,
				"assetType":   "Node",
				"labels": map[string]interface{}{
					"node": node.Name,
				},
				"properties": map[string]interface{}{
					"nodeIP":        getNodeInternalIP(node),
					"nodeName":      node.Name,
					"architecture":  node.Status.NodeInfo.Architecture,
					"os":            node.Status.NodeInfo.OperatingSystem,
					"kubeletVersion": node.Status.NodeInfo.KubeletVersion,
					"created":       node.CreationTimestamp.Format("2006-01-02T15:04:05Z"),
				},
				"health": map[string]interface{}{
					"status": getNodeStatus(node),
				},
			},
		},
	}
	return asset
}

// applyAsset applies an Asset to the cluster
func (c *Controller) applyAsset(ctx context.Context, namespace string, asset *unstructured.Unstructured) error {
	// Check if asset already exists
	name := asset.GetName()
	namespace = asset.GetNamespace()

	existing, err := c.dynamicClient.Resource(assetGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		// Update existing asset
		// Preserve UID, ResourceVersion, etc.
		existing.Object["spec"] = asset.Object["spec"]
		_, err = c.dynamicClient.Resource(assetGVR).Namespace(namespace).Update(ctx, existing, metav1.UpdateOptions{})
		return err
	}

	// Create new asset
	_, err = c.dynamicClient.Resource(assetGVR).Namespace(namespace).Create(ctx, asset, metav1.CreateOptions{})
	return err
}

// Helper functions
func getNodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	return ""
}

func getNodeStatus(node *corev1.Node) string {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			if condition.Status == corev1.ConditionTrue {
				return "Healthy"
			}
			return "Unhealthy"
		}
	}
	return "Unknown"
}
