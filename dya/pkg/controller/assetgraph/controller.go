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
	"sync/atomic"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/discovery"
	"k8s.io/kubernetes/dya/pkg/event"
	"k8s.io/kubernetes/dya/pkg/graph"
	"k8s.io/kubernetes/dya/pkg/metrics"
)

// Controller is the asset graph controller
type Controller struct {
	kubeClient           kubernetes.Interface
	dynamicClient        dynamic.Interface
	graph                *graph.Graph
	discoverer           *discovery.Discoverer
	pipeline             *event.Pipeline
	workqueue            workqueue.RateLimitingInterface
	informerFactory      informers.SharedInformerFactory
	reconciliationCount  int64
	reconciliationErrors int64
}

// NewController creates a new controller
func NewController(config *rest.Config) (*Controller, error) {
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic clientset: %w", err)
	}

	g := graph.NewGraph()
	discoverer := discovery.NewDiscoverer(kubeClient, g)
	pipeline := event.NewPipeline()

	ctrl := &Controller{
		kubeClient:      kubeClient,
		dynamicClient:   dynamicClient,
		graph:           g,
		discoverer:      discoverer,
		pipeline:        pipeline,
		workqueue:       workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		informerFactory: informers.NewSharedInformerFactory(kubeClient, 30*time.Second),
	}

	ctrl.registerEventHandlers()

	return ctrl, nil
}

// registerEventHandlers registers Kubernetes event handlers
func (c *Controller) registerEventHandlers() {
	// Watch Pods
	podInformer := c.informerFactory.Core().V1().Pods()
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handlePodAdd,
		UpdateFunc: c.handlePodUpdate,
		DeleteFunc: c.handlePodDelete,
	})

	// Watch Nodes
	nodeInformer := c.informerFactory.Core().V1().Nodes()
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handleNodeAdd,
		UpdateFunc: c.handleNodeUpdate,
		DeleteFunc: c.handleNodeDelete,
	})

	// Watch Services
	serviceInformer := c.informerFactory.Core().V1().Services()
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.handleServiceAdd,
		UpdateFunc: c.handleServiceUpdate,
		DeleteFunc: c.handleServiceDelete,
	})
}

// Run starts the controller
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer c.workqueue.ShutDown()

	klog.Info("Starting Asset Graph controller")

	// Initial discovery
	klog.Info("Running initial asset discovery...")
	if err := c.discoverer.DiscoverAll(ctx); err != nil {
		klog.Errorf("Initial discovery failed: %v", err)
	}

	// Start informers
	c.informerFactory.Start(ctx.Done())

	// Wait for cache sync
	if !cache.WaitForCacheSync(ctx.Done(),
		c.informerFactory.Core().V1().Pods().Informer().HasSynced,
		c.informerFactory.Core().V1().Nodes().Informer().HasSynced,
		c.informerFactory.Core().V1().Services().Informer().HasSynced,
	) {
		return fmt.Errorf("failed to sync informer caches")
	}

	klog.Info("Informers synced")

	// Start workers
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	// Update metrics periodically
	go c.updateMetricsLoop(ctx)

	klog.Infof("Started %d workers", workers)
	<-ctx.Done()
	klog.Info("Shutting down workers")

	return nil
}

// runWorker processes work items
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem processes a work item from the queue
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}
	defer c.workqueue.Done(obj)

	key, ok := obj.(string)
	if !ok {
		c.workqueue.Forget(obj)
		return true
	}

	if err := c.reconcile(ctx, key); err != nil {
		c.workqueue.AddRateLimited(key)
		atomic.AddInt64(&c.reconciliationErrors, 1)
		return true
	}

	c.workqueue.Forget(obj)
	atomic.AddInt64(&c.reconciliationCount, 1)
	return true
}

// reconcile reconciles an asset
func (c *Controller) reconcile(ctx context.Context, key string) error {
	_, _, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("invalid key: %s", key)
	}
	klog.V(4).Infof("Reconciling: %s", key)
	return nil
}

// updateMetricsLoop updates metrics periodically
func (c *Controller) updateMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update graph metrics
			nodes, edges := c.graph.Count()
			metrics.SetGraphMetrics(float64(nodes), float64(edges))

			// Update reconciliation metrics
			metrics.SetReconciliationMetrics(
				float64(atomic.LoadInt64(&c.reconciliationCount)),
				float64(atomic.LoadInt64(&c.reconciliationErrors)),
			)

			// Update event metrics
			eventMetrics := c.pipeline.GetMetrics()
			metrics.SetEventMetrics(
				float64(eventMetrics.Total),
				float64(eventMetrics.Processed),
				float64(eventMetrics.Failed),
			)
		}
	}
}

// GetGraph returns the asset graph
func (c *Controller) GetGraph() *graph.Graph {
	return c.graph
}

// GetPipeline returns the event pipeline
func (c *Controller) GetPipeline() *event.Pipeline {
	return c.pipeline
}

// KubeClient returns the Kubernetes client
func (c *Controller) KubeClient() kubernetes.Interface {
	return c.kubeClient
}

// --- Event Handlers ---

// handlePodAdd handles Pod addition
func (c *Controller) handlePodAdd(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	c.workqueue.Add(key)

	// Create event
	evt := &event.Event{
		ID:        fmt.Sprintf("pod-add-%s-%d", key, time.Now().UnixNano()),
		Source:    "kubernetes",
		Type:      "create",
		AssetID:   fmt.Sprintf("pod/%s", key),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"pod":       pod.Name,
			"namespace": pod.Namespace,
			"node":      pod.Spec.NodeName,
		},
	}
	_ = c.pipeline.Process(context.Background(), evt)
}

// handlePodUpdate handles Pod update
func (c *Controller) handlePodUpdate(old, new interface{}) {
	pod, ok := new.(*v1.Pod)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	c.workqueue.Add(key)
}

// handlePodDelete handles Pod deletion
func (c *Controller) handlePodDelete(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	assetID := fmt.Sprintf("pod/%s", key)

	// Remove from graph
	_ = c.graph.RemoveNode(assetID)

	// Create event
	evt := &event.Event{
		ID:        fmt.Sprintf("pod-delete-%s-%d", key, time.Now().UnixNano()),
		Source:    "kubernetes",
		Type:      "delete",
		AssetID:   assetID,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"pod":       pod.Name,
			"namespace": pod.Namespace,
		},
	}
	_ = c.pipeline.Process(context.Background(), evt)
}

// handleNodeAdd handles Node addition
func (c *Controller) handleNodeAdd(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}
	c.workqueue.Add(node.Name)
}

// handleNodeUpdate handles Node update
func (c *Controller) handleNodeUpdate(old, new interface{}) {
	node, ok := new.(*v1.Node)
	if !ok {
		return
	}
	c.workqueue.Add(node.Name)
}

// handleNodeDelete handles Node deletion
func (c *Controller) handleNodeDelete(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}
	_ = c.graph.RemoveNode(fmt.Sprintf("node/%s", node.Name))
}

// handleServiceAdd handles Service addition
func (c *Controller) handleServiceAdd(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s/%s", svc.Namespace, svc.Name)
	c.workqueue.Add(key)
}

// handleServiceUpdate handles Service update
func (c *Controller) handleServiceUpdate(old, new interface{}) {
	svc, ok := new.(*v1.Service)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s/%s", svc.Namespace, svc.Name)
	c.workqueue.Add(key)
}

// handleServiceDelete handles Service deletion
func (c *Controller) handleServiceDelete(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		return
	}
	_ = c.graph.RemoveNode(fmt.Sprintf("service/%s/%s", svc.Namespace, svc.Name))
}
