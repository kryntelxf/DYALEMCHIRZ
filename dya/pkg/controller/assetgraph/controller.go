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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

var (
	assetGVR = schema.GroupVersionResource{
		Group:    "dya.kryntelxf.com",
		Version:  "v1alpha1",
		Resource: "assets",
	}
)

// Controller is the controller for Asset Graph
type Controller struct {
	// kubeClient is the Kubernetes clientset
	kubeClient kubernetes.Interface

	// dynamicClient is the dynamic clientset for CRD resources
	dynamicClient dynamic.Interface

	// assetInformer is the informer for Asset resources
	assetInformer cache.SharedIndexInformer

	// coreInformerFactory is the informer factory for core resources
	coreInformerFactory informers.SharedInformerFactory

	// workqueue is a rate limited work queue
	workqueue workqueue.RateLimitingInterface
}

// NewController returns a new Asset Graph controller
func NewController(config *rest.Config) (*Controller, error) {
	// Create Kubernetes clientset
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %v", err)
	}

	// Create dynamic clientset
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic clientset: %v", err)
	}

	// Create dynamic informer factory
	dynamicFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, time.Second*30)

	// Get Asset informer
	assetInformer := dynamicFactory.ForResource(assetGVR).Informer()

	// Create core informer factory
	coreInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)

	// Create workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	controller := &Controller{
		kubeClient:           kubeClient,
		dynamicClient:        dynamicClient,
		assetInformer:        assetInformer,
		coreInformerFactory:  coreInformerFactory,
		workqueue:            queue,
	}

	// Set up event handlers for Asset resources
	assetInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueAsset,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueAsset(new)
		},
		DeleteFunc: controller.enqueueAsset,
	})

	// Set up event handlers for Pods
	podInformer := coreInformerFactory.Core().V1().Pods()
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handlePod,
		UpdateFunc: func(old, new interface{}) { controller.handlePod(new) },
		DeleteFunc: controller.handlePodDelete,
	})

	// Set up event handlers for Services
	serviceInformer := coreInformerFactory.Core().V1().Services()
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleService,
		UpdateFunc: func(old, new interface{}) { controller.handleService(new) },
		DeleteFunc: controller.handleServiceDelete,
	})

	// Set up event handlers for Nodes
	nodeInformer := coreInformerFactory.Core().V1().Nodes()
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleNode,
		UpdateFunc: func(old, new interface{}) { controller.handleNode(new) },
		DeleteFunc: controller.handleNodeDelete,
	})

	return controller, nil
}

// Run starts the controller
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Info("Starting Asset Graph controller")

	// Start informer factories
	c.coreInformerFactory.Start(ctx.Done())
	c.assetInformer.GetController().Run(ctx.Done())

	// Wait for informer caches to sync
	klog.Info("Waiting for informer caches to sync...")
	if ok := cache.WaitForCacheSync(ctx.Done(), c.assetInformer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	if ok := cache.WaitForCacheSync(ctx.Done(),
		c.coreInformerFactory.Core().V1().Pods().Informer().HasSynced,
		c.coreInformerFactory.Core().V1().Services().Informer().HasSynced,
		c.coreInformerFactory.Core().V1().Nodes().Informer().HasSynced,
	); !ok {
		return fmt.Errorf("failed to wait for core caches to sync")
	}
	klog.Info("Informer caches synced")

	klog.Infof("Starting %d workers", workers)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	klog.Info("Started workers")
	<-ctx.Done()
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is the worker function
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

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)

		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := c.reconcile(ctx, key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		c.workqueue.Forget(obj)
		klog.V(4).Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// reconcile reconciles the Asset
func (c *Controller) reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Asset from informer cache
	obj, exists, err := c.assetInformer.GetStore().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		klog.V(4).Infof("Asset %s/%s has been deleted", namespace, name)
		return nil
	}

	asset, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("object is not an unstructured: %v", obj)
	}

	klog.V(4).Infof("Reconciling Asset %s/%s", namespace, name)

	// Update status
	if err := c.updateAssetStatus(ctx, namespace, name, asset); err != nil {
		return err
	}

	return nil
}

// updateAssetStatus updates the status of an Asset
func (c *Controller) updateAssetStatus(ctx context.Context, namespace, name string, asset *unstructured.Unstructured) error {
	// Get the current asset from API (not cache)
	currentAsset, err := c.dynamicClient.Resource(assetGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("Asset %s/%s not found", namespace, name)
			return nil
		}
		return fmt.Errorf("failed to get asset: %v", err)
	}

	// Update status
	status := map[string]interface{}{
		"observedGeneration": asset.GetGeneration(),
		"lastSync":           metav1.Now().Format(time.RFC3339),
	}

	if err := unstructured.SetNestedField(currentAsset.Object, status, "status"); err != nil {
		return fmt.Errorf("failed to set status: %v", err)
	}

	_, err = c.dynamicClient.Resource(assetGVR).Namespace(namespace).UpdateStatus(ctx, currentAsset, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update status: %v", err)
	}

	klog.V(4).Infof("Updated status for Asset %s/%s", namespace, name)
	return nil
}

// enqueueAsset adds an Asset to the workqueue
func (c *Controller) enqueueAsset(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

// ==================== DELETE HANDLERS ====================

// handlePodDelete handles Pod deletion
func (c *Controller) handlePodDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	klog.V(4).Infof("Pod deleted: %s/%s", pod.Namespace, pod.Name)
	// Optionally delete the Asset or mark as deleted
}

// handleServiceDelete handles Service deletion
func (c *Controller) handleServiceDelete(obj interface{}) {
	service, ok := obj.(*corev1.Service)
	if !ok {
		return
	}
	klog.V(4).Infof("Service deleted: %s/%s", service.Namespace, service.Name)
}

// handleNodeDelete handles Node deletion
func (c *Controller) handleNodeDelete(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	klog.V(4).Infof("Node deleted: %s", node.Name)
}
