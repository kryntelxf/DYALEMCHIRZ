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

package globaledge

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu          sync.RWMutex
	clusters    []Cluster
	regions     []Region
	gateways    []Gateway
	routers     []Router
	syncers     []GlobalSyncer
	running     bool
}

type Cluster struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	Endpoint    string    `json:"endpoint"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Region struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Location    string    `json:"location"`
	Latency     int       `json:"latency"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Gateway struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	ClusterID   string    `json:"clusterId"`
	Endpoint    string    `json:"endpoint"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Router interface {
	Route(request interface{}) (*RouteResult, error)
	Name() string
}

type GlobalSyncer interface {
	Sync() error
	Name() string
}

type RouteResult struct {
	ClusterID   string    `json:"clusterId"`
	Region      string    `json:"region"`
	Latency     int       `json:"latency"`
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		clusters: make([]Cluster, 0),
		regions:  make([]Region, 0),
		gateways: make([]Gateway, 0),
		routers:  make([]Router, 0),
		syncers:  make([]GlobalSyncer, 0),
		running:  false,
	}
}

func (e *Engine) RegisterCluster(cluster Cluster) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clusters = append(e.clusters, cluster)
	klog.Infof("Registered global cluster: %s", cluster.Name)
}

func (e *Engine) RegisterRegion(region Region) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.regions = append(e.regions, region)
	klog.Infof("Registered region: %s", region.Name)
}

func (e *Engine) RegisterGateway(gateway Gateway) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gateways = append(e.gateways, gateway)
	klog.Infof("Registered gateway: %s", gateway.Name)
}

func (e *Engine) RegisterRouter(router Router) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routers = append(e.routers, router)
	klog.Infof("Registered router: %s", router.Name())
}

func (e *Engine) RegisterSyncer(syncer GlobalSyncer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.syncers = append(e.syncers, syncer)
	klog.Infof("Registered global syncer: %s", syncer.Name())
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Global Edge Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Global Edge Engine stopped")
}

func (e *Engine) GetClusters() []Cluster {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.clusters
}

func (e *Engine) GetRegions() []Region {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.regions
}

func (e *Engine) Route(request interface{}) []*RouteResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*RouteResult, 0)
	for _, router := range e.routers {
		result, err := router.Route(request)
		if err != nil {
			klog.Errorf("Router %s failed: %v", router.Name(), err)
			continue
		}
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (e *Engine) SyncGlobal() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, syncer := range e.syncers {
		if err := syncer.Sync(); err != nil {
			klog.Errorf("Global syncer %s failed: %v", syncer.Name(), err)
		}
	}
}
