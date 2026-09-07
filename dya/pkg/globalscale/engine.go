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

package globalscale

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu             sync.RWMutex
	regions        []Region
	clusters       []Cluster
	loadBalancers  []LoadBalancer
	cacheNodes     []CacheNode
	monitors       []Monitor
	autoScalers    []AutoScaler
	disasterRecovery []DisasterRecovery
	running        bool
}

type Region struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Location    string    `json:"location"`
	Status      string    `json:"status"`
	Capacity    int64     `json:"capacity"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Cluster struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	Nodes       int       `json:"nodes"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type LoadBalancer struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	Endpoint    string    `json:"endpoint"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type CacheNode struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	Size        int64     `json:"size"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Monitor struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	Type        string    `json:"type"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type AutoScaler struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	MinNodes    int       `json:"minNodes"`
	MaxNodes    int       `json:"maxNodes"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

type DisasterRecovery struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Region      string    `json:"region"`
	BackupRegion string   `json:"backupRegion"`
	RPO         string    `json:"rpo"`
	RTO         string    `json:"rto"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
}

func NewEngine() *Engine {
	return &Engine{
		regions:        make([]Region, 0),
		clusters:       make([]Cluster, 0),
		loadBalancers:  make([]LoadBalancer, 0),
		cacheNodes:     make([]CacheNode, 0),
		monitors:       make([]Monitor, 0),
		autoScalers:    make([]AutoScaler, 0),
		disasterRecovery: make([]DisasterRecovery, 0),
		running:        false,
	}
}

func (e *Engine) RegisterRegion(region Region) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.regions = append(e.regions, region)
	klog.Infof("Registered global region: %s", region.Name)
}

func (e *Engine) RegisterCluster(cluster Cluster) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clusters = append(e.clusters, cluster)
	klog.Infof("Registered global cluster: %s", cluster.Name)
}

func (e *Engine) RegisterLoadBalancer(lb LoadBalancer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.loadBalancers = append(e.loadBalancers, lb)
	klog.Infof("Registered global load balancer: %s", lb.Name)
}

func (e *Engine) RegisterCacheNode(cache CacheNode) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cacheNodes = append(e.cacheNodes, cache)
	klog.Infof("Registered cache node: %s", cache.Name)
}

func (e *Engine) RegisterMonitor(monitor Monitor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.monitors = append(e.monitors, monitor)
	klog.Infof("Registered global monitor: %s", monitor.Name)
}

func (e *Engine) RegisterAutoScaler(autoScaler AutoScaler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.autoScalers = append(e.autoScalers, autoScaler)
	klog.Infof("Registered auto-scaler: %s", autoScaler.Name)
}

func (e *Engine) RegisterDisasterRecovery(dr DisasterRecovery) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.disasterRecovery = append(e.disasterRecovery, dr)
	klog.Infof("Registered disaster recovery plan: %s", dr.Name)
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Global Scale Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Global Scale Engine stopped")
}

func (e *Engine) GetRegions() []Region {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.regions
}

func (e *Engine) GetClusters() []Cluster {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.clusters
}

func (e *Engine) GetLoadBalancers() []LoadBalancer {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.loadBalancers
}

func (e *Engine) GetCacheNodes() []CacheNode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cacheNodes
}

func (e *Engine) GetMonitors() []Monitor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.monitors
}

func (e *Engine) GetAutoScalers() []AutoScaler {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.autoScalers
}

func (e *Engine) GetDisasterRecovery() []DisasterRecovery {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.disasterRecovery
}
