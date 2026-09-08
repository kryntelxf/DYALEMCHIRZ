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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/health"
	"k8s.io/kubernetes/dya/pkg/impact"
	"k8s.io/kubernetes/dya/pkg/metrics"
	"k8s.io/kubernetes/dya/pkg/query"
	"k8s.io/kubernetes/dya/pkg/storage"
)

var (
	masterURL  string
	kubeconfig string
	workers    int
	healthPort int
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.IntVar(&workers, "workers", 1, "Number of worker threads for controllers.")
	flag.IntVar(&healthPort, "health-port", 8080, "Port for health and metrics endpoints.")
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                                                              ║")
	fmt.Println("║   🚀  DYALEMCHIRZ CONTROLLER  🚀                             ║")
	fmt.Println("║   AI-Native Resilience Operating Platform                    ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║   Stage 2: Asset Graph Enhancement                          ║")
	fmt.Println("║   Version: 0.3.0                                            ║")
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	klog.Info("DYALEMCHIRZ controller starting...")

	// Get Kubernetes config
	cfg, err := getConfig()
	if err != nil {
		klog.Fatalf("Failed to get Kubernetes config: %v", err)
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalCh
		klog.Info("Received shutdown signal, stopping...")
		cancel()
	}()

	// Create health checker
	healthChecker := health.NewChecker()

	// Create and start controller
	klog.Info("Creating Asset Graph controller...")
	controller, err := assetgraph.NewController(cfg)
	if err != nil {
		klog.Fatalf("Failed to create controller: %v", err)
	}

	// Initialize storage
	storageStore := storage.NewGraphStore(controller.KubeClient())

	// Initialize impact analyzer
	impactAnalyzer := impact.NewAnalyzer(controller.GetGraph())

	// Initialize query API
	queryAPI := query.NewAPI(controller.GetGraph())

	// Load graph from storage
	klog.Info("Loading graph from storage...")
	if _, err := storageStore.Load(ctx); err != nil {
		klog.Warningf("Failed to load graph from storage: %v", err)
	}

	// Mark components as healthy
	healthChecker.SetComponent("controller", true)
	healthChecker.SetComponent("kubernetes-api", true)
	healthChecker.SetComponent("storage", true)
	healthChecker.SetReady(true)

	// Start health server
	go startHealthServer(healthPort, healthChecker, controller, impactAnalyzer, queryAPI)

	// Run controller
	klog.Infof("Starting Asset Graph controller with %d workers...", workers)
	if err := controller.Run(ctx, workers); err != nil {
		klog.Fatalf("Controller failed: %v", err)
	}

	// Save graph to storage before shutdown
	klog.Info("Saving graph to storage...")
	if err := storageStore.Save(ctx, controller.GetGraph()); err != nil {
		klog.Errorf("Failed to save graph: %v", err)
	}

	klog.Info("Controller shutdown complete")
}

// startHealthServer starts the health endpoint
func startHealthServer(port int, checker *health.Checker, controller *assetgraph.Controller, impactAnalyzer *impact.Analyzer, queryAPI *query.API) {
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if checker.IsHealthy() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("unhealthy"))
		}
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if checker.IsReady() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ready"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
		}
	})

	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsData := metrics.GetMetrics()
		w.Header().Set("Content-Type", "text/plain")
		for name, value := range metricsData {
			fmt.Fprintf(w, "# HELP %s DYALEMCHIRZ metric\n", name)
			fmt.Fprintf(w, "# TYPE %s gauge\n", name)
			fmt.Fprintf(w, "%s %f\n", name, value)
		}
	})

	// Graph query endpoints
	mux.HandleFunc("/api/graph/nodes", func(w http.ResponseWriter, r *http.Request) {
		nodes := queryAPI.GetAllNodes()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"nodes\": %d, \"data\": %v}\n", len(nodes), nodes)
	})

	mux.HandleFunc("/api/graph/dependencies", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		deps := queryAPI.GetDependencies(assetID)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"dependencies\": %v}\n", assetID, deps)
	})

	mux.HandleFunc("/api/graph/dependents", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		deps := queryAPI.GetDependents(assetID)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"dependents\": %v}\n", assetID, deps)
	})

	// Impact analysis endpoint
	mux.HandleFunc("/api/impact/analyze", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		result := impactAnalyzer.Analyze(assetID)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"impact\": %v}\n", assetID, result)
	})

	mux.HandleFunc("/api/nodes/by-kind", func(w http.ResponseWriter, r *http.Request) {
		kind := r.URL.Query().Get("kind")
		if kind == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing kind parameter"))
			return
		}
		nodes := queryAPI.GetNodesByKind(kind)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"kind\": \"%s\", \"count\": %d, \"nodes\": %v}\n", kind, len(nodes), nodes)
	})

	addr := fmt.Sprintf(":%d", port)
	klog.Infof("Health server listening on %s", addr)

	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		klog.Errorf("Health server failed: %v", err)
	}
}

// getConfig returns the rest.Config
func getConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	if _, err := rest.InClusterConfig(); err == nil {
		return rest.InClusterConfig()
	}
	return nil, fmt.Errorf("could not get Kubernetes config. Use -kubeconfig or run in-cluster")
}
