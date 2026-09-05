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
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/dya/pkg/ai"
	"k8s.io/kubernetes/dya/pkg/ai/detectors"
	"k8s.io/kubernetes/dya/pkg/ai/predictors"
	"k8s.io/kubernetes/dya/pkg/ai/scorers"
	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/resilience"
	"k8s.io/kubernetes/dya/pkg/resilience/checkers"
	"k8s.io/kubernetes/dya/pkg/resilience/planners"
)

var (
	masterURL  string
	kubeconfig string
	workers    int
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.IntVar(&workers, "workers", 1, "Number of worker threads for controllers.")
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                                                              ║")
	fmt.Println("║   🚀  DYALEMCHIRZ CONTROLLER  🚀                             ║")
	fmt.Println("║   AI-Native Resilience Operating Platform                    ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║   Phase 6: Resilience Engine                                ║")
	fmt.Println("║   Version: 0.1.0                                            ║")
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	klog.Info("DYALEMCHIRZ controller starting...")

	// Get Kubernetes config
	cfg, err := getConfig()
	if err != nil {
		klog.Fatalf("Failed to get Kubernetes config: %v", err)
	}

	// Setup signal handling for graceful shutdown
	stopCh := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalCh
		klog.Info("Received shutdown signal, stopping...")
		close(stopCh)
	}()

	// ============================================
	// 1. CREATE AI ENGINE
	// ============================================
	klog.Info("Creating AI Engine...")
	aiEngine := ai.NewEngine()

	// Register AI components
	klog.Info("Registering AI components...")
	aiEngine.RegisterDetector(&detectors.HealthDetector{})
	aiEngine.RegisterDetector(&detectors.AnomalyDetector{})
	aiEngine.RegisterScorer(&scorers.RiskScorer{})
	aiEngine.RegisterScorer(&scorers.HealthScorer{})
	aiEngine.RegisterPredictor(&predictors.FailurePredictor{})
	aiEngine.RegisterPredictor(&predictors.ResourcePredictor{})

	// Start AI Engine
	klog.Info("Starting AI Engine...")
	aiEngine.Start()
	defer aiEngine.Stop()
	klog.Info("AI Engine started successfully")

	// ============================================
	// 2. CREATE RESILIENCE ENGINE
	// ============================================
	klog.Info("Creating Resilience Engine...")
	resilienceEngine := resilience.NewEngine()

	// Register Resilience components
	klog.Info("Registering Resilience components...")
	resilienceEngine.RegisterHealthChecker(&checkers.HealthChecker{})
	resilienceEngine.RegisterRecoveryPlanner(&planners.RecoveryPlanner{})

	// Start Resilience Engine
	klog.Info("Starting Resilience Engine...")
	resilienceEngine.Start()
	defer resilienceEngine.Stop()
	klog.Info("Resilience Engine started successfully")

	// ============================================
	// 3. CREATE ASSET GRAPH CONTROLLER
	// ============================================
	klog.Info("Creating Asset Graph controller...")
	assetGraphController, err := assetgraph.NewController(cfg)
	if err != nil {
		klog.Fatalf("Failed to create Asset Graph controller: %v", err)
	}

	// Start Asset Graph controller
	klog.Infof("Starting Asset Graph controller with %d workers...", workers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := assetGraphController.Run(ctx, workers); err != nil {
			klog.Fatalf("Asset Graph controller failed: %v", err)
		}
	}()

	// ============================================
	// 4. ALL COMPONENTS STARTED
	// ============================================
	klog.Info("All components started successfully")
	klog.Info("DYALEMCHIRZ is ready")
	klog.Info("")
	klog.Info("╔══════════════════════════════════════════════════════════════╗")
	klog.Info("║  Components Running:                                         ║")
	klog.Info("║  ✅ AI Engine (anomaly detection, risk scoring, prediction)   ║")
	klog.Info("║  ✅ Resilience Engine (health, failure, recovery)             ║")
	klog.Info("║  ✅ Asset Graph Controller                                   ║")
	klog.Info("╚══════════════════════════════════════════════════════════════╝")
	klog.Info("")
	klog.Info("Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-stopCh
	klog.Info("Shutting down gracefully...")
	cancel()

	// Give controllers time to clean up
	time.Sleep(2 * time.Second)
	klog.Info("Shutdown complete")
}

// getConfig returns the rest.Config for the Kubernetes API server
func getConfig() (*rest.Config, error) {
	// Use kubeconfig if provided, otherwise use in-cluster config
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	return rest.InClusterConfig()
}
