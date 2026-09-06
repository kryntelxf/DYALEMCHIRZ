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
	aidetectors "k8s.io/kubernetes/dya/pkg/ai/detectors"
	"k8s.io/kubernetes/dya/pkg/ai/predictors"
	"k8s.io/kubernetes/dya/pkg/ai/scorers"
	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/digitaltwin"
	"k8s.io/kubernetes/dya/pkg/digitaltwin/analyzers"
	"k8s.io/kubernetes/dya/pkg/digitaltwin/simulators"
	"k8s.io/kubernetes/dya/pkg/edge"
	"k8s.io/kubernetes/dya/pkg/edge/buffers"
	edgeenforcers "k8s.io/kubernetes/dya/pkg/edge/enforcers"
	"k8s.io/kubernetes/dya/pkg/edge/handlers"
	"k8s.io/kubernetes/dya/pkg/edge/syncers"
	"k8s.io/kubernetes/dya/pkg/recovery"
	"k8s.io/kubernetes/dya/pkg/recovery/executors"
	"k8s.io/kubernetes/dya/pkg/recovery/notifiers"
	recoveryverifiers "k8s.io/kubernetes/dya/pkg/recovery/verifiers"
	"k8s.io/kubernetes/dya/pkg/resilience"
	"k8s.io/kubernetes/dya/pkg/resilience/checkers"
	"k8s.io/kubernetes/dya/pkg/resilience/planners"
	"k8s.io/kubernetes/dya/pkg/security"
	"k8s.io/kubernetes/dya/pkg/security/auditors"
	securitydetectors "k8s.io/kubernetes/dya/pkg/security/detectors"
	securityenforcers "k8s.io/kubernetes/dya/pkg/security/enforcers"
	securityverifiers "k8s.io/kubernetes/dya/pkg/security/verifiers"
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
	fmt.Println("║   Phase 10: Recovery Orchestrator                           ║")
	fmt.Println("║   Version: 0.1.0                                            ║")
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	klog.Info("DYALEMCHIRZ controller starting...")

	cfg, err := getConfig()
	if err != nil {
		klog.Fatalf("Failed to get Kubernetes config: %v", err)
	}

	stopCh := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalCh
		klog.Info("Received shutdown signal, stopping...")
		close(stopCh)
	}()

	// 1. AI ENGINE
	klog.Info("Creating AI Engine...")
	aiEngine := ai.NewEngine()
	aiEngine.RegisterDetector(&aidetectors.HealthDetector{})
	aiEngine.RegisterDetector(&aidetectors.AnomalyDetector{})
	aiEngine.RegisterScorer(&scorers.RiskScorer{})
	aiEngine.RegisterScorer(&scorers.HealthScorer{})
	aiEngine.RegisterPredictor(&predictors.FailurePredictor{})
	aiEngine.RegisterPredictor(&predictors.ResourcePredictor{})
	aiEngine.Start()
	defer aiEngine.Stop()
	klog.Info("AI Engine started")

	// 2. RESILIENCE ENGINE
	klog.Info("Creating Resilience Engine...")
	resilienceEngine := resilience.NewEngine()
	resilienceEngine.RegisterHealthChecker(&checkers.HealthChecker{})
	resilienceEngine.RegisterRecoveryPlanner(&planners.RecoveryPlanner{})
	resilienceEngine.Start()
	defer resilienceEngine.Stop()
	klog.Info("Resilience Engine started")

	// 3. DIGITAL TWIN ENGINE
	klog.Info("Creating Digital Twin Engine...")
	digitalTwinEngine := digitaltwin.NewEngine()
	digitalTwinEngine.RegisterSimulator(&simulators.FailureSimulator{})
	digitalTwinEngine.RegisterAnalyzer(&analyzers.ImpactAnalyzer{})
	digitalTwinEngine.Start()
	defer digitalTwinEngine.Stop()
	klog.Info("Digital Twin Engine started")

	// 4. SECURITY ENGINE
	klog.Info("Creating Security Engine...")
	securityEngine := security.NewEngine()
	securityEngine.RegisterVerifier(&securityverifiers.IdentityVerifier{})
	securityEngine.RegisterEnforcer(&securityenforcers.PolicyEnforcer{})
	securityEngine.RegisterAuditor(&auditors.AuditLogger{})
	securityEngine.RegisterDetector(&securitydetectors.AnomalyDetector{})
	securityEngine.Start()
	defer securityEngine.Stop()
	klog.Info("Security Engine started")

	// 5. EDGE ENGINE
	klog.Info("Creating Edge Engine...")
	edgeEngine := edge.NewEngine()
	edgeEngine.RegisterHandler(&handlers.LocalHandler{})
	edgeEngine.RegisterSyncer(&syncers.Syncer{})
	edgeEngine.RegisterEnforcer(&edgeenforcers.LocalEnforcer{})
	edgeEngine.RegisterBuffer(buffers.NewBuffer())
	edgeEngine.Start()
	defer edgeEngine.Stop()
	klog.Info("Edge Engine started")

	// 6. RECOVERY ORCHESTRATOR
	klog.Info("Creating Recovery Orchestrator...")
	recoveryOrchestrator := recovery.NewOrchestrator()
	recoveryOrchestrator.RegisterExecutor(&executors.BasicExecutor{})
	recoveryOrchestrator.RegisterVerifier(&recoveryverifiers.BasicVerifier{})
	recoveryOrchestrator.RegisterNotifier(&notifiers.BasicNotifier{})
	recoveryOrchestrator.Start()
	defer recoveryOrchestrator.Stop()
	klog.Info("Recovery Orchestrator started")

	// 7. ASSET GRAPH CONTROLLER
	klog.Info("Creating Asset Graph controller...")
	assetGraphController, err := assetgraph.NewController(cfg)
	if err != nil {
		klog.Fatalf("Failed to create Asset Graph controller: %v", err)
	}

	klog.Infof("Starting Asset Graph controller with %d workers...", workers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := assetGraphController.Run(ctx, workers); err != nil {
			klog.Fatalf("Asset Graph controller failed: %v", err)
		}
	}()

	// 8. ALL COMPONENTS STARTED
	klog.Info("All components started successfully")
	klog.Info("DYALEMCHIRZ is ready")
	klog.Info("")
	klog.Info("╔══════════════════════════════════════════════════════════════╗")
	klog.Info("║  Components Running:                                         ║")
	klog.Info("║  ✅ AI Engine                                               ║")
	klog.Info("║  ✅ Resilience Engine                                       ║")
	klog.Info("║  ✅ Digital Twin Engine                                     ║")
	klog.Info("║  ✅ Security Engine                                         ║")
	klog.Info("║  ✅ Edge Engine                                             ║")
	klog.Info("║  ✅ Recovery Orchestrator                                   ║")
	klog.Info("║  ✅ Asset Graph Controller                                  ║")
	klog.Info("╚══════════════════════════════════════════════════════════════╝")
	klog.Info("")
	klog.Info("Press Ctrl+C to stop")

	<-stopCh
	klog.Info("Shutting down gracefully...")
	cancel()
	time.Sleep(2 * time.Second)
	klog.Info("Shutdown complete")
}

func getConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	return rest.InClusterConfig()
}
