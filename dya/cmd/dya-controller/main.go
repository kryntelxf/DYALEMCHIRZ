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

	"k8s.io/kubernetes/dya/pkg/ai"
	"k8s.io/kubernetes/dya/pkg/ai/detectors"
	"k8s.io/kubernetes/dya/pkg/ai/predictors"
	"k8s.io/kubernetes/dya/pkg/ai/scorers"
	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/event"
	"k8s.io/kubernetes/dya/pkg/health"
	"k8s.io/kubernetes/dya/pkg/impact"
	"k8s.io/kubernetes/dya/pkg/metrics"
	"k8s.io/kubernetes/dya/pkg/query"
	"k8s.io/kubernetes/dya/pkg/resilience"
	"k8s.io/kubernetes/dya/pkg/resilience/checkers"
	"k8s.io/kubernetes/dya/pkg/resilience/detectors"
	"k8s.io/kubernetes/dya/pkg/resilience/planners"
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
	fmt.Println("║   Stage 5: Resilience Engine                                ║")
	fmt.Println("║   Version: 0.6.0                                            ║")
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

	// ============================================
	// EVENT INTELLIGENCE COMPONENTS
	// ============================================

	eventStore := event.NewStore(10000)
	normalizer := event.NewNormalizer()
	correlator := event.NewCorrelator(5 * time.Minute)
	analyzer := event.NewAnalyzer(10)

	// ============================================
	// AI ENGINE COMPONENTS
	// ============================================

	klog.Info("Creating AI Engine...")
	aiEngine := ai.NewEngine()

	klog.Info("Registering AI components...")
	aiEngine.RegisterDetector(detectors.NewStatisticalDetector(2.0, 10))
	aiEngine.RegisterScorer(scorers.NewRiskScorer())
	aiEngine.RegisterPredictor(predictors.NewSimplePredictor())

	klog.Info("Starting AI Engine...")
	aiEngine.Start()
	defer aiEngine.Stop()
	klog.Info("AI Engine started successfully")

	// ============================================
	// RESILIENCE ENGINE COMPONENTS
	// ============================================

	klog.Info("Creating Resilience Engine...")
	resilienceEngine := resilience.NewEngine()

	klog.Info("Registering Resilience components...")
	resilienceEngine.RegisterHealthChecker(&checkers.BasicChecker{})
	resilienceEngine.RegisterFailureDetector(&detectors.BasicDetector{})
	resilienceEngine.RegisterRecoveryPlanner(&planners.BasicPlanner{})

	klog.Info("Starting Resilience Engine...")
	resilienceEngine.Start()
	defer resilienceEngine.Stop()
	klog.Info("Resilience Engine started successfully")

	// Load graph from storage
	klog.Info("Loading graph from storage...")
	if _, err := storageStore.Load(ctx); err != nil {
		klog.Warningf("Failed to load graph from storage: %v", err)
	}

	// Register event handlers to the pipeline
	pipeline := controller.GetPipeline()
	pipeline.RegisterHandler(&eventStoreHandler{
		store:      eventStore,
		normalizer: normalizer,
	})
	pipeline.RegisterHandler(&eventAnalyzerHandler{
		analyzer: analyzer,
	})
	pipeline.RegisterHandler(&eventCorrelatorHandler{
		correlator: correlator,
	})
	pipeline.RegisterHandler(&aiEventHandler{
		aiEngine: aiEngine,
	})
	pipeline.RegisterHandler(&resilienceEventHandler{
		resilienceEngine: resilienceEngine,
	})

	// Mark components as healthy
	healthChecker.SetComponent("controller", true)
	healthChecker.SetComponent("kubernetes-api", true)
	healthChecker.SetComponent("storage", true)
	healthChecker.SetComponent("event-pipeline", true)
	healthChecker.SetComponent("ai-engine", true)
	healthChecker.SetComponent("resilience-engine", true)
	healthChecker.SetReady(true)

	// Start health server
	go startHealthServer(healthPort, healthChecker, controller, impactAnalyzer, queryAPI, eventStore, aiEngine, resilienceEngine)

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

// ============================================
// EVENT HANDLERS
// ============================================

// eventStoreHandler handles event storage
type eventStoreHandler struct {
	store      *event.Store
	normalizer *event.Normalizer
}

func (h *eventStoreHandler) Name() string {
	return "event-store-handler"
}

func (h *eventStoreHandler) Handle(e *event.Event) error {
	if e == nil {
		return nil
	}
	normalized := h.normalizer.Normalize(e)
	h.store.Add(normalized)
	klog.V(4).Infof("Stored event: %s for asset %s", e.ID, e.AssetID)
	return nil
}

// eventAnalyzerHandler handles event analysis
type eventAnalyzerHandler struct {
	analyzer *event.Analyzer
}

func (h *eventAnalyzerHandler) Name() string {
	return "event-analyzer-handler"
}

func (h *eventAnalyzerHandler) Handle(e *event.Event) error {
	if e == nil {
		return nil
	}
	result := h.analyzer.Analyze(e)
	if result.AnomalyDetected {
		klog.Warningf("Anomaly detected: %s (severity: %s)", result.Message, result.Severity)
	}
	return nil
}

// eventCorrelatorHandler handles event correlation
type eventCorrelatorHandler struct {
	correlator *event.Correlator
}

func (h *eventCorrelatorHandler) Name() string {
	return "event-correlator-handler"
}

func (h *eventCorrelatorHandler) Handle(e *event.Event) error {
	if e == nil {
		return nil
	}
	klog.V(4).Infof("Correlating event: %s", e.ID)
	return nil
}

// aiEventHandler handles AI processing
type aiEventHandler struct {
	aiEngine *ai.Engine
}

func (h *aiEventHandler) Name() string {
	return "ai-event-handler"
}

func (h *aiEventHandler) Handle(e *event.Event) error {
	if e == nil || h.aiEngine == nil {
		return nil
	}
	anomalies := h.aiEngine.Detect(e)
	for _, anomaly := range anomalies {
		if anomaly != nil && anomaly.Detected {
			klog.Warningf("AI detected anomaly: %s (score: %.2f, severity: %s)",
				anomaly.Description, anomaly.Score, anomaly.Severity)
		}
	}
	riskScores := h.aiEngine.Score(e)
	for _, score := range riskScores {
		if score != nil {
			klog.V(4).Infof("Risk score for asset %s: %.2f", score.AssetID, score.Score)
		}
	}
	return nil
}

// resilienceEventHandler handles resilience processing
type resilienceEventHandler struct {
	resilienceEngine *resilience.Engine
}

func (h *resilienceEventHandler) Name() string {
	return "resilience-event-handler"
}

func (h *resilienceEventHandler) Handle(e *event.Event) error {
	if e == nil || h.resilienceEngine == nil {
		return nil
	}
	klog.V(4).Infof("Resilience engine processing event for asset: %s", e.AssetID)
	return nil
}

// ============================================
// HEALTH SERVER
// ============================================

// startHealthServer starts the health endpoint
func startHealthServer(port int, checker *health.Checker, controller *assetgraph.Controller, impactAnalyzer *impact.Analyzer, queryAPI *query.API, eventStore *event.Store, aiEngine *ai.Engine, resilienceEngine *resilience.Engine) {
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

	// ============================================
	// EVENT INTELLIGENCE ENDPOINTS
	// ============================================

	mux.HandleFunc("/api/events/recent", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if r.URL.Query().Get("limit") != "" {
			fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
		}
		events := eventStore.GetRecent(limit)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"count\": %d, \"events\": %v}\n", len(events), events)
	})

	mux.HandleFunc("/api/events/by-asset", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		events := eventStore.GetByAsset(assetID)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"count\": %d, \"events\": %v}\n", assetID, len(events), events)
	})

	mux.HandleFunc("/api/events/count", func(w http.ResponseWriter, r *http.Request) {
		count := eventStore.Count()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"total_events\": %d}\n", count)
	})

	// ============================================
	// AI ENDPOINTS
	// ============================================

	mux.HandleFunc("/api/ai/anomalies", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		events := eventStore.GetByAsset(assetID)
		if len(events) == 0 {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"anomalies\": []}\n", assetID)
			return
		}
		latestEvent := events[len(events)-1]
		anomalies := aiEngine.Detect(latestEvent)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"anomalies\": %v}\n", assetID, anomalies)
	})

	mux.HandleFunc("/api/ai/risk", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		node, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"risk\": null}\n", assetID)
			return
		}
		scores := aiEngine.Score(node)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"risk\": %v}\n", assetID, scores)
	})

	mux.HandleFunc("/api/ai/predict", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		node, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"predictions\": []}\n", assetID)
			return
		}
		predictions := aiEngine.Predict(node)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"predictions\": %v}\n", assetID, predictions)
	})

	mux.HandleFunc("/api/ai/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if aiEngine != nil && aiEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			aiEngine != nil && aiEngine.IsRunning())
	})

	// ============================================
	// RESILIENCE ENDPOINTS
	// ============================================

	// Health check endpoint
	mux.HandleFunc("/api/resilience/health", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		node, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"health\": null}\n", assetID)
			return
		}
		healthStatuses := resilienceEngine.CheckHealth(node)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"health\": %v}\n", assetID, healthStatuses)
	})

	// Failure detection endpoint
	mux.HandleFunc("/api/resilience/failures", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		node, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"failures\": []}\n", assetID)
			return
		}
		failures := resilienceEngine.DetectFailures(node)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"failures\": %v}\n", assetID, failures)
	})

	// Recovery plan endpoint
	mux.HandleFunc("/api/resilience/recovery", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		failureType := r.URL.Query().Get("type")
		if failureType == "" {
			failureType = "unknown"
		}
		node, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"recovery\": null}\n", assetID)
			return
		}
		failure := &resilience.Failure{
			AssetID:     assetID,
			Type:        failureType,
			Severity:    "medium",
			Description: "Simulated failure for testing",
			Timestamp:   time.Now(),
		}
		plans := resilienceEngine.PlanRecovery(node, failure)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"recovery\": %v}\n", assetID, plans)
	})

	// Resilience health endpoint
	mux.HandleFunc("/api/resilience/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if resilienceEngine != nil && resilienceEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			resilienceEngine != nil && resilienceEngine.IsRunning())
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
