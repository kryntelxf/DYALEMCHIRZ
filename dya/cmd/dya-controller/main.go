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
	aidetectors "k8s.io/kubernetes/dya/pkg/ai/detectors"
	aipredictors "k8s.io/kubernetes/dya/pkg/ai/predictors"
	"k8s.io/kubernetes/dya/pkg/ai/scorers"
	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/developer"
	"k8s.io/kubernetes/dya/pkg/developer/sdks"
	"k8s.io/kubernetes/dya/pkg/digitaltwin"
	dtanalyzers "k8s.io/kubernetes/dya/pkg/digitaltwin/analyzers"
	"k8s.io/kubernetes/dya/pkg/digitaltwin/simulators"
	"k8s.io/kubernetes/dya/pkg/edge"
	"k8s.io/kubernetes/dya/pkg/edge/buffers"
	edgeenforcers "k8s.io/kubernetes/dya/pkg/edge/enforcers"
	edgehandlers "k8s.io/kubernetes/dya/pkg/edge/handlers"
	edgesyncers "k8s.io/kubernetes/dya/pkg/edge/syncers"
	"k8s.io/kubernetes/dya/pkg/enterprise"
	enterpriseauditors "k8s.io/kubernetes/dya/pkg/enterprise/auditors"
	enterprisehandlers "k8s.io/kubernetes/dya/pkg/enterprise/handlers"
	"k8s.io/kubernetes/dya/pkg/event"
	"k8s.io/kubernetes/dya/pkg/health"
	"k8s.io/kubernetes/dya/pkg/impact"
	"k8s.io/kubernetes/dya/pkg/knowledge"
	knowledgeanalyzers "k8s.io/kubernetes/dya/pkg/knowledge/analyzers"
	"k8s.io/kubernetes/dya/pkg/knowledge/extractors"
	"k8s.io/kubernetes/dya/pkg/knowledge/queriers"
	"k8s.io/kubernetes/dya/pkg/metrics"
	"k8s.io/kubernetes/dya/pkg/query"
	"k8s.io/kubernetes/dya/pkg/recovery"
	"k8s.io/kubernetes/dya/pkg/recovery/executors"
	"k8s.io/kubernetes/dya/pkg/recovery/notifiers"
	recoveryverifiers "k8s.io/kubernetes/dya/pkg/recovery/verifiers"
	"k8s.io/kubernetes/dya/pkg/resilience"
	"k8s.io/kubernetes/dya/pkg/resilience/checkers"
	resdetectors "k8s.io/kubernetes/dya/pkg/resilience/detectors"
	"k8s.io/kubernetes/dya/pkg/resilience/planners"
	"k8s.io/kubernetes/dya/pkg/security"
	securityauditors "k8s.io/kubernetes/dya/pkg/security/auditors"
	"k8s.io/kubernetes/dya/pkg/security/detectors"
	"k8s.io/kubernetes/dya/pkg/security/enforcers"
	securityverifiers "k8s.io/kubernetes/dya/pkg/security/verifiers"
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
	fmt.Println("║   Stage 12: Developer Platform                               ║")
	fmt.Println("║   Version: 0.13.0                                           ║")
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	klog.Info("DYALEMCHIRZ controller starting...")

	cfg, err := getConfig()
	if err != nil {
		klog.Fatalf("Failed to get Kubernetes config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signalCh
		klog.Info("Received shutdown signal, stopping...")
		cancel()
	}()

	healthChecker := health.NewChecker()

	klog.Info("Creating Asset Graph controller...")
	controller, err := assetgraph.NewController(cfg)
	if err != nil {
		klog.Fatalf("Failed to create controller: %v", err)
	}

	storageStore := storage.NewGraphStore(controller.KubeClient())
	impactAnalyzer := impact.NewAnalyzer(controller.GetGraph())
	queryAPI := query.NewAPI(controller.GetGraph())

	// EVENT INTELLIGENCE
	eventStore := event.NewStore(10000)
	normalizer := event.NewNormalizer()
	correlator := event.NewCorrelator(5 * time.Minute)
	analyzer := event.NewAnalyzer(10)

	// AI ENGINE
	klog.Info("Creating AI Engine...")
	aiEngine := ai.NewEngine()
	aiEngine.RegisterDetector(aidetectors.NewStatisticalDetector(2.0, 10))
	aiEngine.RegisterScorer(scorers.NewRiskScorer())
	aiEngine.RegisterPredictor(aipredictors.NewSimplePredictor())
	aiEngine.Start()
	defer aiEngine.Stop()
	klog.Info("AI Engine started successfully")

	// RESILIENCE ENGINE
	klog.Info("Creating Resilience Engine...")
	resilienceEngine := resilience.NewEngine()
	resilienceEngine.RegisterHealthChecker(&checkers.BasicChecker{})
	resilienceEngine.RegisterFailureDetector(&resdetectors.BasicDetector{})
	resilienceEngine.RegisterRecoveryPlanner(&planners.BasicPlanner{})
	resilienceEngine.Start()
	defer resilienceEngine.Stop()
	klog.Info("Resilience Engine started successfully")

	// RECOVERY ORCHESTRATOR
	klog.Info("Creating Recovery Orchestrator...")
	recoveryOrchestrator := recovery.NewOrchestrator()
	recoveryOrchestrator.RegisterExecutor(&executors.BasicExecutor{})
	recoveryOrchestrator.RegisterVerifier(&recoveryverifiers.BasicVerifier{})
	recoveryOrchestrator.RegisterNotifier(&notifiers.BasicNotifier{})
	recoveryOrchestrator.Start()
	defer recoveryOrchestrator.Stop()
	klog.Info("Recovery Orchestrator started successfully")

	// DIGITAL TWIN
	klog.Info("Creating Digital Twin Engine...")
	digitalTwinEngine := digitaltwin.NewEngine()
	digitalTwinEngine.RegisterSimulator(&simulators.BasicSimulator{})
	digitalTwinEngine.RegisterAnalyzer(&dtanalyzers.BasicAnalyzer{})
	digitalTwinEngine.Start()
	defer digitalTwinEngine.Stop()
	klog.Info("Digital Twin Engine started successfully")

	// SECURITY ENGINE
	klog.Info("Creating Security Engine...")
	securityEngine := security.NewEngine()
	securityEngine.RegisterVerifier(&securityverifiers.BasicVerifier{})
	securityEngine.RegisterEnforcer(&enforcers.BasicEnforcer{})
	securityEngine.RegisterAuditor(&securityauditors.BasicAuditor{})
	securityEngine.RegisterDetector(&detectors.BasicDetector{})
	securityEngine.Start()
	defer securityEngine.Stop()
	klog.Info("Security Engine started successfully")

	// EDGE FABRIC
	klog.Info("Creating Edge Engine...")
	edgeEngine := edge.NewEngine()
	edgeEngine.RegisterHandler(&edgehandlers.BasicHandler{})
	edgeEngine.RegisterSyncer(&edgesyncers.BasicSyncer{})
	edgeEngine.RegisterEnforcer(&edgeenforcers.BasicEnforcer{})
	edgeEngine.RegisterBuffer(buffers.NewBasicBuffer())
	edgeEngine.Start()
	defer edgeEngine.Stop()
	klog.Info("Edge Engine started successfully")

	// KNOWLEDGE ENGINE
	klog.Info("Creating Knowledge Engine...")
	knowledgeEngine := knowledge.NewEngine()
	knowledgeEngine.RegisterExtractor(&extractors.BasicExtractor{})
	knowledgeEngine.RegisterAnalyzer(&knowledgeanalyzers.BasicAnalyzer{})
	knowledgeEngine.RegisterQuerier(&queriers.BasicQuerier{})
	knowledgeEngine.Start()
	defer knowledgeEngine.Stop()
	klog.Info("Knowledge Engine started successfully")

	// ENTERPRISE ENGINE
	klog.Info("Creating Enterprise Engine...")
	enterpriseEngine := enterprise.NewEngine()
	enterpriseEngine.RegisterAuditor(&enterpriseauditors.BasicAuditor{})
	enterpriseEngine.RegisterAPIHandler(&enterprisehandlers.BasicHandler{})

	enterpriseEngine.RegisterTenant(enterprise.Tenant{
		ID:          "tenant-1",
		Name:        "Default Tenant",
		Description: "Default enterprise tenant",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	})

	enterpriseEngine.RegisterRole(enterprise.Role{
		ID:          "role-1",
		Name:        "Admin",
		Permissions: []string{"read", "write", "delete", "admin"},
		CreatedAt:   time.Now(),
	})

	enterpriseEngine.RegisterOrganization(enterprise.Organization{
		ID:          "org-1",
		Name:        "Default Organization",
		TenantID:    "tenant-1",
		Members:     []string{"admin"},
		CreatedAt:   time.Now(),
	})

	enterpriseEngine.Start()
	defer enterpriseEngine.Stop()
	klog.Info("Enterprise Engine started successfully")

	// DEVELOPER ENGINE
	klog.Info("Creating Developer Engine...")
	developerEngine := developer.NewEngine()

	// Register official SDKs
	officialSDKs := sdks.GetOfficialSDKs()
	for _, sdk := range officialSDKs {
		if s, ok := sdk.(developer.SDK); ok {
			developerEngine.RegisterSDK(s)
		}
	}

	// Register sample plugin
	developerEngine.RegisterPlugin(developer.Plugin{
		ID:          "plugin-monitor",
		Name:        "Monitor Plugin",
		Version:     "1.0.0",
		Type:        "monitoring",
		Author:      "DYALEMCHIRZ Team",
		Enabled:     true,
		CreatedAt:   time.Now(),
	})

	// Register sample template
	developerEngine.RegisterTemplate(developer.Template{
		ID:          "template-go",
		Name:        "Go Service Template",
		Type:        "service",
		Path:        "/templates/go-service",
		Description: "Template for Go microservices",
		CreatedAt:   time.Now(),
	})

	// Register sample tool
	developerEngine.RegisterTool(developer.Tool{
		ID:          "tool-dya-cli",
		Name:        "DYALEMCHIRZ CLI",
		Command:     "dya",
		Description: "Command line tool for DYALEMCHIRZ",
		CreatedAt:   time.Now(),
	})

	// Register sample doc
	developerEngine.RegisterDoc(developer.Doc{
		ID:          "doc-api",
		Title:       "API Reference",
		Path:        "/docs/api",
		Description: "Complete API reference documentation",
		UpdatedAt:   time.Now(),
	})

	developerEngine.Start()
	defer developerEngine.Stop()
	klog.Info("Developer Engine started successfully")

	// LOAD GRAPH
	klog.Info("Loading graph from storage...")
	if _, err := storageStore.Load(ctx); err != nil {
		klog.Warningf("Failed to load graph from storage: %v", err)
	}

	// REGISTER EVENT HANDLERS
	pipeline := controller.GetPipeline()
	pipeline.RegisterHandler(&eventStoreHandler{store: eventStore, normalizer: normalizer})
	pipeline.RegisterHandler(&eventAnalyzerHandler{analyzer: analyzer})
	pipeline.RegisterHandler(&eventCorrelatorHandler{correlator: correlator})
	pipeline.RegisterHandler(&aiEventHandler{aiEngine: aiEngine})
	pipeline.RegisterHandler(&resilienceEventHandler{resilienceEngine: resilienceEngine})
	pipeline.RegisterHandler(&recoveryEventHandler{recoveryOrchestrator: recoveryOrchestrator})
	pipeline.RegisterHandler(&securityEventHandler{securityEngine: securityEngine})
	pipeline.RegisterHandler(&knowledgeEventHandler{knowledgeEngine: knowledgeEngine})

	// HEALTH CHECKS
	healthChecker.SetComponent("controller", true)
	healthChecker.SetComponent("kubernetes-api", true)
	healthChecker.SetComponent("storage", true)
	healthChecker.SetComponent("event-pipeline", true)
	healthChecker.SetComponent("ai-engine", true)
	healthChecker.SetComponent("resilience-engine", true)
	healthChecker.SetComponent("recovery-orchestrator", true)
	healthChecker.SetComponent("digital-twin", true)
	healthChecker.SetComponent("security-engine", true)
	healthChecker.SetComponent("edge-engine", true)
	healthChecker.SetComponent("knowledge-engine", true)
	healthChecker.SetComponent("enterprise-engine", true)
	healthChecker.SetComponent("developer-engine", true)
	healthChecker.SetReady(true)

	// START HEALTH SERVER
	go startHealthServer(healthPort, healthChecker, controller, impactAnalyzer, queryAPI, eventStore, aiEngine, resilienceEngine, recoveryOrchestrator, digitalTwinEngine, securityEngine, edgeEngine, knowledgeEngine, enterpriseEngine, developerEngine)

	// RUN CONTROLLER
	klog.Infof("Starting Asset Graph controller with %d workers...", workers)
	if err := controller.Run(ctx, workers); err != nil {
		klog.Fatalf("Controller failed: %v", err)
	}

	// SAVE GRAPH
	klog.Info("Saving graph to storage...")
	if err := storageStore.Save(ctx, controller.GetGraph()); err != nil {
		klog.Errorf("Failed to save graph: %v", err)
	}

	klog.Info("Controller shutdown complete")
}

// ============================================
// EVENT HANDLERS
// ============================================

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
	return nil
}

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

type recoveryEventHandler struct {
	recoveryOrchestrator *recovery.Orchestrator
}

func (h *recoveryEventHandler) Name() string {
	return "recovery-event-handler"
}

func (h *recoveryEventHandler) Handle(e *event.Event) error {
	if e == nil || h.recoveryOrchestrator == nil {
		return nil
	}
	klog.V(4).Infof("Recovery orchestrator processing event for asset: %s", e.AssetID)
	return nil
}

type securityEventHandler struct {
	securityEngine *security.Engine
}

func (h *securityEventHandler) Name() string {
	return "security-event-handler"
}

func (h *securityEventHandler) Handle(e *event.Event) error {
	if e == nil || h.securityEngine == nil {
		return nil
	}
	h.securityEngine.Audit(e)
	anomalies := h.securityEngine.Detect(e)
	for _, anomaly := range anomalies {
		if anomaly != nil {
			klog.Warningf("Security anomaly detected: %s (severity: %s)", anomaly.Description, anomaly.Severity)
		}
	}
	return nil
}

type knowledgeEventHandler struct {
	knowledgeEngine *knowledge.Engine
}

func (h *knowledgeEventHandler) Name() string {
	return "knowledge-event-handler"
}

func (h *knowledgeEventHandler) Handle(e *event.Event) error {
	if e == nil || h.knowledgeEngine == nil {
		return nil
	}
	knowledge := h.knowledgeEngine.Extract(e)
	for _, k := range knowledge {
		if k != nil {
			klog.V(4).Infof("Extracted knowledge: %s", k.ID)
		}
	}
	return nil
}

// ============================================
// HEALTH SERVER
// ============================================

func startHealthServer(port int, checker *health.Checker, controller *assetgraph.Controller, impactAnalyzer *impact.Analyzer, queryAPI *query.API, eventStore *event.Store, aiEngine *ai.Engine, resilienceEngine *resilience.Engine, recoveryOrchestrator *recovery.Orchestrator, digitalTwinEngine *digitaltwin.Engine, securityEngine *security.Engine, edgeEngine *edge.Engine, knowledgeEngine *knowledge.Engine, enterpriseEngine *enterprise.Engine, developerEngine *developer.Engine) {
	mux := http.NewServeMux()

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

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsData := metrics.GetMetrics()
		w.Header().Set("Content-Type", "text/plain")
		for name, value := range metricsData {
			fmt.Fprintf(w, "# HELP %s DYALEMCHIRZ metric\n", name)
			fmt.Fprintf(w, "# TYPE %s gauge\n", name)
			fmt.Fprintf(w, "%s %f\n", name, value)
		}
	})

	// Graph endpoints
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

	// Event endpoints
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

	// AI endpoints
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

	// Resilience endpoints
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

	// Recovery Orchestrator endpoints
	mux.HandleFunc("/api/recovery/execute", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset")
		if assetID == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing asset parameter"))
			return
		}
		_, ok := queryAPI.GetNode(assetID)
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "{\"asset\": \"%s\", \"status\": \"asset not found\"}\n", assetID)
			return
		}

		plan := &simplePlan{
			assetID: assetID,
			name:    "recovery-" + assetID,
			steps: []recovery.Step{
				{ID: "step-1", Name: "Investigate failure", Action: "investigate", Description: "Investigate the cause of failure", Timeout: 30 * time.Second},
				{ID: "step-2", Name: "Restart service", Action: "restart", Description: "Restart the failed service", Timeout: 60 * time.Second},
				{ID: "step-3", Name: "Verify recovery", Action: "verify", Description: "Verify that the service is recovered", Timeout: 30 * time.Second},
			},
		}

		status := recoveryOrchestrator.ExecutePlan(plan)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"asset\": \"%s\", \"status\": %v}\n", assetID, status)
	})

	mux.HandleFunc("/api/recovery/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if recoveryOrchestrator != nil && recoveryOrchestrator.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			recoveryOrchestrator != nil && recoveryOrchestrator.IsRunning())
	})

	// Digital Twin endpoints
	mux.HandleFunc("/api/digitaltwin/simulate", func(w http.ResponseWriter, r *http.Request) {
		scenarioName := r.URL.Query().Get("name")
		if scenarioName == "" {
			scenarioName = "default-scenario"
		}

		scenario := &digitaltwin.Scenario{
			ID:          "scenario-" + time.Now().Format("20060102150405"),
			Name:        scenarioName,
			Description: "Simulation scenario",
			Changes: map[string]interface{}{
				"type": "test",
			},
			Parameters: map[string]string{
				"source": "api",
			},
		}

		results := digitalTwinEngine.Simulate(scenario)
		analyses := digitalTwinEngine.Analyze(results)

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"scenario\": \"%s\", \"results\": %v, \"analyses\": %v}\n", scenario.ID, results, analyses)
	})

	mux.HandleFunc("/api/digitaltwin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if digitalTwinEngine != nil && digitalTwinEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			digitalTwinEngine != nil && digitalTwinEngine.IsRunning())
	})

	// Security endpoints
	mux.HandleFunc("/api/security/verify", func(w http.ResponseWriter, r *http.Request) {
		identity := r.URL.Query().Get("identity")
		if identity == "" {
			identity = "unknown"
		}
		results := securityEngine.Verify(identity)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"identity\": \"%s\", \"results\": %v}\n", identity, results)
	})

	mux.HandleFunc("/api/security/enforce", func(w http.ResponseWriter, r *http.Request) {
		policy := r.URL.Query().Get("policy")
		if policy == "" {
			policy = "default"
		}
		context := r.URL.Query().Get("context")
		if context == "" {
			context = "default"
		}
		results := securityEngine.Enforce(policy, context)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"policy\": \"%s\", \"results\": %v}\n", policy, results)
	})

	mux.HandleFunc("/api/security/anomalies", func(w http.ResponseWriter, r *http.Request) {
		activity := r.URL.Query().Get("activity")
		if activity == "" {
			activity = "unknown"
		}
		anomalies := securityEngine.Detect(activity)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"activity\": \"%s\", \"anomalies\": %v}\n", activity, anomalies)
	})

	mux.HandleFunc("/api/security/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if securityEngine != nil && securityEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			securityEngine != nil && securityEngine.IsRunning())
	})

	// Edge endpoints
	mux.HandleFunc("/api/edge/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if edgeEngine != nil && edgeEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			edgeEngine != nil && edgeEngine.IsRunning())
	})

	mux.HandleFunc("/api/edge/sync", func(w http.ResponseWriter, r *http.Request) {
		if edgeEngine != nil {
			edgeEngine.Sync()
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("sync triggered"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("edge engine not available"))
		}
	})

	mux.HandleFunc("/api/edge/buffer", func(w http.ResponseWriter, r *http.Request) {
		if edgeEngine == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("edge engine not available"))
			return
		}
		results := edgeEngine.FlushBuffers()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"buffers\": %v}\n", results)
	})

	// Knowledge endpoints
	mux.HandleFunc("/api/knowledge/extract", func(w http.ResponseWriter, r *http.Request) {
		if knowledgeEngine == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("knowledge engine not available"))
			return
		}
		data := r.URL.Query().Get("data")
		if data == "" {
			data = "default"
		}
		knowledge := knowledgeEngine.Extract(data)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"knowledge\": %v}\n", knowledge)
	})

	mux.HandleFunc("/api/knowledge/query", func(w http.ResponseWriter, r *http.Request) {
		if knowledgeEngine == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("knowledge engine not available"))
			return
		}
		query := r.URL.Query().Get("q")
		if query == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing query parameter"))
			return
		}
		results := knowledgeEngine.Query(query)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"query\": \"%s\", \"results\": %v}\n", query, results)
	})

	mux.HandleFunc("/api/knowledge/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if knowledgeEngine != nil && knowledgeEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			knowledgeEngine != nil && knowledgeEngine.IsRunning())
	})

	// Enterprise endpoints
	mux.HandleFunc("/api/enterprise/tenants", func(w http.ResponseWriter, r *http.Request) {
		tenants := enterpriseEngine.GetTenants()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"tenants\": %v}\n", tenants)
	})

	mux.HandleFunc("/api/enterprise/roles", func(w http.ResponseWriter, r *http.Request) {
		roles := enterpriseEngine.GetRoles()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"roles\": %v}\n", roles)
	})

	mux.HandleFunc("/api/enterprise/organizations", func(w http.ResponseWriter, r *http.Request) {
		orgs := enterpriseEngine.GetOrganizations()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"organizations\": %v}\n", orgs)
	})

	mux.HandleFunc("/api/enterprise/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if enterpriseEngine != nil && enterpriseEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			enterpriseEngine != nil && enterpriseEngine.IsRunning())
	})

	// Developer endpoints
	mux.HandleFunc("/api/developer/sdks", func(w http.ResponseWriter, r *http.Request) {
		sdks := developerEngine.GetSDKs()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"sdks\": %v}\n", sdks)
	})

	mux.HandleFunc("/api/developer/plugins", func(w http.ResponseWriter, r *http.Request) {
		plugins := developerEngine.GetPlugins()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"plugins\": %v}\n", plugins)
	})

	mux.HandleFunc("/api/developer/templates", func(w http.ResponseWriter, r *http.Request) {
		templates := developerEngine.GetTemplates()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"templates\": %v}\n", templates)
	})

	mux.HandleFunc("/api/developer/tools", func(w http.ResponseWriter, r *http.Request) {
		tools := developerEngine.GetTools()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"tools\": %v}\n", tools)
	})

	mux.HandleFunc("/api/developer/docs", func(w http.ResponseWriter, r *http.Request) {
		docs := developerEngine.GetDocs()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"docs\": %v}\n", docs)
	})

	mux.HandleFunc("/api/developer/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"%s\", \"running\": %v}\n",
			func() string {
				if developerEngine != nil && developerEngine.IsRunning() {
					return "healthy"
				}
				return "unhealthy"
			}(),
			developerEngine != nil && developerEngine.IsRunning())
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

// simplePlan implements recovery.Plan
type simplePlan struct {
	assetID string
	name    string
	steps   []recovery.Step
}

func (p *simplePlan) GetSteps() []recovery.Step {
	return p.steps
}

func (p *simplePlan) GetAssetID() string {
	return p.assetID
}

func (p *simplePlan) GetPriority() int {
	return 1
}

func (p *simplePlan) RequiresApproval() bool {
	return false
}

func (p *simplePlan) Name() string {
	return p.name
}

func getConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	}
	if _, err := rest.InClusterConfig(); err == nil {
		return rest.InClusterConfig()
	}
	return nil, fmt.Errorf("could not get Kubernetes config. Use -kubeconfig or run in-cluster")
}
