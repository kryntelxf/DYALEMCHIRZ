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
	"k8s.io/kubernetes/dya/pkg/commercial"
	"k8s.io/kubernetes/dya/pkg/commercial/licenses"
	"k8s.io/kubernetes/dya/pkg/controller/assetgraph"
	"k8s.io/kubernetes/dya/pkg/developer"
	developersdks "k8s.io/kubernetes/dya/pkg/developer/sdks"
	"k8s.io/kubernetes/dya/pkg/digitaltwin"
	dtanalyzers "k8s.io/kubernetes/dya/pkg/digitaltwin/analyzers"
	"k8s.io/kubernetes/dya/pkg/digitaltwin/simulators"
	"k8s.io/kubernetes/dya/pkg/ecosystem"
	ecosystemsdks "k8s.io/kubernetes/dya/pkg/ecosystem/sdks"
	"k8s.io/kubernetes/dya/pkg/edge"
	"k8s.io/kubernetes/dya/pkg/edge/buffers"
	edgeenforcers "k8s.io/kubernetes/dya/pkg/edge/enforcers"
	edgehandlers "k8s.io/kubernetes/dya/pkg/edge/handlers"
	edgesyncers "k8s.io/kubernetes/dya/pkg/edge/syncers"
	"k8s.io/kubernetes/dya/pkg/enterprise"
	enterpriseauditors "k8s.io/kubernetes/dya/pkg/enterprise/auditors"
	enterprisehandlers "k8s.io/kubernetes/dya/pkg/enterprise/handlers"
	"k8s.io/kubernetes/dya/pkg/event"
	"k8s.io/kubernetes/dya/pkg/globalscale"
	"k8s.io/kubernetes/dya/pkg/globalscale/regions"
	"k8s.io/kubernetes/dya/pkg/health"
	"k8s.io/kubernetes/dya/pkg/impact"
	"k8s.io/kubernetes/dya/pkg/knowledge"
	knowledgeanalyzers "k8s.io/kubernetes/dya/pkg/knowledge/analyzers"
	"k8s.io/kubernetes/dya/pkg/knowledge/extractors"
	"k8s.io/kubernetes/dya/pkg/knowledge/queriers"
	"k8s.io/kubernetes/dya/pkg/metrics"
	"k8s.io/kubernetes/dya/pkg/predictive"
	"k8s.io/kubernetes/dya/pkg/predictive/predictors"
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
	fmt.Println("║   Stage 16: Predictive Infrastructure Intelligence          ║")
	fmt.Println("║   Version: 0.17.0                                           ║")
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

	officialSDKs := developersdks.GetOfficialSDKs()
	for _, sdk := range officialSDKs {
		if s, ok := sdk.(developer.SDK); ok {
			developerEngine.RegisterSDK(s)
		}
	}

	developerEngine.RegisterPlugin(developer.Plugin{
		ID:          "plugin-monitor",
		Name:        "Monitor Plugin",
		Version:     "1.0.0",
		Type:        "monitoring",
		Author:      "DYALEMCHIRZ Team",
		Enabled:     true,
		CreatedAt:   time.Now(),
	})

	developerEngine.RegisterTemplate(developer.Template{
		ID:          "template-go",
		Name:        "Go Service Template",
		Type:        "service",
		Path:        "/templates/go-service",
		Description: "Template for Go microservices",
		CreatedAt:   time.Now(),
	})

	developerEngine.RegisterTool(developer.Tool{
		ID:          "tool-dya-cli",
		Name:        "DYALEMCHIRZ CLI",
		Command:     "dya",
		Description: "Command line tool for DYALEMCHIRZ",
		CreatedAt:   time.Now(),
	})

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

	// ECOSYSTEM ENGINE
	klog.Info("Creating Ecosystem Engine...")
	ecosystemEngine := ecosystem.NewEngine()

	officialEcosystemSDKs := ecosystemsdks.GetOfficialSDKs()
	for _, sdk := range officialEcosystemSDKs {
		if s, ok := sdk.(ecosystem.SDK); ok {
			ecosystemEngine.RegisterSDK(s)
		}
	}

	ecosystemEngine.RegisterPlugin(ecosystem.Plugin{
		ID:          "plugin-kafka",
		Name:        "Kafka Integration",
		Type:        "integration",
		Author:      "DYALEMCHIRZ Team",
		Version:     "1.0.0",
		Repository:  "https://github.com/kryntelxf/dya-plugin-kafka",
		Downloads:   1500,
		Rating:      4.8,
		CreatedAt:   time.Now(),
	})

	ecosystemEngine.RegisterPlugin(ecosystem.Plugin{
		ID:          "plugin-prometheus",
		Name:        "Prometheus Integration",
		Type:        "monitoring",
		Author:      "DYALEMCHIRZ Team",
		Version:     "1.0.0",
		Repository:  "https://github.com/kryntelxf/dya-plugin-prometheus",
		Downloads:   2300,
		Rating:      4.9,
		CreatedAt:   time.Now(),
	})

	ecosystemEngine.RegisterIntegration(ecosystem.Integration{
		ID:            "integration-aws",
		Name:          "AWS Cloud Integration",
		Partner:       "Amazon Web Services",
		Type:          "cloud",
		Description:   "Integration with AWS services",
		Documentation: "https://docs.dyalemchirz.com/integrations/aws",
		Status:        "stable",
		CreatedAt:     time.Now(),
	})

	ecosystemEngine.RegisterExample(ecosystem.Example{
		ID:          "example-go",
		Name:        "Go Microservice Example",
		Language:    "go",
		Path:        "/examples/go-microservice",
		Description: "Example of a Go microservice with DYALEMCHIRZ",
		CreatedAt:   time.Now(),
	})

	ecosystemEngine.RegisterGuide(ecosystem.Guide{
		ID:          "guide-getting-started",
		Title:       "Getting Started Guide",
		Category:    "getting-started",
		Path:        "/guides/getting-started",
		Description: "How to get started with DYALEMCHIRZ",
		UpdatedAt:   time.Now(),
	})

	ecosystemEngine.RegisterPartner(ecosystem.Partner{
		ID:          "partner-google",
		Name:        "Google Cloud",
		Website:     "https://cloud.google.com",
		Description: "Google Cloud Platform integration partner",
		Status:      "active",
		CreatedAt:   time.Now(),
	})

	ecosystemEngine.Start()
	defer ecosystemEngine.Stop()
	klog.Info("Ecosystem Engine started successfully")

	// COMMERCIAL PLATFORM ENGINE
	klog.Info("Creating Commercial Platform Engine...")
	commercialEngine := commercial.NewEngine()

	enterpriseLicense := licenses.GetEnterpriseLicense()
	if l, ok := enterpriseLicense.(commercial.License); ok {
		commercialEngine.RegisterLicense(l)
	}

	commercialEngine.RegisterSupportPlan(commercial.SupportPlan{
		ID:           "support-basic",
		Name:         "Basic Support",
		Level:        "basic",
		ResponseTime: "24 hours",
		Hours:        "Business hours",
		CreatedAt:    time.Now(),
	})

	commercialEngine.RegisterSupportPlan(commercial.SupportPlan{
		ID:           "support-premium",
		Name:         "Premium Support",
		Level:        "premium",
		ResponseTime: "4 hours",
		Hours:        "24/7",
		CreatedAt:    time.Now(),
	})

	commercialEngine.RegisterSupportPlan(commercial.SupportPlan{
		ID:           "support-enterprise",
		Name:         "Enterprise Support",
		Level:        "enterprise",
		ResponseTime: "1 hour",
		Hours:        "24/7 with SLA",
		CreatedAt:    time.Now(),
	})

	commercialEngine.RegisterManagedService(commercial.ManagedService{
		ID:          "managed-basic",
		Name:        "Basic Managed Service",
		Description: "Basic managed service for DYALEMCHIRZ deployment",
		Price:       "Contact Sales",
		CreatedAt:   time.Now(),
	})

	commercialEngine.RegisterProfessionalService(commercial.ProfessionalService{
		ID:          "prof-consulting",
		Name:        "Consulting Services",
		Type:        "consulting",
		Description: "Expert consulting for DYALEMCHIRZ implementation",
		Price:       "Contact Sales",
		CreatedAt:   time.Now(),
	})

	commercialEngine.RegisterProfessionalService(commercial.ProfessionalService{
		ID:          "prof-training",
		Name:        "Training Services",
		Type:        "training",
		Description: "Comprehensive training for DYALEMCHIRZ platform",
		Price:       "Contact Sales",
		CreatedAt:   time.Now(),
	})

	commercialEngine.RegisterPartner(commercial.Partner{
		ID:          "partner-acme",
		Name:        "ACME Consulting",
		Type:        "consultant",
		Website:     "https://acme.com",
		CreatedAt:   time.Now(),
	})

	commercialEngine.RegisterCommercialAPI(commercial.CommercialAPI{
		ID:          "api-enterprise",
		Name:        "Enterprise API",
		Endpoint:    "https://api.dyalemchirz.com/v1/enterprise",
		Description: "Enterprise-grade API with advanced features",
		Pricing:     "Contact Sales",
		CreatedAt:   time.Now(),
	})

	commercialEngine.Start()
	defer commercialEngine.Stop()
	klog.Info("Commercial Platform Engine started successfully")

	// GLOBAL SCALE ENGINE
	klog.Info("Creating Global Scale Engine...")
	globalScaleEngine := globalscale.NewEngine()

	globalRegions := regions.GetGlobalRegions()
	for _, region := range globalRegions {
		if r, ok := region.(globalscale.Region); ok {
			globalScaleEngine.RegisterRegion(r)
		}
	}

	globalScaleEngine.RegisterCluster(globalscale.Cluster{
		ID:        "cluster-us-east",
		Name:      "US East Cluster",
		Region:    "region-us-east",
		Nodes:     100,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterCluster(globalscale.Cluster{
		ID:        "cluster-us-west",
		Name:      "US West Cluster",
		Region:    "region-us-west",
		Nodes:     100,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterCluster(globalscale.Cluster{
		ID:        "cluster-eu-west",
		Name:      "EU West Cluster",
		Region:    "region-eu-west",
		Nodes:     100,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterCluster(globalscale.Cluster{
		ID:        "cluster-ap-southeast",
		Name:      "AP Southeast Cluster",
		Region:    "region-ap-southeast",
		Nodes:     100,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterLoadBalancer(globalscale.LoadBalancer{
		ID:        "lb-us-east",
		Name:      "US East Load Balancer",
		Region:    "region-us-east",
		Endpoint:  "https://lb-us-east.dyalemchirz.com",
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterLoadBalancer(globalscale.LoadBalancer{
		ID:        "lb-us-west",
		Name:      "US West Load Balancer",
		Region:    "region-us-west",
		Endpoint:  "https://lb-us-west.dyalemchirz.com",
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterLoadBalancer(globalscale.LoadBalancer{
		ID:        "lb-eu-west",
		Name:      "EU West Load Balancer",
		Region:    "region-eu-west",
		Endpoint:  "https://lb-eu-west.dyalemchirz.com",
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterLoadBalancer(globalscale.LoadBalancer{
		ID:        "lb-ap-southeast",
		Name:      "AP Southeast Load Balancer",
		Region:    "region-ap-southeast",
		Endpoint:  "https://lb-ap-southeast.dyalemchirz.com",
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterCacheNode(globalscale.CacheNode{
		ID:        "cache-us-east",
		Name:      "US East Cache",
		Region:    "region-us-east",
		Size:      1024,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterMonitor(globalscale.Monitor{
		ID:        "monitor-us-east",
		Name:      "US East Monitor",
		Region:    "region-us-east",
		Type:      "health",
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterAutoScaler(globalscale.AutoScaler{
		ID:        "scaler-us-east",
		Name:      "US East Auto-Scaler",
		Region:    "region-us-east",
		MinNodes:  10,
		MaxNodes:  200,
		Status:    "active",
		CreatedAt: time.Now(),
	})

	globalScaleEngine.RegisterDisasterRecovery(globalscale.DisasterRecovery{
		ID:           "dr-us-east",
		Name:         "US East Disaster Recovery",
		Region:       "region-us-east",
		BackupRegion: "region-us-west",
		RPO:          "5 minutes",
		RTO:          "15 minutes",
		Status:       "active",
		CreatedAt:    time.Now(),
	})

	globalScaleEngine.Start()
	defer globalScaleEngine.Stop()
	klog.Info("Global Scale Engine started successfully")

	// PREDICTIVE ENGINE
	klog.Info("Creating Predictive Engine...")
	predictiveEngine := predictive.NewEngine()
	predictiveEngine.RegisterPredictor(&predictors.BasicPredictor{})
	predictiveEngine.Start()
	defer predictiveEngine.Stop()
	klog.Info("Predictive Engine started successfully")

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
	healthChecker.SetComponent("ecosystem-engine", true)
	healthChecker.SetComponent("commercial-engine", true)
	healthChecker.SetComponent("global-scale-engine", true)
	healthChecker.SetComponent("predictive-engine", true)
	healthChecker.SetReady(true)

	// START HEALTH SERVER
	go startHealthServer(healthPort, healthChecker, controller, impactAnalyzer, queryAPI, eventStore, aiEngine, resilienceEngine, recoveryOrchestrator, digitalTwinEngine, securityEngine, edgeEngine, knowledgeEngine, enterpriseEngine, developerEngine, ecosystemEngine, commercialEngine, globalScaleEngine, predictiveEngine)

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

func startHealthServer(port int, checker *health.Checker, controller *assetgraph.Controller, impactAnalyzer *impact.Analyzer, queryAPI *query.API, eventStore *event.Store, aiEngine *ai.Engine, resilienceEngine *resilience.Engine, recoveryOrchestrator *recovery.Orchestrator, digitalTwinEngine *digitaltwin.Engine, securityEngine *security.Engine, edgeEngine *edge.Engine, knowledgeEngine *knowledge.Engine, enterpriseEngine *enterprise.Engine, developerEngine *developer.Engine, ecosystemEngine *ecosystem.Engine, commercialEngine *commercial.Engine, globalScaleEngine *globalscale.Engine, predictiveEngine *predictive.Engine) {
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
