#!/bin/bash

# ============================================
# DYALEMCHIRZ FIX SCRIPT
# ============================================
# Ini akan memperbaiki semua error CI/CD
# Jalankan: chmod +x fix.sh && ./fix.sh
# ============================================

set -e

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                                                              ║"
echo "║   🔧  DYALEMCHIRZ FIX SCRIPT  🔧                             ║"
echo "║   Memperbaiki semua error CI/CD                             ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"

# ============================================
# 1. CEK STRUCTURE FOLDER
# ============================================
echo ""
echo "📁 Step 1: Cek struktur folder..."

if [ ! -d "dya" ]; then
    echo "❌ Folder 'dya' tidak ditemukan!"
    echo "   Membuat struktur folder..."
    mkdir -p dya/cmd/dya-controller
    mkdir -p dya/pkg/apiserver/middleware
    mkdir -p dya/pkg/enterprise/auditors
    mkdir -p dya/pkg/enterprise/handlers
    mkdir -p dya/pkg/controller/assetgraph
    mkdir -p dya/pkg/graph
    mkdir -p dya/pkg/event
    mkdir -p dya/pkg/ai
    mkdir -p dya/pkg/resilience
    mkdir -p dya/pkg/recovery
    mkdir -p dya/pkg/digitaltwin
    mkdir -p dya/pkg/security
    mkdir -p dya/pkg/edge
    mkdir -p dya/pkg/knowledge
    mkdir -p dya/pkg/globalscale
    mkdir -p dya/pkg/predictive
    mkdir -p dya/pkg/commercial
    mkdir -p dya/pkg/developer
    mkdir -p dya/pkg/ecosystem
    mkdir -p dya/pkg/storage
    mkdir -p dya/pkg/health
    mkdir -p dya/pkg/impact
    mkdir -p dya/pkg/query
    mkdir -p dya/pkg/metrics
    mkdir -p dya/bin
    echo "✅ Struktur folder dibuat"
else
    echo "✅ Folder 'dya' sudah ada"
fi

# ============================================
# 2. BUAT go.mod DI ROOT (jika belum ada)
# ============================================
echo ""
echo "📄 Step 2: Cek go.mod di root..."

if [ ! -f "go.mod" ]; then
    echo "❌ go.mod tidak ditemukan di root!"
    echo "   Membuat go.mod..."
    cat > go.mod << 'EOF'
module k8s.io/kubernetes

go 1.26.0

require (
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/klog/v2 v2.140.0
)

replace (
	k8s.io/api => ./staging/src/k8s.io/api
	k8s.io/apimachinery => ./staging/src/k8s.io/apimachinery
	k8s.io/client-go => ./staging/src/k8s.io/client-go
)
EOF
    echo "✅ go.mod dibuat"
else
    echo "✅ go.mod sudah ada"
fi

# ============================================
# 3. BUAT dya/go.mod (jika belum ada)
# ============================================
echo ""
echo "📄 Step 3: Cek dya/go.mod..."

if [ ! -f "dya/go.mod" ]; then
    echo "❌ dya/go.mod tidak ditemukan!"
    echo "   Membuat dya/go.mod..."
    cat > dya/go.mod << 'EOF'
module k8s.io/kubernetes/dya

go 1.26.0

require (
	k8s.io/api v0.0.0
	k8s.io/apimachinery v0.0.0
	k8s.io/client-go v0.0.0
	k8s.io/klog/v2 v2.140.0
)
EOF
    echo "✅ dya/go.mod dibuat"
else
    echo "✅ dya/go.mod sudah ada"
fi

# ============================================
# 4. BUAT FILE MAIN.GO
# ============================================
echo ""
echo "📄 Step 4: Membuat dya/cmd/dya-controller/main.go..."

cat > dya/cmd/dya-controller/main.go << 'EOF'
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
	"encoding/json"
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

	"k8s.io/kubernetes/dya/pkg/apiserver/middleware"
	"k8s.io/kubernetes/dya/pkg/enterprise"
	"k8s.io/kubernetes/dya/pkg/enterprise/auditors"
	"k8s.io/kubernetes/dya/pkg/enterprise/handlers"
	"k8s.io/kubernetes/dya/pkg/health"
)

var (
	kubeconfig string
	healthPort int
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig.")
	flag.IntVar(&healthPort, "health-port", 8080, "Port for health endpoints.")
}

func main() {
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                                                              ║")
	fmt.Println("║   🚀  DYALEMCHIRZ CONTROLLER  🚀                             ║")
	fmt.Println("║   AI-Native Resilience Operating Platform                    ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║   Version: 0.17.0                                           ║")
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	klog.Info("DYALEMCHIRZ controller starting...")

	cfg, err := getConfig()
	if err != nil {
		klog.Fatalf("Failed to get config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalCh
		klog.Info("Shutting down...")
		cancel()
	}()

	healthChecker := health.NewChecker()

	// ============================================
	// ENTERPRISE ENGINE
	// ============================================
	klog.Info("Creating Enterprise Engine...")
	enterpriseEngine := enterprise.NewEngine()
	enterpriseEngine.RegisterAuditor(&auditors.BasicAuditor{})
	enterpriseEngine.RegisterAPIHandler(&handlers.BasicHandler{})

	tenantManager := enterprise.NewTenantManager()

	// Create default tenants
	defaultQuota := &enterprise.Quota{
		MaxNodes:       100,
		MaxAssets:      1000,
		MaxEvents:      10000,
		MaxAIProcesses: 10,
	}

	tenant1, _ := tenantManager.CreateTenant("tenant-1", "Production", "Main tenant", defaultQuota)
	klog.Infof("Created tenant: %s", tenant1.Name)

	enterpriseEngine.Start()
	defer enterpriseEngine.Stop()
	klog.Info("Enterprise Engine started")

	// Auth Middleware
	authMiddleware := middleware.NewAuthMiddleware()
	adminKey := authMiddleware.GenerateAPIKey("tenant-1", "admin", "admin")
	klog.Infof("Admin API Key: %s", adminKey)

	// HEALTH CHECKS
	healthChecker.SetComponent("enterprise-engine", true)
	healthChecker.SetReady(true)

	// START SERVER
	go startHealthServer(healthPort, healthChecker, authMiddleware, tenantManager)

	// WAIT
	<-ctx.Done()
	klog.Info("Controller shutdown complete")
}

// ============================================
// HEALTH SERVER
// ============================================

func startHealthServer(port int, checker *health.Checker, auth *middleware.AuthMiddleware, tm *enterprise.TenantManager) {
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

	// Protected API
	mux.HandleFunc("/api/tenants", auth.Authenticate(
		func(w http.ResponseWriter, r *http.Request) {
			tenants := tm.GetAllTenants()
			middleware.WriteJSON(w, tenants, http.StatusOK)
		},
	))

	mux.HandleFunc("/api/tenant/usage", auth.Authenticate(
		func(w http.ResponseWriter, r *http.Request) {
			tenantID, _ := middleware.GetTenantFromContext(r.Context())
			usage, err := tm.GetTenantUsage(tenantID)
			if err != nil {
				middleware.WriteJSON(w, map[string]string{"error": err.Error()}, http.StatusBadRequest)
				return
			}
			middleware.WriteJSON(w, map[string]interface{}{
				"tenant": tenantID,
				"usage":  usage,
			}, http.StatusOK)
		},
	))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	klog.Infof("Starting server on port %d", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		klog.Fatalf("Server failed: %v", err)
	}
}

func getConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
EOF

echo "✅ main.go dibuat"

# ============================================
# 5. BUAT FILE AUTH MIDDLEWARE
# ============================================
echo ""
echo "📄 Step 5: Membuat auth middleware..."

cat > dya/pkg/apiserver/middleware/auth.go << 'EOF'
package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Claims struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	TenantID string `json:"tenantId"`
	Role     string `json:"role"`
	Expires  int64  `json:"exp"`
}

type AuthMiddleware struct {
	mu      sync.RWMutex
	apiKeys map[string]*Claims
}

func NewAuthMiddleware() *AuthMiddleware {
	m := &AuthMiddleware{
		apiKeys: make(map[string]*Claims),
	}
	m.apiKeys["dya-admin-key-2026"] = &Claims{
		UserID:   "admin",
		Username: "admin",
		TenantID: "tenant-1",
		Role:     "admin",
		Expires:  time.Now().Add(365 * 24 * time.Hour).Unix(),
	}
	return m
}

func (m *AuthMiddleware) GenerateAPIKey(tenantID, userID, role string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("dya-key-%s-%d", tenantID, time.Now().UnixNano())
	m.apiKeys[key] = &Claims{
		UserID:   userID,
		Username: userID,
		TenantID: tenantID,
		Role:     role,
		Expires:  time.Now().Add(24 * time.Hour).Unix(),
	}
	return key
}

func (m *AuthMiddleware) Authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		var apiKey string

		if authHeader != "" {
			apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			apiKey = strings.TrimPrefix(apiKey, "bearer ")
		} else {
			apiKey = r.URL.Query().Get("api_key")
		}

		if apiKey == "" {
			WriteJSON(w, map[string]string{"error": "missing api key"}, http.StatusUnauthorized)
			return
		}

		m.mu.RLock()
		claims, ok := m.apiKeys[apiKey]
		m.mu.RUnlock()

		if !ok {
			WriteJSON(w, map[string]string{"error": "invalid api key"}, http.StatusUnauthorized)
			return
		}

		if claims.Expires < time.Now().Unix() {
			WriteJSON(w, map[string]string{"error": "api key expired"}, http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), "claims", claims)
		ctx = context.WithValue(ctx, "tenantID", claims.TenantID)
		ctx = context.WithValue(ctx, "userID", claims.UserID)
		ctx = context.WithValue(ctx, "role", claims.Role)

		next(w, r.WithContext(ctx))
	}
}

func RequireRole(requiredRole string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			role, ok := r.Context().Value("role").(string)
			if !ok {
				WriteJSON(w, map[string]string{"error": "role not found"}, http.StatusForbidden)
				return
			}
			if role == "admin" || role == requiredRole {
				next(w, r)
				return
			}
			WriteJSON(w, map[string]string{"error": fmt.Sprintf("requires role: %s", requiredRole)}, http.StatusForbidden)
		}
	}
}

func GetTenantFromContext(ctx context.Context) (string, bool) {
	tenant, ok := ctx.Value("tenantID").(string)
	return tenant, ok
}

func WriteJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
EOF

echo "✅ auth.go dibuat"

# ============================================
# 6. BUAT FILE ENTERPRISE
# ============================================
echo ""
echo "📄 Step 6: Membuat enterprise files..."

cat > dya/pkg/enterprise/engine.go << 'EOF'
package enterprise

import (
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Engine struct {
	mu            sync.RWMutex
	tenants       []Tenant
	roles         []Role
	organizations []Organization
	auditors      []Auditor
	apiHandlers   []APIHandler
	running       bool
}

type Tenant struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Role struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Organization struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	TenantID    string    `json:"tenantId"`
	Members     []string  `json:"members"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Auditor interface {
	Audit(event interface{}) error
	Name() string
}

type APIHandler interface {
	Handle(request interface{}) (*APIResponse, error)
	Name() string
}

type APIResponse struct {
	Success   bool        `json:"success"`
	Data      interface{} `json:"data"`
	Error     string      `json:"error"`
	Timestamp time.Time   `json:"timestamp"`
}

func NewEngine() *Engine {
	return &Engine{
		tenants:       make([]Tenant, 0),
		roles:         make([]Role, 0),
		organizations: make([]Organization, 0),
		auditors:      make([]Auditor, 0),
		apiHandlers:   make([]APIHandler, 0),
		running:       false,
	}
}

func (e *Engine) RegisterTenant(tenant Tenant) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tenants = append(e.tenants, tenant)
	klog.Infof("Registered tenant: %s", tenant.Name)
}

func (e *Engine) RegisterRole(role Role) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.roles = append(e.roles, role)
	klog.Infof("Registered role: %s", role.Name)
}

func (e *Engine) RegisterOrganization(org Organization) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.organizations = append(e.organizations, org)
}

func (e *Engine) RegisterAuditor(auditor Auditor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.auditors = append(e.auditors, auditor)
}

func (e *Engine) RegisterAPIHandler(handler APIHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.apiHandlers = append(e.apiHandlers, handler)
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	klog.Info("Enterprise Engine started")
}

func (e *Engine) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	klog.Info("Enterprise Engine stopped")
}

func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.running
}

func (e *Engine) GetTenants() []Tenant {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tenants
}

func (e *Engine) GetRoles() []Role {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.roles
}

func (e *Engine) GetOrganizations() []Organization {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.organizations
}

func (e *Engine) Audit(event interface{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, auditor := range e.auditors {
		if err := auditor.Audit(event); err != nil {
			klog.Errorf("Auditor %s failed: %v", auditor.Name(), err)
		}
	}
}

func (e *Engine) HandleAPI(request interface{}) []*APIResponse {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*APIResponse, 0)
	for _, handler := range e.apiHandlers {
		resp, err := handler.Handle(request)
		if err != nil {
			klog.Errorf("API handler %s failed: %v", handler.Name(), err)
			continue
		}
		if resp != nil {
			results = append(results, resp)
		}
	}
	return results
}
EOF

cat > dya/pkg/enterprise/tenant_manager.go << 'EOF'
package enterprise

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type Quota struct {
	MaxNodes       int `json:"maxNodes"`
	MaxAssets      int `json:"maxAssets"`
	MaxEvents      int `json:"maxEvents"`
	MaxAIProcesses int `json:"maxAIProcesses"`
}

type QuotaUsed struct {
	Nodes       int `json:"nodes"`
	Assets      int `json:"assets"`
	Events      int `json:"events"`
	AIProcesses int `json:"aiProcesses"`
}

type TenantWithQuota struct {
	Tenant
	Quota     *Quota     `json:"quota"`
	Used      *QuotaUsed `json:"used"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

type TenantManager struct {
	mu          sync.RWMutex
	tenants     map[string]*TenantWithQuota
	ResourceMap map[string]map[string]bool
}

func NewTenantManager() *TenantManager {
	return &TenantManager{
		tenants:     make(map[string]*TenantWithQuota),
		ResourceMap: make(map[string]map[string]bool),
	}
}

func (tm *TenantManager) CreateTenant(id, name, description string, quota *Quota) (*TenantWithQuota, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.tenants[id]; exists {
		return nil, fmt.Errorf("tenant %s already exists", id)
	}

	if quota == nil {
		quota = &Quota{
			MaxNodes:       100,
			MaxAssets:      1000,
			MaxEvents:      10000,
			MaxAIProcesses: 10,
		}
	}

	tenant := &TenantWithQuota{
		Tenant: Tenant{
			ID:          id,
			Name:        name,
			Description: description,
		},
		Quota: quota,
		Used: &QuotaUsed{
			Nodes:       0,
			Assets:      0,
			Events:      0,
			AIProcesses: 0,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	tm.tenants[id] = tenant
	tm.ResourceMap[id] = make(map[string]bool)

	klog.Infof("Created tenant: %s (ID: %s)", name, id)
	return tenant, nil
}

func (tm *TenantManager) GetTenant(id string) (*TenantWithQuota, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tenant, ok := tm.tenants[id]
	return tenant, ok
}

func (tm *TenantManager) GetAllTenants() []*TenantWithQuota {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	result := make([]*TenantWithQuota, 0, len(tm.tenants))
	for _, t := range tm.tenants {
		result = append(result, t)
	}
	return result
}

func (tm *TenantManager) DeleteTenant(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, exists := tm.tenants[id]; !exists {
		return fmt.Errorf("tenant %s not found", id)
	}
	delete(tm.tenants, id)
	delete(tm.ResourceMap, id)
	return nil
}

func (tm *TenantManager) AddResourceUsage(tenantID, resourceID, resourceType string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %s not found", tenantID)
	}
	if tm.ResourceMap[tenantID] == nil {
		tm.ResourceMap[tenantID] = make(map[string]bool)
	}
	tm.ResourceMap[tenantID][resourceID] = true
	switch resourceType {
	case "node":
		tenant.Used.Nodes++
	case "asset":
		tenant.Used.Assets++
	case "event":
		tenant.Used.Events++
	case "ai":
		tenant.Used.AIProcesses++
	}
	tenant.UpdatedAt = time.Now()
	return nil
}

func (tm *TenantManager) GetTenantUsage(tenantID string) (*QuotaUsed, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	tenant, ok := tm.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("tenant %s not found", tenantID)
	}
	return &QuotaUsed{
		Nodes:       tenant.Used.Nodes,
		Assets:      tenant.Used.Assets,
		Events:      tenant.Used.Events,
		AIProcesses: tenant.Used.AIProcesses,
	}, nil
}
EOF

cat > dya/pkg/enterprise/auditors/basic.go << 'EOF'
package auditors

import "k8s.io/klog/v2"

type BasicAuditor struct{}

func (a *BasicAuditor) Audit(event interface{}) error {
	klog.V(4).Infof("Auditing event: %+v", event)
	return nil
}

func (a *BasicAuditor) Name() string {
	return "basic-auditor"
}
EOF

cat > dya/pkg/enterprise/handlers/basic.go << 'EOF'
package handlers

import (
	"time"

	"k8s.io/kubernetes/dya/pkg/enterprise"
)

type BasicHandler struct{}

func (h *BasicHandler) Handle(request interface{}) (*enterprise.APIResponse, error) {
	return &enterprise.APIResponse{
		Success:   true,
		Data:      map[string]string{"status": "ok"},
		Timestamp: time.Now(),
	}, nil
}

func (h *BasicHandler) Name() string {
	return "basic-handler"
}
EOF

echo "✅ Enterprise files dibuat"

# ============================================
# 7. BUAT FILE HEALTH
# ============================================
echo ""
echo "📄 Step 7: Membuat health checker..."

cat > dya/pkg/health/checker.go << 'EOF'
package health

import "sync"

type Checker struct {
	mu         sync.RWMutex
	components map[string]bool
	ready      bool
}

func NewChecker() *Checker {
	return &Checker{
		components: make(map[string]bool),
		ready:      false,
	}
}

func (c *Checker) SetComponent(name string, healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = healthy
}

func (c *Checker) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, healthy := range c.components {
		if !healthy {
			return false
		}
	}
	return true
}

func (c *Checker) SetReady(ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = ready
}

func (c *Checker) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}
EOF

echo "✅ health checker dibuat"

# ============================================
# 8. BUILD
# ============================================
echo ""
echo "🔨 Step 8: Build project..."

cd dya
go mod tidy 2>/dev/null || true
cd ..

go mod tidy 2>/dev/null || true

echo ""
echo "🔨 Building dya-controller..."
go build -v -o dya/bin/dya-controller ./dya/cmd/dya-controller

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ BUILD SUCCESS!"
    echo "   Binary: dya/bin/dya-controller"
else
    echo ""
    echo "❌ BUILD FAILED"
    echo "   Silakan periksa error di atas"
    exit 1
fi

# ============================================
# 9. DONE
# ============================================
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                                                              ║"
echo "║   ✅  FIX COMPLETE!  ✅                                      ║"
echo "║                                                              ║"
echo "║   Sekarang jalankan:                                        ║"
echo "║   ./dya/bin/dya-controller                                  ║"
echo "║                                                              ║"
echo "║   Test API:                                                 ║"
echo "║   curl http://localhost:8080/api/tenants?api_key=dya-admin-key-2026"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
