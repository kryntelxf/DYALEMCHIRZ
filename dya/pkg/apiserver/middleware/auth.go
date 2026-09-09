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

// Claims represents JWT claims
type Claims struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	TenantID string `json:"tenantId"`
	Role     string `json:"role"`
	Expires  int64  `json:"exp"`
}

// AuthMiddleware handles authentication
type AuthMiddleware struct {
	mu      sync.RWMutex
	apiKeys map[string]*Claims
}

// NewAuthMiddleware creates a new auth middleware
func NewAuthMiddleware() *AuthMiddleware {
	m := &AuthMiddleware{
		apiKeys: make(map[string]*Claims),
	}

	// Default admin key (GANTI DI PRODUCTION!)
	m.apiKeys["dya-admin-key-2026"] = &Claims{
		UserID:   "admin",
		Username: "admin",
		TenantID: "tenant-1",
		Role:     "admin",
		Expires:  time.Now().Add(365 * 24 * time.Hour).Unix(),
	}

	return m
}

// GenerateAPIKey creates a new API key
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

// Authenticate middleware
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

		klog.V(4).Infof("Authenticated: user=%s tenant=%s role=%s",
			claims.UserID, claims.TenantID, claims.Role)

		next(w, r.WithContext(ctx))
	}
}

// RequireRole checks if user has required role
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

// GetClaimsFromContext gets claims from context
func GetClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value("claims").(*Claims)
	return claims, ok
}

// GetTenantFromContext gets tenant from context
func GetTenantFromContext(ctx context.Context) (string, bool) {
	tenant, ok := ctx.Value("tenantID").(string)
	return tenant, ok
}

// WriteJSON helper
func WriteJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
