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
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// AuditLogEntry represents an audit log entry
type AuditLogEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	UserID      string    `json:"userId"`
	TenantID    string    `json:"tenantId"`
	Action      string    `json:"action"`
	Resource    string    `json:"resource"`
	IP          string    `json:"ip"`
	UserAgent   string    `json:"userAgent"`
	Status      int       `json:"status"`
	Message     string    `json:"message"`
}

// AuditMiddleware logs all requests
type AuditMiddleware struct {
	mu    sync.RWMutex
	logs  []AuditLogEntry
}

// NewAuditMiddleware creates a new audit middleware
func NewAuditMiddleware() *AuditMiddleware {
	return &AuditMiddleware{
		logs: make([]AuditLogEntry, 0),
	}
}

// Audit logs the request
func (a *AuditMiddleware) Audit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, _ := r.Context().Value("userID").(string)
		tenantID, _ := GetTenantFromContext(r.Context())
		
		entry := AuditLogEntry{
			Timestamp: time.Now(),
			UserID:    userID,
			TenantID:  tenantID,
			Action:    r.Method,
			Resource:  r.URL.Path,
			IP:        r.RemoteAddr,
			UserAgent: r.UserAgent(),
		}
		
		// Wrap response writer to capture status
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next(ww, r)
		
		entry.Status = ww.status
		
		a.mu.Lock()
		a.logs = append(a.logs, entry)
		// Keep only last 10000 logs
		if len(a.logs) > 10000 {
			a.logs = a.logs[len(a.logs)-10000:]
		}
		a.mu.Unlock()
		
		klog.V(4).Infof("Audit: user=%s action=%s resource=%s status=%d", 
			userID, entry.Action, entry.Resource, entry.Status)
	}
}

// GetAuditLogs returns audit logs
func (a *AuditMiddleware) GetAuditLogs() []AuditLogEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.logs
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
