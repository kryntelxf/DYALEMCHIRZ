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
)

// TenantIsolation middleware ensures tenant isolation
func TenantIsolation(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := GetTenantFromContext(r.Context())
		if !ok {
			WriteJSON(w, map[string]string{"error": "tenant not found"}, http.StatusForbidden)
			return
		}
		
		// Check if resource belongs to tenant
		resourceTenant := r.URL.Query().Get("tenant")
		if resourceTenant != "" && resourceTenant != tenantID {
			role, _ := r.Context().Value("role").(string)
			if role != "admin" {
				WriteJSON(w, map[string]string{"error": "access denied to this tenant"}, http.StatusForbidden)
				return
			}
		}
		
		next(w, r)
	}
}

// IsAdmin checks if current user is admin
func IsAdmin(r *http.Request) bool {
	role, ok := r.Context().Value("role").(string)
	return ok && role == "admin"
}

// GetCurrentTenantID returns the current tenant ID from context
func GetCurrentTenantID(r *http.Request) string {
	tenant, _ := GetTenantFromContext(r.Context())
	return tenant
}
