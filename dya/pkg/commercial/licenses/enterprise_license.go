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

package licenses

import "time"

func GetEnterpriseLicense() interface{} {
	return struct {
		ID          string
		Name        string
		Type        string
		Features    []string
		Price       string
		CreatedAt   time.Time
	}{
		ID:   "license-enterprise",
		Name: "Enterprise License",
		Type: "enterprise",
		Features: []string{
			"Multi-tenancy",
			"RBAC",
			"Advanced Security",
			"24/7 Support",
			"SLA Guarantee",
			"Custom Integrations",
			"Dedicated Account Manager",
		},
		Price:     "Contact Sales",
		CreatedAt: time.Now(),
	}
}
