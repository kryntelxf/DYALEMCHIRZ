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

package checkers

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/resilience"
)

type HealthChecker struct{}

func (c *HealthChecker) Name() string {
	return "health-checker"
}

func (c *HealthChecker) Check(asset interface{}) (*resilience.HealthStatus, error) {
	klog.V(4).Info("HealthChecker running")
	return &resilience.HealthStatus{
		AssetID:   "unknown",
		Status:    "Healthy",
		Score:     100.0,
		Message:   "Asset is healthy",
		Timestamp: time.Now(),
	}, nil
}
