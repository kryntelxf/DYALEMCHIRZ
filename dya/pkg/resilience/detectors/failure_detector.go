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

package detectors

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/resilience"
)

// FailureDetector detects failures
type FailureDetector struct{}

// Name returns the name of the detector
func (d *FailureDetector) Name() string {
	return "failure-detector"
}

// Detect detects failures
func (d *FailureDetector) Detect(asset interface{}) (*resilience.Failure, error) {
	klog.V(4).Info("FailureDetector running")
	return nil, nil
}
