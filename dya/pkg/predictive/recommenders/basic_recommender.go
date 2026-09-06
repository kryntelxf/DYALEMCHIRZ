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

package recommenders

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/predictive"
)

type BasicRecommender struct{}

func (r *BasicRecommender) Name() string {
	return "basic-recommender"
}

func (r *BasicRecommender) Recommend(data interface{}) (*predictive.Recommendation, error) {
	klog.V(4).Info("BasicRecommender running")
	return &predictive.Recommendation{
		AssetID:     "unknown",
		Action:      "monitor",
		Description: "Monitor resource usage for potential issues",
		Priority:    "low",
		Timestamp:   time.Now(),
	}, nil
}
