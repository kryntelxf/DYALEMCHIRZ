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

package forecasters

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/predictive"
)

type CapacityForecaster struct{}

func (f *CapacityForecaster) Name() string {
	return "capacity-forecaster"
}

func (f *CapacityForecaster) Forecast(data interface{}) (*predictive.Forecast, error) {
	klog.V(4).Info("CapacityForecaster running")
	return &predictive.Forecast{
		AssetID:    "unknown",
		Metric:     "cpu",
		Values:     []float64{75.0, 78.0, 80.0, 82.0},
		Timestamps: []time.Time{time.Now(), time.Now().Add(1 * time.Hour), time.Now().Add(2 * time.Hour), time.Now().Add(3 * time.Hour)},
		Confidence: 90.0,
		Timestamp:  time.Now(),
	}, nil
}
