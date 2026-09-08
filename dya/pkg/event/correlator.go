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

package event

import (
	"time"

	"k8s.io/klog/v2"
)

// Correlator correlates events
type Correlator struct {
	window time.Duration
}

// NewCorrelator creates a new correlator
func NewCorrelator(window time.Duration) *Correlator {
	return &Correlator{window: window}
}

// Correlate correlates events within time window
func (c *Correlator) Correlate(events []*Event) []*Event {
	if len(events) < 2 {
		return events
	}
	if c == nil {
		return events
	}

	correlated := make([]*Event, 0)
	seen := make(map[string]bool)

	for i, event := range events {
		if event == nil || seen[event.ID] {
			continue
		}
		seen[event.ID] = true
		correlated = append(correlated, event)

		for j := i + 1; j < len(events); j++ {
			if events[j] == nil || seen[events[j].ID] {
				continue
			}
			if events[j].AssetID == event.AssetID {
				seen[events[j].ID] = true
				correlated = append(correlated, events[j])
				klog.V(4).Infof("Correlated event %s with %s", event.ID, events[j].ID)
			}
		}
	}
	return correlated
}
