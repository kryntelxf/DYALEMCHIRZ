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

package regions

import "time"

func GetGlobalRegions() []interface{} {
	return []interface{}{
		struct {
			ID          string
			Name        string
			Location    string
			Status      string
			Capacity    int64
			CreatedAt   time.Time
		}{
			ID:          "region-us-east",
			Name:        "US East",
			Location:    "us-east-1",
			Status:      "active",
			Capacity:    1000000,
			CreatedAt:   time.Now(),
		},
		struct {
			ID          string
			Name        string
			Location    string
			Status      string
			Capacity    int64
			CreatedAt   time.Time
		}{
			ID:          "region-us-west",
			Name:        "US West",
			Location:    "us-west-1",
			Status:      "active",
			Capacity:    1000000,
			CreatedAt:   time.Now(),
		},
		struct {
			ID          string
			Name        string
			Location    string
			Status      string
			Capacity    int64
			CreatedAt   time.Time
		}{
			ID:          "region-eu-west",
			Name:        "EU West",
			Location:    "eu-west-1",
			Status:      "active",
			Capacity:    1000000,
			CreatedAt:   time.Now(),
		},
		struct {
			ID          string
			Name        string
			Location    string
			Status      string
			Capacity    int64
			CreatedAt   time.Time
		}{
			ID:          "region-ap-southeast",
			Name:        "AP Southeast",
			Location:    "ap-southeast-1",
			Status:      "active",
			Capacity:    1000000,
			CreatedAt:   time.Now(),
		},
	}
}
