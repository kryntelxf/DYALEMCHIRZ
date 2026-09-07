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

package sdks

import (
	"time"
)

func GetOfficialSDKs() []interface{} {
	return []interface{}{
		struct {
			ID          string
			Name        string
			Language    string
			Version     string
			Repository  string
			Documentation string
			Status      string
			CreatedAt   time.Time
		}{
			ID:           "sdk-go",
			Name:         "Go SDK",
			Language:     "go",
			Version:      "1.0.0",
			Repository:   "https://github.com/kryntelxf/dya-sdk-go",
			Documentation: "https://docs.dyalemchirz.com/sdk/go",
			Status:       "stable",
			CreatedAt:    time.Now(),
		},
		struct {
			ID          string
			Name        string
			Language    string
			Version     string
			Repository  string
			Documentation string
			Status      string
			CreatedAt   time.Time
		}{
			ID:           "sdk-python",
			Name:         "Python SDK",
			Language:     "python",
			Version:      "1.0.0",
			Repository:   "https://github.com/kryntelxf/dya-sdk-python",
			Documentation: "https://docs.dyalemchirz.com/sdk/python",
			Status:       "stable",
			CreatedAt:    time.Now(),
		},
		struct {
			ID          string
			Name        string
			Language    string
			Version     string
			Repository  string
			Documentation string
			Status      string
			CreatedAt   time.Time
		}{
			ID:           "sdk-java",
			Name:         "Java SDK",
			Language:     "java",
			Version:      "1.0.0",
			Repository:   "https://github.com/kryntelxf/dya-sdk-java",
			Documentation: "https://docs.dyalemchirz.com/sdk/java",
			Status:       "beta",
			CreatedAt:    time.Now(),
		},
	}
}
