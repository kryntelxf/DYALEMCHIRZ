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

type GoSDK struct{}

func NewGoSDK() interface{} {
	return struct {
		ID          string
		Name        string
		Version     string
		Language    string
		Repository  string
		Description string
		CreatedAt   time.Time
	}{
		ID:          "sdk-go",
		Name:        "DYALEMCHIRZ Go SDK",
		Version:     "1.0.0",
		Language:    "go",
		Repository:  "https://github.com/kryntelxf/dya-sdk-go",
		Description: "Go SDK for DYALEMCHIRZ platform",
		CreatedAt:   time.Now(),
	}
}
