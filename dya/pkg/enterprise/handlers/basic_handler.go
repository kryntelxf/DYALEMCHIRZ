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

package handlers

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/enterprise"
)

type BasicHandler struct{}

func (h *BasicHandler) Name() string {
	return "basic-handler"
}

func (h *BasicHandler) Handle(request interface{}) (*enterprise.APIResponse, error) {
	klog.V(4).Info("BasicHandler processing request")
	return &enterprise.APIResponse{
		Success:   true,
		Data:      map[string]string{"status": "ok"},
		Error:     "",
		Timestamp: time.Now(),
	}, nil
}
