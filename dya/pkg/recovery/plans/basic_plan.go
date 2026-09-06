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

package plans

import (
	"time"

	"k8s.io/kubernetes/dya/pkg/recovery"
)

type BasicPlan struct {
	assetID string
	name    string
	steps   []recovery.Step
}

func NewBasicPlan(assetID string) *BasicPlan {
	return &BasicPlan{
		assetID: assetID,
		name:    "basic-recovery-" + assetID,
		steps: []recovery.Step{
			{
				ID:          "step-1",
				Name:        "Investigate failure",
				Action:      "investigate",
				Description: "Investigate the cause of failure",
				Timeout:     30 * time.Second,
			},
			{
				ID:          "step-2",
				Name:        "Restart service",
				Action:      "restart",
				Description: "Restart the failed service",
				Timeout:     60 * time.Second,
			},
			{
				ID:          "step-3",
				Name:        "Verify recovery",
				Action:      "verify",
				Description: "Verify that the service is recovered",
				Timeout:     30 * time.Second,
			},
		},
	}
}

func (p *BasicPlan) GetSteps() []recovery.Step {
	return p.steps
}

func (p *BasicPlan) GetAssetID() string {
	return p.assetID
}

func (p *BasicPlan) GetPriority() int {
	return 1
}

func (p *BasicPlan) RequiresApproval() bool {
	return false
}

func (p *BasicPlan) Name() string {
	return p.name
}
