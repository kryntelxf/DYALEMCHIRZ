package enforcers

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/policy"
)

type BasicEnforcer struct{}

func (e *BasicEnforcer) Name() string {
	return "basic-enforcer"
}

func (e *BasicEnforcer) Enforce(result *policy.EvaluationResult) (*policy.EnforcementResult, error) {
	klog.V(4).Infof("Enforcing policy: %s", result.PolicyID)
	return &policy.EnforcementResult{
		PolicyID:  result.PolicyID,
		Action:    "enforce",
		Success:   true,
		Message:   "Policy enforced successfully",
		Timestamp: time.Now(),
	}, nil
}
