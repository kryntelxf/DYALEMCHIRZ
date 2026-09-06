package auditors

import (
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/policy"
)

type BasicAuditor struct{}

func (a *BasicAuditor) Name() string {
	return "basic-auditor"
}

func (a *BasicAuditor) Audit(result *policy.EvaluationResult) error {
	klog.V(4).Infof("Auditing policy: %s", result.PolicyID)
	return nil
}
