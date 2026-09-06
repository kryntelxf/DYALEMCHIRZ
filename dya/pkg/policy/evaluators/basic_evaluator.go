package evaluators

import (
	"time"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/dya/pkg/policy"
)

type BasicEvaluator struct{}

func (e *BasicEvaluator) Name() string {
	return "basic-evaluator"
}

func (e *BasicEvaluator) Evaluate(p *policy.Policy, context interface{}) (*policy.EvaluationResult, error) {
	klog.V(4).Infof("Evaluating policy: %s", p.Name)
	return &policy.EvaluationResult{
		PolicyID:  p.ID,
		Compliant: true,
		Message:   "Policy compliant",
		Timestamp: time.Now(),
	}, nil
}
