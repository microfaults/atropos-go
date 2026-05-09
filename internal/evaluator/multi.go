package evaluator

import (
	"context"
)

// MultiEvaluator chains multiple evaluators and returns the first non-nil Decision.
// It is safe for concurrent use as long as the underlying evaluators are safe.
type MultiEvaluator struct {
	evaluators []Evaluator
}

// NewMultiEvaluator creates a new MultiEvaluator that iterates through the provided
// evaluators in order.
func NewMultiEvaluator(evaluators ...Evaluator) *MultiEvaluator {
	return &MultiEvaluator{
		evaluators: evaluators,
	}
}

// Evaluate calls Evaluate on each underlying evaluator in order. It returns the
// first non-nil Decision it receives. If all evaluators return nil, it returns nil.
func (m *MultiEvaluator) Evaluate(ctx context.Context, req Request) *Decision {
	for _, e := range m.evaluators {
		if e == nil {
			continue
		}
		if decision := e.Evaluate(ctx, req); decision != nil {
			return decision
		}
	}
	return nil
}
