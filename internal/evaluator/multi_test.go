package evaluator

import (
	"context"
	"testing"
)

type mockEvaluator struct {
	decision *Decision
}

func (m *mockEvaluator) Evaluate(ctx context.Context, req Request) *Decision {
	return m.decision
}

func TestMultiEvaluator(t *testing.T) {
	d1 := &Decision{Name: "first"}
	d2 := &Decision{Name: "second"}

	tests := []struct {
		name       string
		evaluators []Evaluator
		want       *Decision
	}{
		{
			name:       "empty",
			evaluators: []Evaluator{},
			want:       nil,
		},
		{
			name: "all nil",
			evaluators: []Evaluator{
				nil,
				&mockEvaluator{decision: nil},
			},
			want: nil,
		},
		{
			name: "first non-nil wins",
			evaluators: []Evaluator{
				&mockEvaluator{decision: nil},
				&mockEvaluator{decision: d1},
				&mockEvaluator{decision: d2},
			},
			want: d1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMultiEvaluator(tt.evaluators...)
			got := m.Evaluate(context.Background(), Request{})
			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}
