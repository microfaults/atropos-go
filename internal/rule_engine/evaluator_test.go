package rule_engine

import (
	"testing"
)

// ─── Mock policy ────────────────────────────────────────────────────

// mockPolicy implements CompiledPolicy for unit testing.
type mockPolicy struct {
	matched   bool
	faultSpec *FaultSpec
	err       error
}

func (m *mockPolicy) Eval(_ map[string]any) (bool, *FaultSpec, error) {
	return m.matched, m.faultSpec, m.err
}

// mockCompiler implements PolicyCompiler using a lookup table of
// pre-configured mock policies.
type mockCompiler struct {
	policies map[string]*mockPolicy
}

func (mc *mockCompiler) Compile(policyID string, _ string) (CompiledPolicy, error) {
	if p, ok := mc.policies[policyID]; ok {
		return p, nil
	}
	// Default: matching policy with no fault_spec.
	return &mockPolicy{matched: true}, nil
}

// ─── Helpers ────────────────────────────────────────────────────────

func cpuSpec() FaultSpec {
	return FaultSpec{
		FaultType:      FaultCPU,
		InjectionPoint: InjectInbound,
		DurationMs:     5000,
		CPU:            &CPUParams{TargetLoad: 0.8, WindowMs: 100},
	}
}

func latencySpec() FaultSpec {
	return FaultSpec{
		FaultType:      FaultLatency,
		InjectionPoint: InjectOutbound,
		DurationMs:     3000,
		Latency:        &LatencyParams{DelayMs: 1000, Jitter: 0.2},
	}
}

// ─── Tests ──────────────────────────────────────────────────────────

func TestEvaluator_SingleConditionSingleFault_Match(t *testing.T) {
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"cond1": {matched: true},
	}}

	spec := cpuSpec()
	b := NewBuilder(mc)
	b.AddCondition("cond1", "Cond", "package cond1\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddFault("fault1", "Fault", spec)
	b.Link("cond1", "fault1")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Spec.FaultType != FaultCPU {
		t.Errorf("expected fault_type=cpu, got %q", results[0].Spec.FaultType)
	}
	if len(results[0].MatchedPath) != 2 || results[0].MatchedPath[0] != "cond1" || results[0].MatchedPath[1] != "fault1" {
		t.Errorf("unexpected path: %v", results[0].MatchedPath)
	}
}

func TestEvaluator_SingleConditionSingleFault_NoMatch(t *testing.T) {
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"cond1": {matched: false},
	}}

	spec := cpuSpec()
	b := NewBuilder(mc)
	b.AddCondition("cond1", "Cond", "package cond1\nimport rego.v1\ndefault match := false")
	b.AddFault("fault1", "Fault", spec)
	b.Link("cond1", "fault1")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results (short-circuited), got %d", len(results))
	}
}

func TestEvaluator_ChainShortCircuitOnSecond(t *testing.T) {
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"cond1": {matched: true},
		"cond2": {matched: false},
	}}

	spec := cpuSpec()
	b := NewBuilder(mc)
	b.AddCondition("cond1", "First", "package cond1\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddCondition("cond2", "Second", "package cond2\nimport rego.v1\ndefault match := false")
	b.AddFault("fault1", "Fault", spec)
	b.Link("cond1", "cond2")
	b.Link("cond2", "fault1")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results (cond2 short-circuits), got %d", len(results))
	}
}

func TestEvaluator_BranchingMultipleChildren(t *testing.T) {
	// cond1 → cond2 → fault1  (cond2 matches)
	// cond1 → cond3 → fault2  (cond3 does NOT match)
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"cond1": {matched: true},
		"cond2": {matched: true},
		"cond3": {matched: false},
	}}

	cpuS := cpuSpec()
	latS := latencySpec()

	b := NewBuilder(mc)
	b.AddCondition("cond1", "Root", "package cond1\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddCondition("cond2", "Branch A", "package cond2\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddCondition("cond3", "Branch B", "package cond3\nimport rego.v1\ndefault match := false")
	b.AddFault("fault1", "CPU", cpuS)
	b.AddFault("fault2", "Latency", latS)
	b.Link("cond1", "cond2")
	b.Link("cond1", "cond3")
	b.Link("cond2", "fault1")
	b.Link("cond3", "fault2")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result (branch B pruned), got %d", len(results))
	}
	if results[0].Spec.FaultType != FaultCPU {
		t.Errorf("expected cpu fault from branch A, got %q", results[0].Spec.FaultType)
	}
}

func TestEvaluator_MultipleRoots(t *testing.T) {
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"root1": {matched: true},
		"root2": {matched: true},
	}}

	s1 := cpuSpec()
	s2 := latencySpec()

	b := NewBuilder(mc)
	b.AddCondition("root1", "Root 1", "package root1\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddCondition("root2", "Root 2", "package root2\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddFault("fault1", "CPU", s1)
	b.AddFault("fault2", "Latency", s2)
	b.Link("root1", "fault1")
	b.Link("root2", "fault2")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results from 2 roots, got %d", len(results))
	}

	types := map[FaultKind]bool{}
	for _, r := range results {
		types[r.Spec.FaultType] = true
	}
	if !types[FaultCPU] || !types[FaultLatency] {
		t.Errorf("expected both cpu and latency faults, got %v", types)
	}
}

func TestEvaluator_HybridConditionWithFaultSpec(t *testing.T) {
	hybridSpec := cpuSpec()
	mc := &mockCompiler{policies: map[string]*mockPolicy{
		"hybrid": {matched: true, faultSpec: &hybridSpec},
	}}

	leafSpec := latencySpec()

	b := NewBuilder(mc)
	b.AddCondition("hybrid", "Hybrid", "package hybrid\nimport rego.v1\ndefault match := false\nmatch if { true }")
	b.AddFault("leaf", "Leaf", leafSpec)
	b.Link("hybrid", "leaf")

	graph, policies, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	eval := NewGraphEvaluator(graph, policies)
	results, err := eval.Evaluate(map[string]any{})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	// Should get 2 results: one from the hybrid node, one from the leaf.
	if len(results) != 2 {
		t.Fatalf("expected 2 results (hybrid + leaf), got %d", len(results))
	}

	types := map[FaultKind]bool{}
	for _, r := range results {
		types[r.Spec.FaultType] = true
	}
	if !types[FaultCPU] || !types[FaultLatency] {
		t.Errorf("expected both cpu (hybrid) and latency (leaf), got %v", types)
	}
}
