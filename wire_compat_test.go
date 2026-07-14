package atropos

// Cross-repo wire contract tests for the manteion↔SDK sync payload. The
// historical bug class here: manteion emitted "active_fault" (singular,
// object) while the SDK decoded "active_faults" (plural, array) and silently
// dropped the intent. Both ends now marshal/unmarshal RuleSync, and
// this test guards the full marshal → unmarshal → Apply loop including the
// omitted-vs-empty field semantics Apply's reconciliation relies on.

import (
	"encoding/json"
	"strings"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
)

func TestRuleSync_WireRoundTripAndApply(t *testing.T) {
	sync := RuleSync{
		Version: 7,
		Rules: []CompiledRule{
			{
				Name:           "lat-rule",
				InjectionPoint: "ingress",
				Labels:         map[string]string{"workflow": "browse"},
				Mode:           "background",
				Priority:       50,
				Fault: &CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"250ms","jitter":"50ms"}`),
				},
			},
			{
				Name:           "cb-rule",
				InjectionPoint: "egress",
				Mode:           "inline",
				CacheBox:       &CompiledCacheBox{Mode: "replay_with_delay", KeyStrategy: "exact_with_host"},
			},
		},
		ActiveFaults: []FaultRequest{{
			ID:         "cfg-123",
			Category:   "resource",
			FaultType:  "cpu",
			DurationMs: 60000,
			Params:     json.RawMessage(`{"target_load":0.7}`),
		}},
		FreezeCfg: &DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42},
	}

	// Wire round-trip.
	raw, err := json.Marshal(sync)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Key names are load-bearing: manteion handlers and the SDK client meet
	// only at this JSON.
	for _, key := range []string{`"version"`, `"rules"`, `"active_faults"`, `"freeze_cfg"`, `"fault_type"`, `"params"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("wire payload missing %s: %s", key, raw)
		}
	}
	var decoded RuleSync
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Apply the decoded payload into fresh targets.
	eval := evaluator.NewStaticEvaluator()
	demo := &demoEvaluator{}
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()

	resp := RegisterResponse{Status: "poll", RuleSync: decoded}
	if err := apply(resp, applyTargets{Evaluator: eval, DemoEval: demo, CacheBox: cb}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := len(eval.Rules()); got != 2 {
		t.Errorf("rules applied = %d, want 2", got)
	}
	active := demo.Active()
	if len(active) != 1 || active[0].ID != "cfg-123" {
		t.Fatalf("active faults = %+v, want one slot cfg-123", active)
	}
}

// TestRuleSync_EmptyVsAbsentSemantics pins Apply's reconciliation contract:
// an empty rules list is "no change", but an empty active_faults list means
// "clear all manual faults" — which is why RuleSync marshals both fields
// explicitly instead of omitempty.
func TestRuleSync_EmptyVsAbsentSemantics(t *testing.T) {
	// Arm one slot first.
	demo := &demoEvaluator{}
	armed := RegisterResponse{RuleSync: RuleSync{
		ActiveFaults: []FaultRequest{{
			ID:        "f1",
			FaultType: "latency",
			Params:    json.RawMessage(`{"delay":"100ms"}`),
		}},
	}}
	if err := apply(armed, applyTargets{DemoEval: demo}); err != nil {
		t.Fatalf("arm: %v", err)
	}
	if len(demo.Active()) != 1 {
		t.Fatal("expected one armed slot")
	}

	// Pre-existing rules must survive an empty rules list.
	eval := evaluator.NewStaticEvaluator(StaticRule{Name: "keep", Point: evaluator.Ingress})

	// A marshalled empty RuleSync carries explicit empty arrays.
	raw, _ := json.Marshal(RuleSync{Version: 8})
	for _, key := range []string{`"rules"`, `"active_faults"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("empty RuleSync must still carry %s explicitly: %s", key, raw)
		}
	}
	var decoded RuleSync
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	resp := RegisterResponse{Status: "poll", RuleSync: decoded}
	if err := apply(resp, applyTargets{Evaluator: eval, DemoEval: demo}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := eval.Rules(); len(got) != 1 || got[0].Name != "keep" {
		t.Errorf("empty rules list must be a no-op, got %+v", got)
	}
	if got := demo.Active(); len(got) != 0 {
		t.Errorf("empty active_faults must clear manual slots, got %+v", got)
	}
}
