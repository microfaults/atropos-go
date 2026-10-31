package instrument

// Cross-repo wire contract tests for the manteion↔SDK sync payload. The
// historical bug class here: manteion emitted "active_fault" (singular,
// object) while the SDK decoded "active_faults" (plural, array) and silently
// dropped the intent. Both ends now marshal/unmarshal wire.RuleSync, and
// this test guards the full marshal → unmarshal → Apply loop including the
// omitted-vs-empty field semantics Apply's reconciliation relies on.

import (
	"encoding/json"
	"strings"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func TestRuleSync_WireRoundTripAndApply(t *testing.T) {
	sync := wire.RuleSync{
		Version: 7,
		Rules: []wire.CompiledRule{
			{
				Name:           "lat-rule",
				InjectionPoint: "ingress",
				Labels:         map[string]string{"workflow": "browse"},
				Mode:           "background",
				Priority:       50,
				Fault: &wire.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"250ms","jitter":"50ms"}`),
				},
			},
			{
				Name:           "cb-rule",
				InjectionPoint: "egress",
				Mode:           "inline",
				CacheBox:       &wire.CompiledCacheBox{Mode: "replay_with_delay", KeyStrategy: "exact_with_host"},
			},
		},
		ActiveFaults: []wire.FaultRequest{{
			ID:         "cfg-123",
			Category:   "resource",
			FaultType:  "cpu",
			DurationMs: 60000,
			Params:     json.RawMessage(`{"target_load":0.7}`),
		}},
		FreezeCfg: &wire.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42},
	}
	// The ETag and the replay set (installed under an experiment/phase pair)
	// must both survive the wire.
	sync.Etag = "7-9f1c0e3a7b2d4f60"
	sync.ReplaySet = &wire.ReplaySetRef{
		ID: "sha256:abc", ExperimentID: "exp-1", PhaseID: "phase-1",
		KeyStrategy: "canonical_v2", Count: 2,
		SHA256: "deadbeef", SizeBytes: 128, URL: "/api/v1/cache/artifacts/sha256:abc",
	}

	// Wire round-trip.
	raw, err := json.Marshal(sync)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Key names are load-bearing: manteion handlers and the SDK client meet
	// only at this JSON.
	for _, key := range []string{`"version"`, `"etag"`, `"rules"`, `"active_faults"`, `"freeze_cfg"`,
		`"replay_set"`, `"experiment_id"`, `"phase_id"`, `"key_strategy"`, `"size_bytes"`,
		`"fault_type"`, `"params"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("wire payload missing %s: %s", key, raw)
		}
	}
	var decoded wire.RuleSync
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Etag != "7-9f1c0e3a7b2d4f60" || decoded.ReplaySet == nil || decoded.ReplaySet.Count != 2 ||
		decoded.ReplaySet.ExperimentID != "exp-1" || decoded.ReplaySet.PhaseID != "phase-1" {
		t.Fatalf("etag/replay_set did not round-trip: %+v", decoded)
	}

	// Apply the decoded payload into fresh targets.
	eval := evaluator.NewStaticEvaluator()
	demo := &demoEvaluator{}
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()

	resp := wire.RegisterResponse{Status: "poll", RuleSync: decoded}
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
// "clear all manual faults" — which is why wire.RuleSync marshals both fields
// explicitly instead of omitempty.
func TestRuleSync_EmptyVsAbsentSemantics(t *testing.T) {
	// Arm one slot first.
	demo := &demoEvaluator{}
	armed := wire.RegisterResponse{RuleSync: wire.RuleSync{
		ActiveFaults: []wire.FaultRequest{{
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
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})

	// A marshalled empty wire.RuleSync carries explicit empty arrays.
	raw, _ := json.Marshal(wire.RuleSync{Version: 8})
	for _, key := range []string{`"rules"`, `"active_faults"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("empty wire.RuleSync must still carry %s explicitly: %s", key, raw)
		}
	}
	var decoded wire.RuleSync
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	resp := wire.RegisterResponse{Status: "poll", RuleSync: decoded}
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

// TestStatusReport_WireKeys pins the status report the instance posts back to
// manteion. Unsupported and Errors are emitted even when empty: an empty list
// is how the instance clears the previous report.
func TestStatusReport_WireKeys(t *testing.T) {
	raw, err := json.Marshal(wire.StatusReport{
		InstanceID: "pod-1", Service: "cart", AppliedGeneration: 42, Etag: "42-abc",
		Unsupported: []wire.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}},
		Errors:      []string{},
		ReplaySet:   &wire.ReplaySetStatus{ID: "sha256:abc", State: wire.ReplaySetCommitted},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"instance_id":"pod-1"`, `"service":"cart"`, `"applied_generation":42`, `"etag":"42-abc"`,
		`"unsupported":[{"rule":"cpu-quota","category":"resource"}]`, `"errors":[]`, `"replay_set":{"id":"sha256:abc","state":"committed"}`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("status report missing %s: %s", key, raw)
		}
	}
	// An empty report still carries both lists explicitly: empty clears the previous report.
	raw, _ = json.Marshal(wire.StatusReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{}})
	if !strings.Contains(string(raw), `"unsupported":[]`) || !strings.Contains(string(raw), `"errors":[]`) || strings.Contains(string(raw), `"replay_set"`) {
		t.Fatalf("empty report = %s", raw)
	}
}
