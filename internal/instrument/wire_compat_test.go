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
	// only at this JSON. The replay set is pinned as one object, in contract
	// order, so a tag typo in any of its eight keys fails here.
	for _, key := range []string{`"version"`, `"etag"`, `"rules"`, `"active_faults"`, `"freeze_cfg"`, `"fault_type"`, `"params"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("wire payload missing %s: %s", key, raw)
		}
	}
	wantRef := `"replay_set":{"id":"sha256:abc","experiment_id":"exp-1","phase_id":"phase-1","key_strategy":"canonical_v2","count":2,"sha256":"deadbeef","size_bytes":128,"url":"/api/v1/cache/artifacts/sha256:abc"}`
	if !strings.Contains(string(raw), wantRef) {
		t.Errorf("replay_set not pinned in contract order\nwant: %s\ngot:  %s", wantRef, raw)
	}
	var decoded wire.RuleSync
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Etag != sync.Etag || decoded.ReplaySet == nil || *decoded.ReplaySet != *sync.ReplaySet {
		t.Fatalf("etag/replay_set did not round-trip: %+v", decoded)
	}

	// omitempty contract: an unset etag and an absent replay set stay off the wire.
	emptyRaw, err := json.Marshal(wire.RuleSync{Version: 8})
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	for _, key := range []string{`"etag"`, `"replay_set"`} {
		if strings.Contains(string(emptyRaw), key) {
			t.Errorf("empty RuleSync must omit %s: %s", key, emptyRaw)
		}
	}

	// Apply the decoded payload into fresh targets.
	eval := evaluator.NewStaticEvaluator()
	demo := &demoEvaluator{}
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()

	if _, err := apply(decoded, applyTargets{Evaluator: eval, DemoEval: demo, CacheBox: cb}, scopeAll); err != nil {
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

// TestRuleSync_EmptyVsAbsentSemantics pins apply's reconciliation contract at
// the wire: a null rules or active_faults list means "no change", while an
// explicit [] is authoritative desired state and clears the category -- which
// is why wire.RuleSync marshals both fields instead of omitting them. (These
// bodies carry an etag: one with neither an etag nor rules is the no-document
// register answer, which applies nothing at all.)
func TestRuleSync_EmptyVsAbsentSemantics(t *testing.T) {
	// Arm one slot first.
	demo := &demoEvaluator{}
	armed := wire.RuleSync{Version: 7, Etag: "7-a", ActiveFaults: []wire.FaultRequest{{
		ID:        "f1",
		FaultType: "latency",
		Params:    json.RawMessage(`{"delay":"100ms"}`),
	}}}
	if _, err := apply(armed, applyTargets{DemoEval: demo}, scopeAll); err != nil {
		t.Fatalf("arm: %v", err)
	}
	if len(demo.Active()) != 1 {
		t.Fatal("expected one armed slot")
	}
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})
	targets := applyTargets{Evaluator: eval, DemoEval: demo}

	// roundTrip marshals s, checks the wire carries keys, and decodes it the
	// way the poll does.
	roundTrip := func(s wire.RuleSync, keys ...string) wire.RuleSync {
		t.Helper()
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range keys {
			if !strings.Contains(string(raw), key) {
				t.Fatalf("wire.RuleSync must carry %s explicitly: %s", key, raw)
			}
		}
		var decoded wire.RuleSync
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return decoded
	}

	// Null lists: no change.
	absent := roundTrip(wire.RuleSync{Version: 8, Etag: "8-b"}, `"rules":null`, `"active_faults":null`)
	if _, err := apply(absent, targets, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := eval.Rules(); len(got) != 1 || got[0].Name != "keep" {
		t.Errorf("a null rules list must be a no-op, got %+v", got)
	}
	if got := demo.Active(); len(got) != 1 {
		t.Errorf("a null active_faults list must be a no-op, got %+v", got)
	}

	// Explicit empty lists: authoritative clears.
	empty := roundTrip(wire.RuleSync{Version: 9, Etag: "9-c", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}},
		`"rules":[]`, `"active_faults":[]`)
	if _, err := apply(empty, targets, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := eval.Rules(); len(got) != 0 {
		t.Errorf("an empty rules list must clear the rule set, got %+v", got)
	}
	if got := demo.Active(); len(got) != 0 {
		t.Errorf("an empty active_faults list must clear manual slots, got %+v", got)
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
