package instrument

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func TestRegister_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/sdk/register" {
			t.Errorf("path = %s, want /api/v1/sdk/register", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req wire.RegisterRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Service != "productcatalog" {
			t.Errorf("service = %q, want productcatalog", req.Service)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(wire.RegisterResponse{
			Status: "registered",
			RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Mode:           "inline",
				Priority:       10,
				Fault: &wire.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"200ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	resp, err := registerWith(context.Background(), http.DefaultClient, server.URL, wire.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.Status != "registered" {
		t.Errorf("status = %q, want registered", resp.Status)
	}
	if len(resp.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(resp.Rules))
	}
	if resp.Rules[0].Fault == nil {
		t.Fatal("expected Fault to be set on compiled rule")
	}
}

func TestRegister_NonCreatedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
	}))
	defer server.Close()

	_, err := registerWith(context.Background(), http.DefaultClient, server.URL, wire.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err == nil {
		t.Fatal("expected error for non-201 response")
	}
	// Assert the status code surfaces in the error — operators rely on log
	// content to diagnose rejected registrations.
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status 400, got: %v", err)
	}
}

func TestRegisterWithClient_UsesSuppliedClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(wire.RegisterResponse{Status: "registered"})
	}))
	defer server.Close()

	custom := &http.Client{Timeout: 3 * time.Second}
	resp, err := registerWith(context.Background(), custom, server.URL, wire.RegisterRequest{ID: "pod-1", Service: "svc", Address: "http://10.0.0.1:8080"})
	if err != nil || resp.Status != "registered" {
		t.Fatalf("registerWith: err=%v status=%q", err, resp.Status)
	}
}

func TestApply_SetsRules(t *testing.T) {
	eval := evaluator.NewStaticEvaluator()
	resp := wire.RegisterResponse{
		RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{{
			Name:           "r1",
			InjectionPoint: "egress",
			Mode:           "inline",
			Fault: &wire.CompiledFault{
				Category:  "inline",
				FaultType: "latency",
				Params:    json.RawMessage(`{"delay":"100ms"}`),
			},
		}}},
	}

	if _, err := apply(resp.RuleSync, applyTargets{Evaluator: eval}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 {
		t.Fatalf("rules set = %d, want 1", len(rules))
	}
	if rules[0].Name != "r1" {
		t.Errorf("rule name = %q", rules[0].Name)
	}
}

// TestApply_NoRulesIsNoop pins the 'no change' half of the rules contract:
// a document (it has an etag) whose rules field is null leaves the rule set
// alone.
func TestApply_NoRulesIsNoop(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "preexisting", Point: evaluator.Ingress})
	resp := wire.RegisterResponse{Status: "registered", RuleSync: wire.RuleSync{Version: 1, Etag: "1-a"}}
	if _, err := apply(resp.RuleSync, applyTargets{Evaluator: eval}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 || rules[0].Name != "preexisting" {
		t.Errorf("Apply clobbered existing rules when response had none: got %+v", rules)
	}
}

func TestApply_RulesWithoutEvaluatorErrors(t *testing.T) {
	resp := wire.RegisterResponse{
		RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{{Name: "r1", InjectionPoint: "egress", Mode: "inline"}}},
	}
	_, err := apply(resp.RuleSync, applyTargets{}, scopeAll)
	if err == nil || !strings.HasPrefix(err.Error(), "apply rules: ") {
		t.Fatalf("err = %v, want the 'apply rules: ' hard error: rules present but no Evaluator target", err)
	}
}

// TestApply_HardErrorAppliesNothing: a hard error (a category present with
// no target to apply it to) is found before anything is applied, so the
// targets keep exactly what they held -- "keeping stale state" is literal.
func TestApply_HardErrorAppliesNothing(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})
	cb := cachebox.New(cachebox.Config{})
	t.Cleanup(cb.Stop)
	cb.SetDelaySource(cachebox.NewDistributionDelaySource(1, 0.5, 1))
	sync := wire.RuleSync{Version: 2, Etag: "2-b",
		Rules:        []wire.CompiledRule{{Name: "r1", InjectionPoint: "egress", Mode: "inline"}},
		ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
	}
	_, err := apply(sync, applyTargets{Evaluator: eval, CacheBox: cb}, scopeAll) // no DemoEval for the slot
	if err == nil || !strings.HasPrefix(err.Error(), "apply active_faults: ") {
		t.Fatalf("err = %v, want the 'apply active_faults: ' hard error", err)
	}
	if rules := eval.Rules(); len(rules) != 1 || rules[0].Name != "keep" {
		t.Fatalf("a hard error must replace no rules, got %+v", rules)
	}
	if !cb.Stats().Delay.Installed {
		t.Fatal("a hard error must not clear the delay source (the document has no freeze_cfg)")
	}
}

// Per-item failures land in the report and never fail the document: the
// rest of it still applies (spec §3). These syncs carry an etag, as every
// materialized document does -- a body with neither an etag nor rules is the
// no-document register answer, which apply ignores
// (TestApply_NoDocumentAppliesNothing).

func TestApply_ActiveFault_InvalidDelay(t *testing.T) {
	demo := &demoEvaluator{}
	sync := wire.RuleSync{Etag: "1-a", ActiveFaults: []wire.FaultRequest{
		{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)},
		{ID: "slot-b", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)},
	}}
	report, err := apply(sync, applyTargets{DemoEval: demo}, scopeAll)
	if err != nil {
		t.Fatalf("a bad slot must not fail the sync: %v", err)
	}
	if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "active_fault slot-a: ") || !strings.Contains(report.Errors[0], "bogus") {
		t.Fatalf("errors = %v", report.Errors)
	}
	if ids := demo.ActiveIDs(); len(ids) != 1 || ids[0] != "slot-b" {
		t.Fatalf("active slots = %v, want only slot-b: the bad slot is not armed and the next one still is", ids)
	}
}

func TestApply_ActiveFault_UnknownType(t *testing.T) {
	demo := &demoEvaluator{}
	sync := wire.RuleSync{Etag: "1-a", ActiveFaults: []wire.FaultRequest{{FaultType: "quantum"}}}
	report, err := apply(sync, applyTargets{DemoEval: demo}, scopeAll)
	if err != nil {
		t.Fatalf("an unknown fault type must not fail the sync: %v", err)
	}
	// No ID: the slot is keyed by its effective category.
	if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "active_fault inline: ") || !strings.Contains(report.Errors[0], "quantum") {
		t.Fatalf("errors = %v", report.Errors)
	}
	if len(demo.ActiveIDs()) != 0 {
		t.Fatalf("an unknown fault type must not be armed: %v", demo.ActiveIDs())
	}
}

func TestApply_FreezeCfg_NegativeMu(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	t.Cleanup(cb.Stop)
	sync := wire.RuleSync{Etag: "1-a", FreezeCfg: &wire.DelayRequest{Mu: -1}}
	report, err := apply(sync, applyTargets{CacheBox: cb}, scopeAll)
	if err != nil {
		t.Fatalf("a bad freeze config must not fail the sync: %v", err)
	}
	if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "freeze_cfg: ") || !strings.Contains(report.Errors[0], "mu") {
		t.Fatalf("errors = %v", report.Errors)
	}
	if d := cb.Stats().Delay; d.Installed {
		t.Fatalf("a rejected freeze config must install nothing, got %+v", d)
	}
}

func TestApply_ActiveFault_CPUStress(t *testing.T) {
	demo := &demoEvaluator{}
	resp := wire.RegisterResponse{
		RuleSync: wire.RuleSync{Etag: "1-a", ActiveFaults: []wire.FaultRequest{{
			Category:   "resource",
			FaultType:  "cpu",
			DurationMs: 5000,
			Params:     json.RawMessage(`{"target_load":0.7}`),
		}}},
	}
	if _, err := apply(resp.RuleSync, applyTargets{DemoEval: demo}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(demo.Active()) == 0 {
		t.Fatal("expected active fault")
	}
}

// TestApply_ActiveFault_NetworkWithoutResolverIsSkipped: a network slot this
// host cannot run (host=proxy, no NetworkResolver) is skipped, counted and
// reported like any other unsupported slot instead of failing the whole sync.
func TestApply_ActiveFault_NetworkWithoutResolverIsSkipped(t *testing.T) {
	demo := &demoEvaluator{}
	resp := wire.RegisterResponse{
		RuleSync: wire.RuleSync{Etag: "1-a", ActiveFaults: []wire.FaultRequest{
			{
				ID: "slot-net", Category: "network", FaultType: "latency", DurationMs: 5000,
				Network: &wire.NetworkEnvelope{Target: "redis"},
				Params:  json.RawMessage(`{"delay":"100ms"}`),
			},
			{ID: "slot-lat", Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)},
		}},
	}
	before := unsupportedCount(t, "network")
	report, err := apply(resp.RuleSync, applyTargets{DemoEval: demo}, scopeAll)
	if err != nil {
		t.Fatalf("apply must not fail on a network slot the host cannot run: %v", err)
	}
	if ids := demo.ActiveIDs(); len(ids) != 1 || ids[0] != "slot-lat" {
		t.Fatalf("active slots = %v, want only the supported one", ids)
	}
	if want := []wire.UnsupportedRule{{Rule: "slot-net", Category: "network"}}; !reflect.DeepEqual(report.Unsupported, want) || len(report.Errors) != 0 {
		t.Fatalf("report = %+v, want the slot reported unsupported under network and no errors", report)
	}
	if got := unsupportedCount(t, "network") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{network} += %v, want 1", got)
	}
}

func TestRegisterAndApply_E2E(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(wire.RegisterResponse{
			Status: "registered",
			RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Labels:         map[string]string{"target": "productcatalog"},
				Mode:           "inline",
				Priority:       10,
				Fault: &wire.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"50ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	eval := evaluator.NewStaticEvaluator()

	resp, err := registerWith(context.Background(), http.DefaultClient, server.URL, wire.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := apply(resp.RuleSync, applyTargets{Evaluator: eval}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rules := eval.Rules()
	if len(rules) != 1 {
		t.Fatalf("rules after apply = %d, want 1", len(rules))
	}
	if rules[0].Name != "freeze-productcatalog" {
		t.Errorf("rule name = %q", rules[0].Name)
	}
	if rules[0].Decision.Fault == nil {
		t.Error("expected Decision.Fault to be set")
	}
}

func TestApply_FreezeCfg_SetsDistributionDelay(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()

	resp := wire.RegisterResponse{
		RuleSync: wire.RuleSync{Etag: "1-a", FreezeCfg: &wire.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42}},
	}
	if _, err := apply(resp.RuleSync, applyTargets{CacheBox: cb}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	entry := &cachebox.Entry{
		Key:             "test-key",
		StatusCode:      200,
		Body:            []byte("ok"),
		ObservedLatency: 5 * time.Millisecond,
	}

	delay := cb.SampleDelay(entry)
	if delay <= 0 {
		t.Errorf("expected positive delay from fitted distribution, got %v", delay)
	}
	if delay == entry.ObservedLatency {
		t.Error("expected fitted delay to differ from observed (sigma > 0)")
	}
}

// TestApply_EmptyRulesClears pins the reconciliation contract: a non-nil
// EMPTY rules list is authoritative desired state, not "no change". Phase
// teardown clears rules via a push fanout that can miss instances; the
// poll path must converge a missed instance to zero rules or it keeps
// replaying/faulting forever. (A nil list stays a no-op -- see
// TestApply_NoRulesIsNoop.)
func TestApply_EmptyRulesClears(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "leftover", Point: evaluator.Egress})
	resp := wire.RegisterResponse{RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{}}}
	if _, err := apply(resp.RuleSync, applyTargets{Evaluator: eval}, scopeAll); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rules := eval.Rules(); len(rules) != 0 {
		t.Fatalf("empty rule set must clear the evaluator, still have %+v", rules)
	}
}

// TestApply_EmptyRulesFiresDrainTracker pins the drain fast path for the
// canonical topology: a baseline-recorded service usually has NO rules
// besides the synthesized recording rule, so the rule update that ends the
// phase arrives as an empty set -- and must still trigger the flush + W3
// drain report.
func TestApply_EmptyRulesFiresDrainTracker(t *testing.T) {
	var drains atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			drains.Add(1)
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	pusher := newCachePushClient(cachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
	})
	defer pusher.Stop()
	cb := cachebox.New(cachebox.Config{Push: pusher.PushFunc()})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())

	eval := evaluator.NewStaticEvaluator()
	tracker := newCacheDrainTracker(cb, pusher, nil)
	targets := applyTargets{Evaluator: eval, CacheDrain: tracker}

	recording := wire.RegisterResponse{RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{{
		Name: "cachebox:passthrough:phase-1", InjectionPoint: "egress", Mode: "inline",
		CacheBox: &wire.CompiledCacheBox{
			Mode: "passthrough",
			Context: &wire.CacheBoxContext{
				ExperimentID: "exp-1", PhaseID: "phase-1",
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}}}}
	if _, err := apply(recording.RuleSync, targets, scopeAll); err != nil {
		t.Fatalf("Apply recording rules: %v", err)
	}
	if got := drains.Load(); got != 0 {
		t.Fatalf("no phase ended yet, but %d drain reports sent", got)
	}

	// Drain start: the recording rule drops out and, this being the only
	// rule for the service, the authoritative set is EMPTY.
	empty := wire.RegisterResponse{RuleSync: wire.RuleSync{Rules: []wire.CompiledRule{}}}
	if _, err := apply(empty.RuleSync, targets, scopeAll); err != nil {
		t.Fatalf("Apply empty rules: %v", err)
	}
	if got := drains.Load(); got != 1 {
		t.Fatalf("empty authoritative set must fire the drain report, got %d", got)
	}
}

func TestApply_ReportsUnsupportedRulesAndDecodeErrors(t *testing.T) {
	targets := applyTargets{Evaluator: newTestEvaluator(), DecodeOpts: []decodeOption{WithCapabilities(CapInline, CapCacheBox)}}
	rules := append(threeRules(), wire.CompiledRule{Name: "comp", Mode: "inline", Composition: &wire.CompiledComposition{Name: "c", ExecutionMode: "parallel"}},
		wire.CompiledRule{Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}})
	report, err := apply(wire.RuleSync{Rules: rules}, targets, scopeAll)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(targets.Evaluator.Rules()); got != 2 {
		t.Fatalf("applied %d rules, want 2 (lat, cb)", got)
	}
	want := []wire.UnsupportedRule{{Rule: "cpu", Category: "resource"}, {Rule: "comp", Category: "composition"}}
	if !reflect.DeepEqual(report.Unsupported, want) {
		t.Fatalf("unsupported = %+v, want %+v", report.Unsupported, want)
	}
	if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "rule bad: decode: ") {
		t.Fatalf("errors = %v", report.Errors)
	}
}

// TestApply_CleanDocumentReportsEmptyLists: the report's lists are never nil,
// so the status report built from them marshals [] (an empty list clears the
// previous report, spec §2).
func TestApply_CleanDocumentReportsEmptyLists(t *testing.T) {
	report, err := apply(wire.RuleSync{Rules: threeRules()[:1]}, applyTargets{Evaluator: newTestEvaluator()}, scopeAll)
	if err != nil {
		t.Fatal(err)
	}
	if report.Unsupported == nil || report.Errors == nil || len(report.Unsupported) != 0 || len(report.Errors) != 0 {
		t.Fatalf("report = %#v, want two empty non-nil lists", report)
	}
}

func TestApply_FreezeCfgContextScopesDelay_AbsentClears(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	sync := wire.RuleSync{Etag: "1-a", FreezeCfg: &wire.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42,
		Context: &wire.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2}}}
	if _, err := apply(sync, applyTargets{CacheBox: cb}, scopeAll); err != nil {
		t.Fatal(err)
	}
	if d := cb.Stats().Delay; !d.Installed || d.ExperimentID != "exp-1" || d.PhaseID != "phase-1" {
		t.Fatalf("Stats().Delay = %+v", d)
	}
	entry := &cachebox.Entry{ObservedLatency: 5 * time.Millisecond}
	if got := cb.SampleDelayFor(&wire.CacheBoxContext{ExperimentID: "exp-9", PhaseID: "phase-9"}, entry); got != 5*time.Millisecond {
		t.Fatalf("other phase must get the observed latency, got %v", got)
	}
	// A 200 without freeze_cfg means none desired.
	if _, err := apply(wire.RuleSync{Rules: []wire.CompiledRule{}}, applyTargets{CacheBox: cb}, scopeAll); err != nil {
		t.Fatal(err)
	}
	if d := cb.Stats().Delay; d.Installed {
		t.Fatalf("absent freeze_cfg must clear the delay source, got %+v", d)
	}
}

// TestApply_FreezeCfgPartialContextIsRejected: a context naming only one of
// the pair scopes nothing. Installing it unscoped would delay every replay,
// so it is reported instead.
func TestApply_FreezeCfgPartialContextIsRejected(t *testing.T) {
	for _, ctx := range []*wire.CacheBoxContext{{ExperimentID: "exp-1"}, {PhaseID: "phase-1"}} {
		cb := cachebox.New(cachebox.Config{})
		t.Cleanup(cb.Stop)
		sync := wire.RuleSync{Etag: "1-a", FreezeCfg: &wire.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42, Context: ctx}}
		report, err := apply(sync, applyTargets{CacheBox: cb}, scopeAll)
		if err != nil {
			t.Fatalf("a partial freeze context must not fail the sync: %v", err)
		}
		if want := []string{"freeze_cfg: context needs both experiment_id and phase_id"}; !reflect.DeepEqual(report.Errors, want) {
			t.Fatalf("context %+v: errors = %v, want %v", ctx, report.Errors, want)
		}
		if d := cb.Stats().Delay; d.Installed {
			t.Fatalf("context %+v: a partial context must install no source, got %+v", ctx, d)
		}
	}
}

func TestApply_ReplaySet_ScopedFromReplayRuleContext(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(3)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)
	targets := applyTargets{Evaluator: newTestEvaluator(), CacheBox: cb, ReplaySet: p}
	ref := refFor(f, entries) // no pair of its own: the replay rule's context scopes it
	sync := wire.RuleSync{
		Rules: []wire.CompiledRule{{Name: "cachebox:replay:phase-1", InjectionPoint: "egress", Mode: "inline",
			CacheBox: &wire.CompiledCacheBox{Mode: "replay", Context: &wire.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2}}}},
		ReplaySet: &ref,
	}
	if _, err := apply(sync, targets, scopeAll); err != nil {
		t.Fatal(err)
	}
	waitState(t, p, wire.ReplaySetCommitted)
	if cb.ReplaySetPhaseKey() != cachebox.PhaseKey("exp-1", "phase-1") {
		t.Fatalf("replay set installed under %q, want the replay rule's pair", cb.ReplaySetPhaseKey())
	}
	// Absent replay_set clears it.
	if _, err := apply(wire.RuleSync{Rules: []wire.CompiledRule{}}, targets, scopeAll); err != nil {
		t.Fatal(err)
	}
	if st, _ := p.Status(); st != nil || cb.ReplaySetLen() != 0 {
		t.Fatalf("absent replay_set must clear: status=%+v len=%d", st, cb.ReplaySetLen())
	}
}

// TestApply_ReplaySet_ScopedByTheRefsOwnPair: a ref that names its pair is
// installed under it, whatever the replay rule's or the freeze's context says.
func TestApply_ReplaySet_ScopedByTheRefsOwnPair(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(3)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)
	ref := refFor(f, entries)
	ref.ExperimentID, ref.PhaseID = "exp-2", "phase-2"
	sync := wire.RuleSync{Etag: "1-a",
		Rules: []wire.CompiledRule{{Name: "cachebox:replay:phase-1", InjectionPoint: "egress", Mode: "inline",
			CacheBox: &wire.CompiledCacheBox{Mode: "replay", Context: &wire.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1"}}}},
		FreezeCfg: &wire.DelayRequest{Mu: 1, Sigma: 0.1, Context: &wire.CacheBoxContext{ExperimentID: "exp-3", PhaseID: "phase-3"}},
		ReplaySet: &ref,
	}
	if _, err := apply(sync, applyTargets{Evaluator: newTestEvaluator(), CacheBox: cb, ReplaySet: p}, scopeAll); err != nil {
		t.Fatal(err)
	}
	waitState(t, p, wire.ReplaySetCommitted)
	if got, want := cb.ReplaySetPhaseKey(), cachebox.PhaseKey("exp-2", "phase-2"); got != want {
		t.Fatalf("replay set installed under %q, want the ref's own pair %q", got, want)
	}
}

func TestApply_ReplaySetWithoutPullerIsReported(t *testing.T) {
	sync := wire.RuleSync{Etag: "1-a", Rules: []wire.CompiledRule{},
		ReplaySet: &wire.ReplaySetRef{ID: "sha256:abc", ExperimentID: "exp-1", PhaseID: "phase-1"}}
	report, err := apply(sync, applyTargets{Evaluator: newTestEvaluator()}, scopeAll)
	if err != nil {
		t.Fatalf("a replay set this host cannot pull must not fail the sync: %v", err)
	}
	if want := []string{"replay_set sha256:abc: this host has no replay-set puller"}; !reflect.DeepEqual(report.Errors, want) {
		t.Fatalf("errors = %v, want %v", report.Errors, want)
	}
}

func TestReplayScope_PrefersRefThenReplayRuleThenFreezeContext(t *testing.T) {
	ctx := func(exp, phase string) *wire.CacheBoxContext {
		return &wire.CacheBoxContext{ExperimentID: exp, PhaseID: phase, KeyStrategy: "canonical_v2", StrategyVersion: 2}
	}
	rule := func(mode string, c *wire.CacheBoxContext) wire.CompiledRule {
		return wire.CompiledRule{Name: "cachebox:" + mode, InjectionPoint: "egress", Mode: "inline",
			CacheBox: &wire.CompiledCacheBox{Mode: mode, Context: c}}
	}
	freeze := func(c *wire.CacheBoxContext) *wire.DelayRequest {
		return &wire.DelayRequest{Mu: 1, Sigma: 0.1, Context: c}
	}
	pair := func(exp, phase string) cachebox.PhasePair {
		return cachebox.PhasePair{ExperimentID: exp, PhaseID: phase}
	}

	for _, tc := range []struct {
		name string
		sync wire.RuleSync
		want cachebox.PhasePair
	}{
		{"the ref's own pair first", wire.RuleSync{
			ReplaySet: &wire.ReplaySetRef{ID: "sha256:a", ExperimentID: "exp-r", PhaseID: "phase-r"},
			Rules:     []wire.CompiledRule{rule("replay", ctx("exp-1", "phase-1"))},
			FreezeCfg: freeze(ctx("exp-f", "phase-f")),
		}, pair("exp-r", "phase-r")},
		{"a ref with half a pair falls back to the replay rule", wire.RuleSync{
			ReplaySet: &wire.ReplaySetRef{ID: "sha256:a", ExperimentID: "exp-r"},
			Rules:     []wire.CompiledRule{rule("replay", ctx("exp-1", "phase-1"))},
		}, pair("exp-1", "phase-1")},
		{"the first replay-family rule, past passthrough ones", wire.RuleSync{
			ReplaySet: &wire.ReplaySetRef{ID: "sha256:a"},
			Rules: []wire.CompiledRule{rule("passthrough", ctx("exp-p", "phase-p")),
				rule("replay_with_delay", ctx("exp-1", "phase-1")), rule("replay", ctx("exp-2", "phase-2"))},
			FreezeCfg: freeze(ctx("exp-f", "phase-f")),
		}, pair("exp-1", "phase-1")},
		{"replay rules without a full context are passed over", wire.RuleSync{
			Rules:     []wire.CompiledRule{rule("replay", nil), rule("replay", ctx("exp-1", "")), {Name: "plain", Mode: "inline"}},
			FreezeCfg: freeze(ctx("exp-f", "phase-f")),
		}, pair("exp-f", "phase-f")},
		{"passthrough-only rules fall back to the freeze context", wire.RuleSync{
			Rules:     []wire.CompiledRule{rule("passthrough", ctx("exp-p", "phase-p"))},
			FreezeCfg: freeze(ctx("exp-f", "phase-f")),
		}, pair("exp-f", "phase-f")},
		{"a partial freeze context is no pair", wire.RuleSync{FreezeCfg: freeze(ctx("exp-f", ""))}, cachebox.PhasePair{}},
		{"neither is the zero pair", wire.RuleSync{
			Rules:     []wire.CompiledRule{rule("passthrough", ctx("exp-p", "phase-p"))},
			FreezeCfg: freeze(nil),
		}, cachebox.PhasePair{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := replayScope(tc.sync); got != tc.want {
				t.Fatalf("replayScope = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestApply_NoDocumentAppliesNothing: a body with neither an etag nor rules
// is manteion's no-document register answer -- it could not materialize the
// desired state (spec §2 Register). The instance keeps everything it holds
// (rules, slots, delay source, replay set) and keeps polling.
func TestApply_NoDocumentAppliesNothing(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})
	demo := &demoEvaluator{}
	demo.Set(&evaluator.Decision{Name: "slot-a", Reason: "register"}, &wire.FaultRequest{ID: "slot-a"})
	cb := cachebox.New(cachebox.Config{})
	t.Cleanup(cb.Stop)
	cb.SetDelaySource(cachebox.NewDistributionDelaySource(1, 0.5, 1))
	cb.InstallReplaySet(cachebox.PhaseKey("exp-1", "phase-1"), map[string]*cachebox.Entry{"k": {Key: "k", StatusCode: 200}})
	p := newReplaySetPuller(replaySetPullerConfig{BaseURL: "http://manteion.invalid", CacheBox: cb, Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(p.Close)
	targets := applyTargets{Evaluator: eval, DemoEval: demo, CacheBox: cb, ReplaySet: p}

	var noDoc wire.RegisterResponse
	if err := json.Unmarshal([]byte(`{"status":"registered","version":0,"etag":"","rules":null,"active_faults":null}`), &noDoc); err != nil {
		t.Fatal(err)
	}
	report, err := apply(noDoc.RuleSync, targets, scopeAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unsupported) != 0 || len(report.Errors) != 0 {
		t.Fatalf("report = %+v, want empty", report)
	}
	if rules := eval.Rules(); len(rules) != 1 || rules[0].Name != "keep" {
		t.Fatalf("rules = %+v, want the held rule kept", rules)
	}
	if ids := demo.ActiveIDs(); len(ids) != 1 || ids[0] != "slot-a" {
		t.Fatalf("slots = %v, want slot-a kept", ids)
	}
	if !cb.Stats().Delay.Installed {
		t.Fatal("a no-document body must not clear the delay source")
	}
	if cb.ReplaySetLen() != 1 {
		t.Fatalf("replay set has %d entries, want the installed one kept", cb.ReplaySetLen())
	}
}
