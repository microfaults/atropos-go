package atropos

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
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
		var req RegisterRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Service != "productcatalog" {
			t.Errorf("service = %q, want productcatalog", req.Service)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(RegisterResponse{
			Status: "registered",
			RuleSync: RuleSync{Rules: []CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Mode:           "inline",
				Priority:       10,
				Fault: &CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"200ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	resp, err := registerWith(context.Background(), http.DefaultClient, server.URL, RegisterRequest{
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

	_, err := registerWith(context.Background(), http.DefaultClient, server.URL, RegisterRequest{
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
		_ = json.NewEncoder(w).Encode(RegisterResponse{Status: "registered"})
	}))
	defer server.Close()

	custom := &http.Client{Timeout: 3 * time.Second}
	resp, err := registerWith(context.Background(), custom, server.URL, RegisterRequest{ID: "pod-1", Service: "svc", Address: "http://10.0.0.1:8080"})
	if err != nil || resp.Status != "registered" {
		t.Fatalf("registerWith: err=%v status=%q", err, resp.Status)
	}
}

func TestApply_SetsRules(t *testing.T) {
	eval := evaluator.NewStaticEvaluator()
	resp := RegisterResponse{
		RuleSync: RuleSync{Rules: []CompiledRule{{
			Name:           "r1",
			InjectionPoint: "egress",
			Mode:           "inline",
			Fault: &CompiledFault{
				Category:  "inline",
				FaultType: "latency",
				Params:    json.RawMessage(`{"delay":"100ms"}`),
			},
		}}},
	}

	if err := apply(resp, applyTargets{Evaluator: eval}); err != nil {
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

func TestApply_NoRulesIsNoop(t *testing.T) {
	eval := evaluator.NewStaticEvaluator(StaticRule{Name: "preexisting", Point: evaluator.Ingress})
	resp := RegisterResponse{Status: "registered"}
	if err := apply(resp, applyTargets{Evaluator: eval}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 || rules[0].Name != "preexisting" {
		t.Errorf("Apply clobbered existing rules when response had none: got %+v", rules)
	}
}

func TestApply_RulesWithoutEvaluatorErrors(t *testing.T) {
	resp := RegisterResponse{
		RuleSync: RuleSync{Rules: []CompiledRule{{Name: "r1", InjectionPoint: "egress", Mode: "inline"}}},
	}
	err := apply(resp, applyTargets{})
	if err == nil {
		t.Fatal("expected error: rules present but no Evaluator target")
	}
}

// Error-path coverage for applyActiveFault / applyFreezeCfg — the dispatch
// logic mirrors admin.go and cachebox_admin.go but the wrapped error prefixes
// are distinct ("apply active_fault:", "apply freeze_cfg:"), so confirm those
// surface cleanly when the inner helper rejects a payload.

func TestApply_ActiveFault_InvalidDelay(t *testing.T) {
	demo := &demoEvaluator{}
	resp := RegisterResponse{
		RuleSync: RuleSync{ActiveFaults: []FaultRequest{{FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}}},
	}
	err := apply(resp, applyTargets{DemoEval: demo})
	if err == nil {
		t.Fatal("expected error for invalid delay duration")
	}
	if !strings.Contains(err.Error(), "apply active_fault") {
		t.Errorf("error should be prefixed 'apply active_fault', got: %v", err)
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("error should echo the bad value, got: %v", err)
	}
}

func TestApply_ActiveFault_UnknownType(t *testing.T) {
	demo := &demoEvaluator{}
	resp := RegisterResponse{
		RuleSync: RuleSync{ActiveFaults: []FaultRequest{{FaultType: "quantum"}}},
	}
	err := apply(resp, applyTargets{DemoEval: demo})
	if err == nil {
		t.Fatal("expected error for unknown fault type")
	}
	if !strings.Contains(err.Error(), "quantum") {
		t.Errorf("error should echo the unknown type, got: %v", err)
	}
}

func TestApply_FreezeCfg_NegativeMu(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	resp := RegisterResponse{
		RuleSync: RuleSync{FreezeCfg: &DelayRequest{Mu: -1}},
	}
	err := apply(resp, applyTargets{CacheBox: cb})
	if err == nil {
		t.Fatal("expected error for negative mu")
	}
	if !strings.Contains(err.Error(), "apply freeze_cfg") {
		t.Errorf("error should be prefixed 'apply freeze_cfg', got: %v", err)
	}
	if !strings.Contains(err.Error(), "mu") {
		t.Errorf("error should mention mu, got: %v", err)
	}
}

func TestApply_ActiveFault_CPUStress(t *testing.T) {
	demo := &demoEvaluator{}
	resp := RegisterResponse{
		RuleSync: RuleSync{ActiveFaults: []FaultRequest{{
			Category:   "resource",
			FaultType:  "cpu",
			DurationMs: 5000,
			Params:     json.RawMessage(`{"target_load":0.7}`),
		}}},
	}
	if err := apply(resp, applyTargets{DemoEval: demo}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(demo.Active()) == 0 {
		t.Fatal("expected active fault")
	}
}

func TestApply_ActiveFault_NetworkRequiresResolver(t *testing.T) {
	demo := &demoEvaluator{}
	resp := RegisterResponse{
		RuleSync: RuleSync{ActiveFaults: []FaultRequest{{
			Category:   "network",
			FaultType:  "latency",
			DurationMs: 5000,
			Network:    &NetworkEnvelope{Target: "redis"},
			Params:     json.RawMessage(`{"delay":"100ms"}`),
		}}},
	}
	err := apply(resp, applyTargets{DemoEval: demo})
	if err == nil {
		t.Fatal("expected error: no resolver")
	}
}

func TestRegisterAndApply_E2E(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(RegisterResponse{
			Status: "registered",
			RuleSync: RuleSync{Rules: []CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Labels:         map[string]string{"target": "productcatalog"},
				Mode:           "inline",
				Priority:       10,
				Fault: &CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"50ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	eval := evaluator.NewStaticEvaluator()

	resp, err := registerWith(context.Background(), http.DefaultClient, server.URL, RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := apply(resp, applyTargets{Evaluator: eval}); err != nil {
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

	resp := RegisterResponse{
		RuleSync: RuleSync{FreezeCfg: &DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42}},
	}
	if err := apply(resp, applyTargets{CacheBox: cb}); err != nil {
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
	eval := evaluator.NewStaticEvaluator(StaticRule{Name: "leftover", Point: evaluator.Egress})
	resp := RegisterResponse{RuleSync: RuleSync{Rules: []CompiledRule{}}}
	if err := apply(resp, applyTargets{Evaluator: eval}); err != nil {
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
			_ = json.NewEncoder(w).Encode(DrainReportResponse{Accepted: true})
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

	recording := RegisterResponse{RuleSync: RuleSync{Rules: []CompiledRule{{
		Name: "cachebox:passthrough:phase-1", InjectionPoint: "egress", Mode: "inline",
		CacheBox: &CompiledCacheBox{
			Mode: "passthrough",
			Context: &CacheBoxContext{
				ExperimentID: "exp-1", PhaseID: "phase-1",
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}}}}
	if err := apply(recording, targets); err != nil {
		t.Fatalf("Apply recording rules: %v", err)
	}
	if got := drains.Load(); got != 0 {
		t.Fatalf("no phase ended yet, but %d drain reports sent", got)
	}

	// Drain start: the recording rule drops out and, this being the only
	// rule for the service, the authoritative set is EMPTY.
	empty := RegisterResponse{RuleSync: RuleSync{Rules: []CompiledRule{}}}
	if err := apply(empty, targets); err != nil {
		t.Fatalf("Apply empty rules: %v", err)
	}
	if got := drains.Load(); got != 1 {
		t.Fatalf("empty authoritative set must fire the drain report, got %d", got)
	}
}
