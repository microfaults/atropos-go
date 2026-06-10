package atropos_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	atropos "git.ucsc.edu/microfaults/atropos-go"
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
		var req atropos.RegisterRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Service != "productcatalog" {
			t.Errorf("service = %q, want productcatalog", req.Service)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(atropos.RegisterResponse{
			Status: "registered",
			RuleSync: atropos.RuleSync{Rules: []atropos.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Mode:           "inline",
				Priority:       10,
				Fault: &atropos.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"200ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	resp, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
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

	_, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
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
		_ = json.NewEncoder(w).Encode(atropos.RegisterResponse{Status: "registered"})
	}))
	defer server.Close()

	custom := &http.Client{Timeout: 3 * time.Second}
	resp, err := atropos.RegisterWithClient(context.Background(), custom, server.URL, atropos.RegisterRequest{ID: "pod-1", Service: "svc", Address: "http://10.0.0.1:8080"})
	if err != nil || resp.Status != "registered" {
		t.Fatalf("RegisterWithClient: err=%v status=%q", err, resp.Status)
	}
}

func TestApply_SetsRules(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{Rules: []atropos.CompiledRule{{
			Name:           "r1",
			InjectionPoint: "egress",
			Mode:           "inline",
			Fault: &atropos.CompiledFault{
				Category:  "inline",
				FaultType: "latency",
				Params:    json.RawMessage(`{"delay":"100ms"}`),
			},
		}}},
	}

	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
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
	eval := atropos.NewStaticEvaluator(atropos.StaticRule{Name: "preexisting", Point: atropos.Ingress})
	resp := atropos.RegisterResponse{Status: "registered"}
	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 || rules[0].Name != "preexisting" {
		t.Errorf("Apply clobbered existing rules when response had none: got %+v", rules)
	}
}

func TestApply_RulesWithoutEvaluatorErrors(t *testing.T) {
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{Rules: []atropos.CompiledRule{{Name: "r1", InjectionPoint: "egress", Mode: "inline"}}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{})
	if err == nil {
		t.Fatal("expected error: rules present but no Evaluator target")
	}
}

// Error-path coverage for applyActiveFault / applyFreezeCfg — the dispatch
// logic mirrors admin.go and cachebox_admin.go but the wrapped error prefixes
// are distinct ("apply active_fault:", "apply freeze_cfg:"), so confirm those
// surface cleanly when the inner helper rejects a payload.

func TestApply_ActiveFault_InvalidDelay(t *testing.T) {
	demo := &atropos.DemoEvaluator{}
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{ActiveFaults: []atropos.FaultRequest{{FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{DemoEval: demo})
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
	demo := &atropos.DemoEvaluator{}
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{ActiveFaults: []atropos.FaultRequest{{FaultType: "quantum"}}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{DemoEval: demo})
	if err == nil {
		t.Fatal("expected error for unknown fault type")
	}
	if !strings.Contains(err.Error(), "quantum") {
		t.Errorf("error should echo the unknown type, got: %v", err)
	}
}

func TestApply_FreezeCfg_NegativeMu(t *testing.T) {
	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{Store: atropos.NewCacheBoxMemStore(16)})
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{FreezeCfg: &atropos.DelayRequest{Mu: -1}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{CacheBox: cb})
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
	demo := &atropos.DemoEvaluator{}
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{ActiveFaults: []atropos.FaultRequest{{
			Category:   "resource",
			FaultType:  "cpu",
			DurationMs: 5000,
			Params:     json.RawMessage(`{"target_load":0.7}`),
		}}},
	}
	if err := atropos.Apply(resp, atropos.ApplyTargets{DemoEval: demo}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(demo.Active()) == 0 {
		t.Fatal("expected active fault")
	}
}

func TestApply_ActiveFault_NetworkRequiresResolver(t *testing.T) {
	demo := &atropos.DemoEvaluator{}
	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{ActiveFaults: []atropos.FaultRequest{{
			Category:   "network",
			FaultType:  "latency",
			DurationMs: 5000,
			Network:    &atropos.NetworkEnvelope{Target: "redis"},
			Params:     json.RawMessage(`{"delay":"100ms"}`),
		}}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{DemoEval: demo})
	if err == nil {
		t.Fatal("expected error: no resolver")
	}
}

func TestRegisterAndApply_E2E(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(atropos.RegisterResponse{
			Status: "registered",
			RuleSync: atropos.RuleSync{Rules: []atropos.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Labels:         map[string]string{"target": "productcatalog"},
				Mode:           "inline",
				Priority:       10,
				Fault: &atropos.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Params:    json.RawMessage(`{"delay":"50ms"}`),
				},
			}}},
		})
	}))
	defer server.Close()

	eval := atropos.NewStaticEvaluator()

	resp, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
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
	store := atropos.NewCacheBoxMemStore(100)
	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{Store: store})
	defer cb.Stop()

	resp := atropos.RegisterResponse{
		RuleSync: atropos.RuleSync{FreezeCfg: &atropos.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42}},
	}
	if err := atropos.Apply(resp, atropos.ApplyTargets{CacheBox: cb}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	entry := &atropos.CacheBoxEntry{
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
