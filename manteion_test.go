package atropos

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newTestEvaluator returns a fresh StaticEvaluator with zero rules.
func newTestEvaluator() *StaticEvaluator {
	return NewStaticEvaluator()
}

// newTestTargets returns ApplyTargets with a fresh evaluator.
func newTestTargets() ApplyTargets {
	return ApplyTargets{Evaluator: newTestEvaluator()}
}

// ---------- waitForReady ----------

func TestManteionClient_WaitForReady_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/sdk/init" {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	c := &ManteionClient{
		cfg: manteionConfig{
			url:         srv.URL,
			initTimeout: 5 * time.Second,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.waitForReady(t.Context()); err != nil {
		t.Fatalf("waitForReady: %v", err)
	}
}

func TestManteionClient_WaitForReady_Retry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sdk/init" {
			return
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &ManteionClient{
		cfg: manteionConfig{
			url:         srv.URL,
			initTimeout: 10 * time.Second,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.waitForReady(t.Context()); err != nil {
		t.Fatalf("waitForReady: %v", err)
	}
	if calls.Load() < 3 {
		t.Fatalf("expected at least 3 calls, got %d", calls.Load())
	}
}

func TestManteionClient_WaitForReady_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := &ManteionClient{
		cfg: manteionConfig{
			url:         srv.URL,
			initTimeout: 600 * time.Millisecond,
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	}
	if err := c.waitForReady(t.Context()); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---------- fetchRules ----------

func TestManteionClient_PollLoop_304(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/sdk/rules" {
			w.WriteHeader(http.StatusNotModified)
		}
	}))
	defer srv.Close()

	eval := newTestEvaluator()
	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc"},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: eval},
		logger:     slog.New(slog.DiscardHandler),
	}

	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatalf("fetchRules: %v", err)
	}
	if len(eval.Rules()) != 0 {
		t.Fatal("304 should not modify evaluator rules")
	}
}

func TestManteionClient_PollLoop_RuleUpdate(t *testing.T) {
	compiled := []CompiledRule{{
		Name:           "test-latency",
		InjectionPoint: "ingress",
		Mode:           "inline",
		Fault: &CompiledFault{
			Category:  "inline",
			FaultType: "latency",
			Config:    json.RawMessage(`{"delay":"100ms"}`),
		},
	}}
	payload, _ := json.Marshal(map[string]any{"version": 5, "rules": compiled})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/sdk/rules" {
			w.Header().Set("Content-Type", "application/json")
			w.Write(payload)
		}
	}))
	defer srv.Close()

	eval := newTestEvaluator()
	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc"},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: eval},
		logger:     slog.New(slog.DiscardHandler),
	}

	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatalf("fetchRules: %v", err)
	}
	if c.ruleVersion.Load() != 5 {
		t.Fatalf("ruleVersion = %d, want 5", c.ruleVersion.Load())
	}
	if len(eval.Rules()) != 1 {
		t.Fatalf("evaluator has %d rules, want 1", len(eval.Rules()))
	}
}

func TestManteionClient_PollLoop_Recovery(t *testing.T) {
	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/sdk/rules":
			if calls.Add(1) <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "rules": []CompiledRule{}})
		case "/api/v1/sdk/register":
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(RegisterResponse{Status: "registered"})
		}
	}))
	defer srv.Close()

	eval := newTestEvaluator()
	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc", pollInterval: 50 * time.Millisecond},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: eval},
		logger:     slog.New(slog.DiscardHandler),
	}
	c.status.Store(int32(ManteionConnected))

	trigger := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })

	// Wait until status recovers to Connected.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ManteionStatus(c.status.Load()) == ManteionConnected && calls.Load() >= 3 {
			cancel()
			c.wg.Wait()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	c.wg.Wait()
	t.Fatalf("poll loop did not recover within deadline; calls=%d status=%d",
		calls.Load(), c.status.Load())
}

// ---------- health ----------

func TestHealth_Status_AllStates(t *testing.T) {
	// offline (nil client)
	h := healthFrom(nil)
	if h.Status != "offline" {
		t.Fatalf("offline status = %q, want offline", h.Status)
	}

	eval := newTestEvaluator()
	c := &ManteionClient{
		cfg:     manteionConfig{url: "http://manteion:8080", serviceName: "svc"},
		targets: ApplyTargets{Evaluator: eval},
		logger:  slog.New(slog.DiscardHandler),
	}

	// disconnected
	c.status.Store(int32(ManteionDisconnected))
	h = healthFrom(c)
	if h.Status != "disconnected" {
		t.Fatalf("disconnected status = %q, want disconnected", h.Status)
	}
	if Ready() { // global not set yet, uses healthFrom(nil) → offline
		// OK — Ready() is offline mode (returns true)
	}

	// connected
	c.status.Store(int32(ManteionConnected))
	c.ruleVersion.Store(3)
	c.lastPollAt.Store(time.Now().UnixNano())
	h = healthFrom(c)
	if h.Status != "connected" {
		t.Fatalf("connected status = %q, want connected", h.Status)
	}
	if h.RuleVersion != 3 {
		t.Fatalf("RuleVersion = %d, want 3", h.RuleVersion)
	}

	// degraded
	c.status.Store(int32(ManteionDegraded))
	c.lastPollAt.Store(time.Now().Add(-30 * time.Second).UnixNano())
	h = healthFrom(c)
	if h.Status != "degraded" {
		t.Fatalf("degraded status = %q, want degraded", h.Status)
	}
	if h.StaleFor == "" {
		t.Fatal("StaleFor should be non-empty for degraded with a past poll time")
	}
}

// ---------- ConnectManteion offline mode ----------

func TestConnectManteion_Offline_NilNil(t *testing.T) {
	c, err := ConnectManteion(t.Context(), "svc", WithOfflineMode(), WithApplyTargets(newTestTargets()))
	if err != nil {
		t.Fatalf("offline mode returned error: %v", err)
	}
	if c != nil {
		t.Fatal("offline mode should return nil client")
	}
	// nil-receiver safe
	if c.Status() != ManteionDisconnected {
		t.Fatal("nil client Status() should be ManteionDisconnected")
	}
	if err := c.Close(t.Context()); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}

func TestConnectManteion_NoEvaluator_Error(t *testing.T) {
	_, err := ConnectManteion(t.Context(), "svc", WithManteionURL("http://localhost:9999"))
	if err == nil {
		t.Fatal("expected error when Evaluator is nil")
	}
}
