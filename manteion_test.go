package atropos

import (
	"context"
	"encoding/json"
	"fmt"
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
			Params:    json.RawMessage(`{"delay":"100ms"}`),
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
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc", pollInterval: 50 * time.Millisecond},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: eval},
		logger:     slog.New(slog.DiscardHandler),
		pollCtx:    ctx,
	}
	c.status.Store(int32(ManteionConnected))

	trigger := make(chan struct{}, 1)
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

// ---------- fetchRules: 1 MiB body limit ----------

// TestFetchRules_BodyLimitEnforced sends a 10 MiB rules response and asserts
// that fetchRules returns an error (json.Unmarshal on a truncated body fails),
// proving the LimitReader cap fires before memory usage can grow unbounded.
func TestFetchRules_BodyLimitEnforced(t *testing.T) {
	const tenMiB = 10 << 20

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sdk/rules" {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Write a JSON prefix, then pad to 10 MiB with spaces so the body is
		// syntactically valid up to the limit but too large to pass through.
		w.Write([]byte(`{"version":1,"rules":[`))
		pad := make([]byte, tenMiB)
		for i := range pad {
			pad[i] = ' '
		}
		w.Write(pad)
		w.Write([]byte(`]}`))
	}))
	defer srv.Close()

	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc"},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: newTestEvaluator()},
		logger:     slog.New(slog.DiscardHandler),
	}

	err := c.fetchRules(t.Context())
	if err == nil {
		t.Fatal("expected error from truncated 10 MiB body, got nil")
	}
}

// ---------- SSE stream parsing ----------

// newTestSSEClient returns a ManteionClient wired to srv for SSE tests.
func newTestSSEClient(t *testing.T, srv *httptest.Server) *ManteionClient {
	t.Helper()
	return &ManteionClient{
		cfg:       manteionConfig{url: srv.URL, serviceName: "svc"},
		sseClient: &http.Client{Timeout: 5 * time.Second},
		logger:    slog.New(slog.DiscardHandler),
	}
}

// TestSSEStream_LargeLineScannerBuffer confirms the scanner can handle a
// single SSE line up to 1 MiB without returning a bufio.ErrTooLong error.
func TestSSEStream_LargeLineScannerBuffer(t *testing.T) {
	const size = 600 * 1024 // 600 KiB — under the 1 MiB cap

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// Single data: line of 600 KiB, then a blank line to dispatch, then close.
		fmt.Fprintf(w, "data: %s\n\n", string(make([]byte, size)))
	}))
	defer srv.Close()

	c := newTestSSEClient(t, srv)
	triggered := false
	err := c.sseStream(t.Context(), srv.URL+"/api/v1/sdk/events", func() { triggered = true })
	if err != nil {
		t.Fatalf("sseStream returned error on 600 KiB line: %v", err)
	}
	// No rules_changed event was sent, so the trigger should not have fired.
	if triggered {
		t.Fatal("triggerPoll fired unexpectedly")
	}
}

// TestSSEStream_EventParsing_NoSpace confirms event: without a trailing space
// (i.e. "event:rules_changed") is still recognised after TrimSpace.
func TestSSEStream_EventParsing_NoSpace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// No space after the colon — spec allows both forms.
		fmt.Fprint(w, "event:rules_changed\n\n")
	}))
	defer srv.Close()

	c := newTestSSEClient(t, srv)
	triggered := false
	c.sseStream(t.Context(), srv.URL+"/api/v1/sdk/events", func() { triggered = true })
	if !triggered {
		t.Fatal("triggerPoll not fired for event:rules_changed (no space)")
	}
}

// TestSSEStream_OnlyRulesChangedTriggers sends a heartbeat event followed by
// a rules_changed event and confirms only the latter fires triggerPoll.
func TestSSEStream_OnlyRulesChangedTriggers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: heartbeat\n\nevent: rules_changed\n\n")
	}))
	defer srv.Close()

	c := newTestSSEClient(t, srv)
	count := 0
	c.sseStream(t.Context(), srv.URL+"/api/v1/sdk/events", func() { count++ })
	if count != 1 {
		t.Fatalf("triggerPoll fired %d times, want exactly 1", count)
	}
}

// ---------- Close: shutdown-while-registering ----------

// TestClose_WaitsForRegisterGoroutine adds a goroutine to the WaitGroup
// (simulating the re-register path in pollLoopWithTrigger) and asserts that
// Close blocks on wg.Wait() until that goroutine finishes.
//
// The goroutine does not use the HTTP client — it blocks on an internal gate
// so context cancellation (triggered by Close) doesn't race it to completion
// before we can observe the blocking behaviour.
func TestClose_WaitsForRegisterGoroutine(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// deregister call from Close
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pollCtx, cancel := context.WithCancel(context.Background())

	c := &ManteionClient{
		cfg: manteionConfig{
			url:         srv.URL,
			serviceName: "svc",
			instanceID:  "test-instance",
		},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    ApplyTargets{Evaluator: newTestEvaluator()},
		logger:     slog.New(slog.DiscardHandler),
		pollCtx:    pollCtx,
		cancel:     cancel,
	}

	// gate blocks the goroutine until we signal, independent of context.
	gate := make(chan struct{})
	goroutineReached := make(chan struct{})

	c.wg.Go(func() {
		close(goroutineReached) // signal we're inside the goroutine
		<-gate                  // block until unblocked by the test
	})

	// Wait until the goroutine is running before calling Close.
	<-goroutineReached

	// Kick off Close in the background; it must block on wg.Wait().
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		c.Close(t.Context())
	}()

	// Close should be blocked on wg.Wait() — confirm it hasn't returned yet.
	select {
	case <-closeDone:
		t.Fatal("Close returned before WaitGroup goroutine finished")
	case <-time.After(100 * time.Millisecond):
	}

	// Unblock the goroutine; Close must now complete promptly.
	close(gate)
	select {
	case <-closeDone:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 2s after WaitGroup goroutine finished")
	}
}
