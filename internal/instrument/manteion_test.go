package instrument

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

	dto "github.com/prometheus/client_model/go"
)

// newTestEvaluator returns a fresh StaticEvaluator with zero rules.
func newTestEvaluator() *evaluator.StaticEvaluator {
	return evaluator.NewStaticEvaluator()
}

// newTestTargets returns applyTargets with a fresh evaluator.
func newTestTargets() applyTargets {
	return applyTargets{Evaluator: newTestEvaluator()}
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
		targets:    applyTargets{Evaluator: eval},
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
	compiled := []wire.CompiledRule{{
		Name:           "test-latency",
		InjectionPoint: "ingress",
		Mode:           "inline",
		Fault: &wire.CompiledFault{
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
		targets:    applyTargets{Evaluator: eval},
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
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "rules": []wire.CompiledRule{}})
		case "/api/v1/sdk/register":
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(wire.RegisterResponse{Status: "registered"})
		}
	}))
	defer srv.Close()

	eval := newTestEvaluator()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	c := &ManteionClient{
		cfg:        manteionConfig{url: srv.URL, serviceName: "svc", pollInterval: 50 * time.Millisecond},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    applyTargets{Evaluator: eval},
		logger:     slog.New(slog.DiscardHandler),
		pollCtx:    ctx,
	}
	c.status.Store(int32(manteionConnected))

	trigger := make(chan struct{}, 1)
	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })

	// Wait until status recovers to Connected.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if manteionStatus(c.status.Load()) == manteionConnected && calls.Load() >= 3 {
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
		targets: applyTargets{Evaluator: eval},
		logger:  slog.New(slog.DiscardHandler),
	}

	// disconnected
	c.status.Store(int32(manteionDisconnected))
	h = healthFrom(c)
	if h.Status != "disconnected" {
		t.Fatalf("disconnected status = %q, want disconnected", h.Status)
	}

	// connected
	c.status.Store(int32(manteionConnected))
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
	c.status.Store(int32(manteionDegraded))
	c.lastPollAt.Store(time.Now().Add(-30 * time.Second).UnixNano())
	h = healthFrom(c)
	if h.Status != "degraded" {
		t.Fatalf("degraded status = %q, want degraded", h.Status)
	}
	if h.StaleFor == "" {
		t.Fatal("StaleFor should be non-empty for degraded with a past poll time")
	}
}

// ---------- connectManteion offline mode ----------

func TestConnectManteion_Offline_NilNil(t *testing.T) {
	c, err := connectManteion(t.Context(), manteionConfig{
		serviceName: "svc",
		offline:     true,
		targets:     newTestTargets(),
		logger:      slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("offline mode returned error: %v", err)
	}
	if c != nil {
		t.Fatal("offline mode should return nil client")
	}
	// nil-receiver safe
	if c.Status() != manteionDisconnected {
		t.Fatal("nil client Status() should be manteionDisconnected")
	}
	if err := c.Close(t.Context()); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}

func TestConnectManteion_NoEvaluator_Error(t *testing.T) {
	_, err := connectManteion(t.Context(), manteionConfig{
		serviceName: "svc",
		url:         "http://localhost:9999",
		logger:      slog.New(slog.DiscardHandler),
	})
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
		targets:    applyTargets{Evaluator: newTestEvaluator()},
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
		targets:    applyTargets{Evaluator: newTestEvaluator()},
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

// ---------- desired-state sync: conditional poll, generation, status report ----------

type pollSeen struct{ Version, IfNoneMatch, Wait string }

// fakeManteion answers the SDK endpoints the way the branch manteion does --
// init, register, deregister, the rules poll and the status report -- and
// records every register body, what every poll asked for and every status
// report it received.
type fakeManteion struct {
	srv        *httptest.Server
	mu         sync.Mutex
	registers  []wire.RegisterRequest
	polls      []pollSeen
	statuses   []wire.StatusReport
	rawStatus  [][]byte
	statusAt   []time.Time                                  // arrival time of each status report
	statusCode int                                          // response to POST /api/v1/sdk/status; 0 = 204
	failStatus int                                          // the next failStatus reports are answered 500 whatever statusCode says
	register   string                                       // 201 body; default `{"status":"registered","version":0,"rules":[],"active_faults":[]}`
	rules      func(w http.ResponseWriter, r *http.Request) // answers GET /api/v1/sdk/rules; read and written under mu (setRules)
}

func (f *fakeManteion) setRules(fn func(http.ResponseWriter, *http.Request)) {
	f.mu.Lock()
	f.rules = fn
	f.mu.Unlock()
}

func newFakeManteion(t *testing.T) *fakeManteion {
	t.Helper()
	f := &fakeManteion{register: `{"status":"registered","version":0,"rules":[],"active_faults":[]}`}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/sdk/init":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/sdk/register" && r.Method == http.MethodPost:
			var reg wire.RegisterRequest
			_ = json.NewDecoder(r.Body).Decode(&reg)
			f.mu.Lock()
			f.registers = append(f.registers, reg)
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, f.register)
		case strings.HasPrefix(r.URL.Path, "/api/v1/sdk/register/") && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/sdk/rules":
			q := r.URL.Query()
			f.mu.Lock()
			f.polls = append(f.polls, pollSeen{Version: q.Get("version"), IfNoneMatch: r.Header.Get("If-None-Match"), Wait: q.Get("wait")})
			fn := f.rules
			f.mu.Unlock()
			fn(w, r)
		case r.URL.Path == "/api/v1/sdk/status" && r.Method == http.MethodPost:
			b, _ := io.ReadAll(r.Body)
			var rep wire.StatusReport
			_ = json.Unmarshal(b, &rep)
			f.mu.Lock()
			f.statuses = append(f.statuses, rep)
			f.rawStatus = append(f.rawStatus, b)
			f.statusAt = append(f.statusAt, time.Now())
			code := f.statusCode
			if f.failStatus > 0 {
				f.failStatus--
				code = http.StatusInternalServerError
			}
			f.mu.Unlock()
			if code == 0 {
				code = http.StatusNoContent
			}
			w.WriteHeader(code)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeManteion) client(targets applyTargets) *ManteionClient {
	return &ManteionClient{
		cfg:        manteionConfig{url: f.srv.URL, serviceName: "svc", instanceID: "pod-1", pollInterval: 10 * time.Second},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		targets:    targets,
		logger:     slog.New(slog.DiscardHandler),
	}
}

// snapshot copies the polls and status reports seen so far.
func (f *fakeManteion) snapshot() (polls []pollSeen, statuses []wire.StatusReport) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pollSeen(nil), f.polls...), append([]wire.StatusReport(nil), f.statuses...)
}

// waitStatuses waits up to 5 s for at least n status reports and returns
// every report seen.
func (f *fakeManteion) waitStatuses(t *testing.T, n int) []wire.StatusReport {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, statuses := f.snapshot()
		if len(statuses) >= n {
			return statuses
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d status reports, want %d", len(statuses), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitPolls waits up to 5 s for at least n rules polls and returns every poll
// seen.
func (f *fakeManteion) waitPolls(t *testing.T, n int) []pollSeen {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		polls, _ := f.snapshot()
		if len(polls) >= n {
			return polls
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d polls, want %d: %+v", len(polls), n, polls)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// registered copies the register bodies seen so far.
func (f *fakeManteion) registered() []wire.RegisterRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wire.RegisterRequest(nil), f.registers...)
}

func respond304(gen, etag string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"`+etag+`"`)
		w.Header().Set("X-Atropos-Generation", gen)
		w.WriteHeader(http.StatusNotModified)
	}
}

func respond200(doc wire.RuleSync) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if doc.Etag != "" {
			w.Header().Set("ETag", `"`+doc.Etag+`"`)
		}
		w.Header().Set("X-Atropos-Generation", strconv.FormatUint(doc.Version, 10))
		_ = json.NewEncoder(w).Encode(doc)
	}
}

// syncGeneration reads the atropos_sync_generation gauge.
func syncGeneration(t *testing.T) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := metrics.SyncGeneration.Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestFetchRules_200AppliesAndReportsStatus(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond200(wire.RuleSync{Version: 42, Etag: "42-abc", ActiveFaults: []wire.FaultRequest{},
		Rules: append(threeRules(), wire.CompiledRule{Name: "comp", Mode: "inline", Composition: &wire.CompiledComposition{Name: "c", ExecutionMode: "parallel"}})}))
	eval := newTestEvaluator()
	c := f.client(applyTargets{Evaluator: eval, DecodeOpts: []decodeOption{WithCapabilities(CapInline, CapCacheBox)}})

	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(eval.Rules()) != 2 || c.ruleVersion.Load() != 42 || c.currentEtag() != "42-abc" || syncGeneration(t) != 42 {
		t.Fatalf("rules=%d gen=%d etag=%q gauge=%v", len(eval.Rules()), c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t))
	}
	statuses := f.waitStatuses(t, 1)
	want := wire.StatusReport{InstanceID: "pod-1", Service: "svc", AppliedGeneration: 42, Etag: "42-abc",
		Unsupported: []wire.UnsupportedRule{{Rule: "cpu", Category: "resource"}, {Rule: "comp", Category: "composition"}}, Errors: []string{}}
	if !reflect.DeepEqual(statuses[0], want) {
		t.Fatalf("status = %+v, want %+v", statuses[0], want)
	}
	if !strings.Contains(string(f.rawStatus[0]), `"errors":[]`) {
		t.Fatalf("empty errors must be sent as []: %s", f.rawStatus[0])
	}
	// The next poll is conditional on what was applied.
	f.setRules(respond304("42", "42-abc"))
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	polls, _ := f.snapshot()
	if got := polls[1]; got.Version != "42" || got.IfNoneMatch != `"42-abc"` {
		t.Fatalf("second poll = %+v, want version=42 and If-None-Match: \"42-abc\"", got)
	}
}

func TestFetchRules_304AdoptsGenerationWithoutApplying(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond304("43", "43-def"))
	eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})
	c := f.client(applyTargets{Evaluator: eval})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c.ruleVersion.Load() != 43 || c.currentEtag() != "43-def" || len(eval.Rules()) != 1 {
		t.Fatalf("gen=%d etag=%q rules=%d", c.ruleVersion.Load(), c.currentEtag(), len(eval.Rules()))
	}
	if _, statuses := f.snapshot(); len(statuses) != 0 {
		t.Fatalf("304 must not send a status report, sent %d", len(statuses))
	}
}

func TestFetchRules_200WithoutEtagAppliesWithoutConditional(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(func(w http.ResponseWriter, _ *http.Request) { // no ETag/X-Atropos-Generation headers, no etag in body
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"version":5,"rules":[],"active_faults":[]}`)
	})
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	for i := 0; i < 2; i++ {
		if err := c.fetchRules(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	polls, _ := f.snapshot()
	if c.ruleVersion.Load() != 5 || polls[1].Version != "5" || polls[1].IfNoneMatch != "" {
		t.Fatalf("gen=%d second poll=%+v, want version=5 and no If-None-Match", c.ruleVersion.Load(), polls[1])
	}
}

func TestRegister_AdoptsStateAndReportsStatus(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":7,"etag":"7-xyz","rules":[{"name":"lat","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"latency","params":{"delay":"1ms"}}}],"active_faults":[]}`
	eval := newTestEvaluator()
	c := f.client(applyTargets{Evaluator: eval})
	if err := c.register(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := f.waitStatuses(t, 1)[0]
	if len(eval.Rules()) != 1 || c.ruleVersion.Load() != 7 || s.AppliedGeneration != 7 || s.Etag != "7-xyz" {
		t.Fatalf("rules=%d gen=%d status=%+v", len(eval.Rules()), c.ruleVersion.Load(), s)
	}
}

func TestSendStatus_MarksPendingOnFailure(t *testing.T) {
	f := newFakeManteion(t)
	f.statusCode = http.StatusInternalServerError
	f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}))
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatalf("a failed status report must not fail the poll: %v", err)
	}
	if !c.statusIsPending() {
		t.Fatal("failed status must be marked pending for resend")
	}
	f.mu.Lock()
	f.statusCode = 0
	f.mu.Unlock()
	c.sendStatus(t.Context())
	if c.statusIsPending() || len(f.waitStatuses(t, 2)) != 2 {
		t.Fatal("successful resend must clear pending")
	}
}

func TestApplyDesired_HardErrorIsReportedNotAdvanced(t *testing.T) {
	f := newFakeManteion(t)
	// rules present but no Evaluator target → hard error
	f.setRules(respond200(wire.RuleSync{Version: 9, Etag: "9-a", Rules: threeRules()[:1], ActiveFaults: []wire.FaultRequest{}}))
	c := f.client(applyTargets{})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	s := f.waitStatuses(t, 1)[0]
	if c.ruleVersion.Load() != 0 || s.AppliedGeneration != 0 || len(s.Errors) != 1 || !strings.Contains(s.Errors[0], "apply rules") {
		t.Fatalf("gen=%d status=%+v", c.ruleVersion.Load(), s)
	}
}

// TestFetchRulesWait_SendsWaitOnlyWhenPositive: wait is the long-poll hold in
// whole seconds (rounded up, so a positive wait never asks for none); a zero
// wait sends no wait parameter at all.
func TestFetchRulesWait_SendsWaitOnlyWhenPositive(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond304("1", "1-a"))
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	for _, wait := range []time.Duration{0, 2 * time.Second, 1500 * time.Millisecond} {
		if err := c.fetchRulesWait(t.Context(), wait); err != nil {
			t.Fatal(err)
		}
	}
	polls, _ := f.snapshot()
	if got := []string{polls[0].Wait, polls[1].Wait, polls[2].Wait}; !reflect.DeepEqual(got, []string{"", "2", "2"}) {
		t.Fatalf("wait parameters = %q, want none, 2 and 2", got)
	}
}

// TestRegister_NoDocumentKeepsWhatTheInstanceHolds: a register answer with
// neither an etag nor rules is manteion's no-document body -- it could not
// materialize the desired state. The instance applies nothing and keeps its
// generation, ETag and report (the next poll stays conditional on what it
// holds), keeps its slots confirmed, and reports the state it still holds.
func TestRegister_NoDocumentKeepsWhatTheInstanceHolds(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":0,"etag":"","rules":null,"active_faults":null}`
	f.setRules(respond200(wire.RuleSync{Version: 7, Etag: "7-xyz", Rules: threeRules(),
		ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}}))
	eval := newTestEvaluator()
	demo := &demoEvaluator{}
	c := f.client(applyTargets{Evaluator: eval, DemoEval: demo, DecodeOpts: []decodeOption{WithCapabilities(CapInline, CapCacheBox)}})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	ageSlots(demo, time.Hour) // the held slot is older than any grace

	if err := c.register(t.Context()); err != nil {
		t.Fatal(err)
	}
	statuses := f.waitStatuses(t, 2)
	if !reflect.DeepEqual(statuses[1], statuses[0]) || statuses[1].AppliedGeneration != 7 || statuses[1].Etag != "7-xyz" {
		t.Fatalf("status after the no-document register = %+v, want the held state %+v", statuses[1], statuses[0])
	}
	if c.ruleVersion.Load() != 7 || c.currentEtag() != "7-xyz" || syncGeneration(t) != 7 || len(eval.Rules()) != 2 {
		t.Fatalf("gen=%d etag=%q gauge=%v rules=%d, want everything held kept",
			c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t), len(eval.Rules()))
	}
	if stale := demo.StaleSlots(30 * time.Second); len(stale) != 0 {
		t.Fatalf("stale slots %v: the held slots stay confirmed while manteion answers", stale)
	}
}

// TestFetchRules_304KeepsAppliedSlotsAlivePastTheGrace: apply confirms the
// slots it applies only on a 200; a 304 says the document is unchanged, so it
// re-confirms the slots the last apply confirmed. Otherwise a steady state of
// 304s would let the watchdog reap desired slots. A slot the document never
// named (armed through /admin/fault) is not confirmed and is reaped.
func TestFetchRules_304KeepsAppliedSlotsAlivePastTheGrace(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{},
		ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}}))
	demo := &demoEvaluator{}
	c := f.client(applyTargets{Evaluator: newTestEvaluator(), DemoEval: demo})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	demo.Set(&evaluator.Decision{Name: "admin"}, &wire.FaultRequest{ID: "admin"})
	ageSlots(demo, time.Hour) // both slots are now older than any grace

	f.setRules(respond304("1", "1-a"))
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	reapStaleSlots(demo, nil, nil, 30*time.Second, nil)
	if ids := demo.ActiveIDs(); !reflect.DeepEqual(ids, []string{"slot-a"}) {
		t.Fatalf("slots after a reap = %v, want slot-a kept alive by the 304 and the admin slot reaped", ids)
	}
}

// TestBuildStatus_IsAFullSnapshot: both lists are always present ([] when
// empty, which clears manteion's previous report), the replay set is the one
// the puller holds, and a failed fetch's reason joins the errors.
func TestBuildStatus_IsAFullSnapshot(t *testing.T) {
	f := newFakeManteion(t)
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	raw, err := json.Marshal(c.buildStatus()) // nothing applied yet, no puller
	if err != nil {
		t.Fatal(err)
	}
	if s := string(raw); !strings.Contains(s, `"unsupported":[]`) || !strings.Contains(s, `"errors":[]`) || strings.Contains(s, `"replay_set"`) {
		t.Fatalf("status = %s, want both lists empty and no replay_set", s)
	}

	c.puller = newReplaySetPuller(replaySetPullerConfig{BaseURL: f.srv.URL, CacheBox: newTestCacheBox(t), Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(c.puller.Close)
	c.lastReport = applyReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{"rule bad: decode: x"}}
	c.puller.Reconcile(&wire.ReplaySetRef{ID: "sha256:abc"}, cachebox.PhasePair{}) // no pair scopes it: the fetch fails
	_, msg := waitState(t, c.puller, wire.ReplaySetFailed)
	s := c.buildStatus()
	if want := (&wire.ReplaySetStatus{ID: "sha256:abc", State: wire.ReplaySetFailed}); !reflect.DeepEqual(s.ReplaySet, want) {
		t.Fatalf("replay_set = %+v, want %+v", s.ReplaySet, want)
	}
	if want := []string{"rule bad: decode: x", msg}; msg == "" || !reflect.DeepEqual(s.Errors, want) {
		t.Fatalf("errors = %q, want %q", s.Errors, want)
	}
}

// TestConnectManteion_WiresTheReplaySetPuller: with a cache box the client
// builds the replay-set puller and hands it to apply before registering, so
// the replay set the register answer names is fetched -- through newReq,
// with the client's auth -- and its commit reaches manteion in a status
// report (OnChange → sendStatus).
func TestConnectManteion_WiresTheReplaySetPuller(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(3)
	art := newFakeArtifact(t, entries)
	ref := refFor(art, entries)
	ref.URL = art.srv.URL + ref.URL // absolute: the artifact server is not manteion's
	ref.ExperimentID, ref.PhaseID = "exp-1", "phase-1"

	f := newFakeManteion(t)
	reg, err := json.Marshal(wire.RegisterResponse{Status: "registered", RuleSync: wire.RuleSync{
		Version: 3, Etag: "3-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}, ReplaySet: &ref}})
	if err != nil {
		t.Fatal(err)
	}
	f.register = string(reg)
	f.setRules(respond304("3", "3-a"))

	var authMu sync.Mutex
	var authed []string
	cb := newTestCacheBox(t)
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator(), CacheBox: cb},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
		authFn: func(r *http.Request) error {
			authMu.Lock()
			authed = append(authed, r.URL.Path)
			authMu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if c.puller == nil || c.targets.ReplaySet != c.puller {
		t.Fatal("a client with a cache box must build the puller and hand it to apply")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, statuses := f.snapshot()
		if n := len(statuses); n > 0 {
			if rs := statuses[n-1].ReplaySet; rs != nil && rs.ID == ref.ID && rs.State == wire.ReplaySetCommitted {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no status report with the replay set committed; got %+v", statuses)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, want := cb.ReplaySetPhaseKey(), cachebox.PhaseKey("exp-1", "phase-1"); got != want {
		t.Fatalf("replay set installed under %q, want %q", got, want)
	}
	authMu.Lock()
	defer authMu.Unlock()
	if !slices.Contains(authed, "/api/v1/cache/artifacts/x") {
		t.Fatalf("artifact requests must be built through newReq (auth); auth saw %v", authed)
	}
}

// TestClose_CancelsThenClosesThePullerThenWaits: Close cancels the poll
// context, closes the replay-set puller (ending its fetch and the status
// reports it would send), and only then waits for the client's goroutines --
// one still waiting on the puller must not hang shutdown.
func TestClose_CancelsThenClosesThePullerThenWaits(t *testing.T) {
	f := newFakeManteion(t)
	cb := newTestCacheBox(t)
	c := f.client(applyTargets{Evaluator: newTestEvaluator(), CacheBox: cb})
	c.puller = newReplaySetPuller(replaySetPullerConfig{BaseURL: f.srv.URL, CacheBox: cb, Logger: slog.New(slog.DiscardHandler)})
	pollCtx, cancel := context.WithCancel(context.Background())
	c.pollCtx, c.cancel = pollCtx, cancel

	cancelledFirst := make(chan bool, 1)
	c.wg.Go(func() {
		<-c.puller.ctx.Done() // ends only when the puller is closed
		cancelledFirst <- pollCtx.Err() != nil
	})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = c.Close(context.Background())
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close must close the puller before waiting for the client's goroutines")
	}
	if !<-cancelledFirst {
		t.Fatal("Close must cancel the poll context before closing the puller")
	}
}

// ---------- long-poll loop ----------

func TestPollLoop_LongPollRepollsImmediately(t *testing.T) {
	pollFloor = 0
	t.Cleanup(func() { pollFloor = time.Second })
	f := newFakeManteion(t)
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // the server "holds" the request
		respond304("3", "3-c")(w, r)
	})
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	c.cfg.pollWait = 2 * time.Second
	c.pollClient = &http.Client{Timeout: 17 * time.Second}
	ctx, cancel := context.WithCancel(t.Context())
	c.pollCtx = ctx
	trigger := make(chan struct{}, 1)
	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
	time.Sleep(650 * time.Millisecond)
	cancel()
	c.wg.Wait()
	polls, _ := f.snapshot()
	if len(polls) < 4 {
		t.Fatalf("got %d polls in 650ms with a 100ms hold and pollInterval=10s; the loop must re-poll immediately", len(polls))
	}
	if polls[0].Wait != "2" {
		t.Fatalf("wait query = %q, want 2 (seconds)", polls[0].Wait)
	}
	if c.ruleVersion.Load() != 3 {
		t.Fatalf("generation = %d, want 3 adopted from the 304 headers", c.ruleVersion.Load())
	}
}

func TestPollLoop_SSETriggerPollsImmediately(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond304("1", "1-a"))
	c := f.client(applyTargets{Evaluator: newTestEvaluator()}) // pollWait 0: ticker semantics, 10s interval
	ctx, cancel := context.WithCancel(t.Context())
	c.pollCtx = ctx
	trigger := make(chan struct{}, 1)
	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
	time.Sleep(50 * time.Millisecond)
	trigger <- struct{}{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p, _ := f.snapshot(); len(p) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	c.wg.Wait()
	if p, _ := f.snapshot(); len(p) < 1 {
		t.Fatal("SSE trigger must cause an immediate poll")
	}
}

func TestPollLoop_ResendsPendingStatus(t *testing.T) {
	f := newFakeManteion(t)
	f.statusCode = http.StatusInternalServerError
	var n atomic.Int32
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}})(w, r)
			return
		}
		respond304("1", "1-a")(w, r)
	})
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	c.cfg.pollInterval = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	c.pollCtx = ctx
	trigger := make(chan struct{}, 1)
	trigger <- struct{}{}
	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
	f.waitStatuses(t, 1) // first attempt, rejected
	f.mu.Lock()
	f.statusCode = 0
	f.mu.Unlock()
	statuses := f.waitStatuses(t, 2) // resent before a later poll
	cancel()
	c.wg.Wait()
	if statuses[1].AppliedGeneration != 1 {
		t.Fatalf("resent status = %+v", statuses[1])
	}
}

// startLoop runs c's poll loop on a context of its own until stop (also
// registered as a cleanup) cancels it; stop waits for the loop and for any
// register it started.
func startLoop(t *testing.T, c *ManteionClient, trigger chan struct{}) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	c.pollCtx = ctx
	c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); c.wg.Wait() }) }
	t.Cleanup(stop)
	return stop
}

// TestPollLoop_TickerFirstPollWaitsAnInterval: without a hold the loop keeps
// the ticker cadence -- connectManteion's startup fetch has just run, so the
// loop's first poll comes one pollInterval later, not at once.
func TestPollLoop_TickerFirstPollWaitsAnInterval(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond304("1", "1-a"))
	c := f.client(applyTargets{Evaluator: newTestEvaluator()}) // pollWait 0, pollInterval 10s
	stop := startLoop(t, c, make(chan struct{}, 1))
	time.Sleep(300 * time.Millisecond)
	stop()
	if polls, _ := f.snapshot(); len(polls) != 0 {
		t.Fatalf("got %d polls in the first 300ms with pollWait=0 and pollInterval=10s; the first poll waits an interval", len(polls))
	}
}

// TestPollLoop_FloorPacesAServerThatDoesNotHold: a server that answers a
// wait poll at once (one that ignores wait) is re-polled at pollFloor's pace,
// not in a hot loop and not at pollInterval's.
func TestPollLoop_FloorPacesAServerThatDoesNotHold(t *testing.T) {
	pollFloor = 200 * time.Millisecond
	t.Cleanup(func() { pollFloor = time.Second })
	f := newFakeManteion(t)
	f.setRules(respond304("1", "1-a")) // answers a wait poll at once
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	c.cfg.pollWait = 2 * time.Second // pollInterval stays 10s
	c.pollClient = &http.Client{Timeout: 17 * time.Second}
	stop := startLoop(t, c, make(chan struct{}, 1))
	time.Sleep(700 * time.Millisecond)
	stop()
	if polls, _ := f.snapshot(); len(polls) < 2 || len(polls) > 5 {
		t.Fatalf("got %d polls in 700ms with a 200ms floor, want 2 to 5 (one every 200ms)", len(polls))
	}
}

// holdingRules answers the rules poll the way manteion's long-poll does: a
// poll that asks for a wait and whose If-None-Match names the current
// document is held until the document changes (bump) or the wait elapses;
// then a matching If-None-Match gets a 304, anything else the document.
type holdingRules struct {
	mu      sync.Mutex
	doc     wire.RuleSync
	changed chan struct{} // closed by the next bump
}

func newHoldingRules(doc wire.RuleSync) *holdingRules {
	return &holdingRules{doc: doc, changed: make(chan struct{})}
}

func (h *holdingRules) current() (wire.RuleSync, chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.doc, h.changed
}

func (h *holdingRules) bump(doc wire.RuleSync) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.doc = doc
	close(h.changed)
	h.changed = make(chan struct{})
}

func (h *holdingRules) serve(w http.ResponseWriter, r *http.Request) {
	doc, changed := h.current()
	inm := r.Header.Get("If-None-Match")
	if wait, _ := strconv.Atoi(r.URL.Query().Get("wait")); wait > 0 && inm == `"`+doc.Etag+`"` {
		select {
		case <-changed:
		case <-time.After(time.Duration(wait) * time.Second):
		case <-r.Context().Done():
			return
		}
		doc, _ = h.current()
	}
	if inm == `"`+doc.Etag+`"` {
		respond304(strconv.FormatUint(doc.Version, 10), doc.Etag)(w, r)
		return
	}
	respond200(doc)(w, r)
}

// TestPollLoop_HeldPollAnswersOnABump: against a manteion that holds a poll
// until the desired state changes, the loop sits in one held poll, applies
// the bump the moment the hold ends and goes straight back into a poll held
// on the new document. Cancelling ends a held poll at once.
func TestPollLoop_HeldPollAnswersOnABump(t *testing.T) {
	pollFloor = 0
	t.Cleanup(func() { pollFloor = time.Second })
	f := newFakeManteion(t)
	h := newHoldingRules(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}})
	f.setRules(h.serve)
	eval := newTestEvaluator()
	c := f.client(applyTargets{Evaluator: eval}) // pollInterval 10s
	c.cfg.pollWait = 5 * time.Second
	c.pollClient = &http.Client{Timeout: 20 * time.Second}
	if err := c.fetchRulesWait(t.Context(), 0); err != nil { // the startup fetch
		t.Fatal(err)
	}
	stop := startLoop(t, c, make(chan struct{}, 1))

	f.waitPolls(t, 2)
	time.Sleep(200 * time.Millisecond)
	polls, _ := f.snapshot()
	want := []pollSeen{{Version: "0"}, {Version: "1", IfNoneMatch: `"1-a"`, Wait: "5"}}
	if !reflect.DeepEqual(polls, want) {
		t.Fatalf("polls = %+v, want the startup fetch, then one poll held on 1-a", polls)
	}

	h.bump(wire.RuleSync{Version: 2, Etag: "2-b", Rules: threeRules()[:1], ActiveFaults: []wire.FaultRequest{}})
	polls = f.waitPolls(t, 3)
	if got := polls[2]; got != (pollSeen{Version: "2", IfNoneMatch: `"2-b"`, Wait: "5"}) {
		t.Fatalf("poll after the bump = %+v, want one held on 2-b at once", got)
	}
	if len(eval.Rules()) != 1 || c.ruleVersion.Load() != 2 {
		t.Fatalf("rules=%d gen=%d, want the bumped document applied", len(eval.Rules()), c.ruleVersion.Load())
	}

	start := time.Now()
	stop()
	if el := time.Since(start); el > time.Second {
		t.Fatalf("stopping took %v; cancelling must end a held poll at once", el)
	}
	if c.Status() != manteionConnected {
		t.Fatalf("status = %v after stopping; a poll cut short by shutdown is not a failure", c.Status())
	}
}

// TestPollLoop_FailuresBackOffAndReRegisterOnce: a failed poll is retried
// after pollInterval·2^min(failures,6) -- with a long-poll hold too, never at
// the floor's pace -- and the sixth consecutive failure re-registers, once.
func TestPollLoop_FailuresBackOffAndReRegisterOnce(t *testing.T) {
	pollFloor = 0
	t.Cleanup(func() { pollFloor = time.Second })
	f := newFakeManteion(t)
	var mu sync.Mutex
	var at []time.Time
	f.setRules(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	c.cfg.pollInterval = 2 * time.Millisecond
	c.cfg.pollWait = 2 * time.Second
	c.pollClient = &http.Client{Timeout: 17 * time.Second}
	stop := startLoop(t, c, make(chan struct{}, 1))
	// Wait on the slice indexed below, not on the fake's poll count: the fake
	// counts a poll before this handler records its time.
	attempts := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(at)
	}
	for deadline := time.Now().Add(5 * time.Second); attempts() < 9 || len(f.registered()) == 0; {
		if time.Now().After(deadline) {
			t.Fatalf("after 5s: %d poll attempts recorded and %d registers, want 9 and 1", attempts(), len(f.registered()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()

	mu.Lock()
	defer mu.Unlock()
	for k := 1; k < 9; k++ {
		if gap, want := at[k].Sub(at[k-1]), 2*time.Millisecond<<min(k, 6); gap < want {
			t.Errorf("gap before poll %d = %v, want at least %v after %d failures", k+1, gap, want, k)
		}
	}
	if n := len(f.registered()); n != 1 {
		t.Fatalf("registers = %d, want exactly one, at the sixth consecutive failure", n)
	}
	if c.Status() != manteionDegraded {
		t.Fatalf("status = %v, want degraded while polls fail", c.Status())
	}
}

// TestPollLoop_RecoveryForgetsTheHeldDocument: when polls succeed again after
// failures, the loop forgets the document it holds -- generation, ETag and
// the atropos_sync_generation gauge together -- before it re-registers, so
// the next poll is unconditional and its 200 is applied in full.
func TestPollLoop_RecoveryForgetsTheHeldDocument(t *testing.T) {
	doc := wire.RuleSync{Version: 5, Etag: "5-e", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":0,"etag":"","rules":null,"active_faults":null}` // no document: the re-register keeps what the reset left
	var n atomic.Int32
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 3:
			respond304("5", "5-e")(w, r)
		default:
			respond200(doc)(w, r)
		}
	})
	// pollInterval stays 10s: the test drives every poll with the trigger.
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	if err := c.fetchRules(t.Context()); err != nil { // the instance holds generation 5
		t.Fatal(err)
	}
	trigger := make(chan struct{}, 1)
	startLoop(t, c, trigger)

	trigger <- struct{}{}
	f.waitPolls(t, 2) // 503
	trigger <- struct{}{}
	f.waitPolls(t, 3) // 304: the connection is restored
	s := f.waitStatuses(t, 2)[1]
	if s.AppliedGeneration != 0 || s.Etag != "" {
		t.Fatalf("the re-register's report = %+v, want generation 0 and no ETag: the recovery forgets both before it re-registers", s)
	}
	if c.ruleVersion.Load() != 0 || c.currentEtag() != "" || syncGeneration(t) != 0 {
		t.Fatalf("gen=%d etag=%q gauge=%v, want all three reset together", c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t))
	}

	trigger <- struct{}{}
	polls := f.waitPolls(t, 4)
	if got := polls[3]; got != (pollSeen{Version: "0"}) {
		t.Fatalf("poll after the recovery = %+v, want an unconditional poll at version 0", got)
	}
	f.waitStatuses(t, 3)
	if c.ruleVersion.Load() != 5 || c.currentEtag() != "5-e" || syncGeneration(t) != 5 {
		t.Fatalf("gen=%d etag=%q gauge=%v, want the full pull applied", c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t))
	}
}

// TestFetchRules_Late304AdoptsNothing: a 304 vouches for the document the
// poll was conditional on. When a register applied a newer document while
// the poll was out, the late 304 adopts nothing: the newer generation and
// ETag stay.
func TestFetchRules_Late304AdoptsNothing(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":2,"etag":"2-b","rules":[],"active_faults":[]}`
	f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}))
	c := f.client(applyTargets{Evaluator: newTestEvaluator()})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		if err := c.register(r.Context()); err != nil { // lands while the poll is out
			t.Error(err)
		}
		respond304("1", "1-a")(w, r)
	})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c.ruleVersion.Load() != 2 || c.currentEtag() != "2-b" || syncGeneration(t) != 2 {
		t.Fatalf("gen=%d etag=%q gauge=%v, want the register's 2 / 2-b kept", c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t))
	}
}

// backdateSlot sets one slot's last confirmation d in the past.
func backdateSlot(demo *demoEvaluator, id string, d time.Duration) {
	demo.mu.Lock()
	defer demo.mu.Unlock()
	demo.slots[id].lastConfirmedAt = time.Now().Add(-d)
}

// TestReapSlots_DroppingAConfirmedSlotForcesAFullPull: once the watchdog
// drops a slot the last apply confirmed, the instance no longer holds the
// document its ETag names. The reap forgets the slot and the ETag -- even
// while a poll is out, whose late 304 then adopts nothing -- so the next
// poll is unconditional and its 200 restores the slot. Dropping a slot the
// document never named (an /admin/fault slot) keeps the ETag.
func TestReapSlots_DroppingAConfirmedSlotForcesAFullPull(t *testing.T) {
	doc := wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{},
		ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}}
	f := newFakeManteion(t)
	f.setRules(respond200(doc))
	demo := &demoEvaluator{}
	c := f.client(applyTargets{Evaluator: newTestEvaluator(), DemoEval: demo})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}

	demo.Set(&evaluator.Decision{Name: "admin"}, &wire.FaultRequest{ID: "admin"})
	backdateSlot(demo, "admin", time.Hour)
	c.reapSlots(30 * time.Second)
	if ids := demo.ActiveIDs(); !reflect.DeepEqual(ids, []string{"slot-a"}) || c.currentEtag() != "1-a" {
		t.Fatalf("slots=%v etag=%q, want the admin slot dropped and the ETag kept", ids, c.currentEtag())
	}

	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == "" {
			respond200(doc)(w, r)
			return
		}
		backdateSlot(demo, "slot-a", time.Hour)
		c.reapSlots(30 * time.Second) // the watchdog fires while the poll is out
		respond304("1", "1-a")(w, r)
	})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ids := demo.ActiveIDs(); len(ids) != 0 || c.currentEtag() != "" {
		t.Fatalf("slots=%v etag=%q, want slot-a dropped and the ETag forgotten, the late 304 adopting nothing", ids, c.currentEtag())
	}

	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	polls, _ := f.snapshot()
	if got := polls[len(polls)-1]; got != (pollSeen{Version: "1"}) {
		t.Fatalf("poll after the reap = %+v, want an unconditional poll at version 1", got)
	}
	if ids := demo.ActiveIDs(); !reflect.DeepEqual(ids, []string{"slot-a"}) || c.currentEtag() != "1-a" {
		t.Fatalf("slots=%v etag=%q, want slot-a restored by the 200", ids, c.currentEtag())
	}
}

// TestReapSlots_WaitsForAnApplyInProgress: a watchdog pass and its
// bookkeeping (the confirmed set, the ETag) wait for an apply in progress on
// the client, so a slot that apply re-confirms is neither dropped from under
// it nor its ETag forgotten.
func TestReapSlots_WaitsForAnApplyInProgress(t *testing.T) {
	f := newFakeManteion(t)
	f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{},
		ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}}}))
	demo := &demoEvaluator{}
	c := f.client(applyTargets{Evaluator: newTestEvaluator(), DemoEval: demo})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	ageSlots(demo, time.Hour)

	c.applyMu.Lock() // an apply in progress
	reaped := make(chan struct{})
	go func() {
		defer close(reaped)
		c.reapSlots(30 * time.Second)
	}()
	select {
	case <-reaped:
		t.Fatal("the reap ran while an apply held the client's applyMu")
	case <-time.After(50 * time.Millisecond):
	}
	demo.Confirm("slot-a") // the apply re-confirms the slot
	c.applyMu.Unlock()
	<-reaped
	if ids := demo.ActiveIDs(); !reflect.DeepEqual(ids, []string{"slot-a"}) || c.currentEtag() != "1-a" {
		t.Fatalf("slots=%v etag=%q, want slot-a and its ETag kept: it was re-confirmed before the reap looked", ids, c.currentEtag())
	}
}

// TestWatchdogGrace_CoversThreePollCadences: a slot is dropped after three
// poll cadences unconfirmed, never sooner than 30 s; with a long-poll hold
// the cadence is the hold, since a held poll answers about once per hold.
func TestWatchdogGrace_CoversThreePollCadences(t *testing.T) {
	for _, tc := range []struct{ interval, wait, want time.Duration }{
		{10 * time.Second, 0, 30 * time.Second},
		{50 * time.Millisecond, 0, 30 * time.Second},
		{20 * time.Second, 0, time.Minute},
		{10 * time.Second, 30 * time.Second, 90 * time.Second},
		{time.Minute, 30 * time.Second, 3 * time.Minute},
	} {
		if got := watchdogGrace(tc.interval, tc.wait); got != tc.want {
			t.Errorf("watchdogGrace(%v, %v) = %v, want %v", tc.interval, tc.wait, got, tc.want)
		}
	}
}

// TestConnectManteion_WatchdogGraceCoversThreeHolds: a desired slot is
// re-confirmed about once per 30 s hold, so the watchdog the client starts
// gives a slot three holds: one unconfirmed for 60 s survives a pass, one
// unconfirmed for 100 s is dropped.
func TestConnectManteion_WatchdogGraceCoversThreeHolds(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":1,"etag":"1-a","rules":[],"active_faults":[]}`
	f.setRules(respond304("1", "1-a"))
	demo := &demoEvaluator{}
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second, pollWait: 30 * time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator(), DemoEval: demo},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	for id, age := range map[string]time.Duration{"slot-60s": 60 * time.Second, "slot-100s": 100 * time.Second} {
		demo.Set(&evaluator.Decision{Name: id}, &wire.FaultRequest{ID: id})
		backdateSlot(demo, id, age)
	}
	for deadline := time.Now().Add(5 * time.Second); slices.Contains(demo.ActiveIDs(), "slot-100s"); {
		if time.Now().After(deadline) {
			t.Fatal("the watchdog never dropped the slot unconfirmed for 100s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !slices.Contains(demo.ActiveIDs(), "slot-60s") {
		t.Fatal("the slot unconfirmed for 60s was dropped; the grace must cover three 30s holds")
	}
}

// TestApplyDesired_HardErrorConfirmsNothing: a document the targets cannot
// hold (a hard error) applies nothing and vouches for nothing: the slots the
// last apply confirmed are not re-confirmed, so they age out under the
// watchdog as if manteion had stopped answering.
func TestApplyDesired_HardErrorConfirmsNothing(t *testing.T) {
	slot := wire.FaultRequest{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}
	f := newFakeManteion(t)
	f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{slot}}))
	demo := &demoEvaluator{}
	c := f.client(applyTargets{Evaluator: newTestEvaluator(), DemoEval: demo})
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	ageSlots(demo, time.Hour)

	// freeze_cfg with no CacheBox target: a hard error
	f.setRules(respond200(wire.RuleSync{Version: 2, Etag: "2-b", Rules: []wire.CompiledRule{},
		ActiveFaults: []wire.FaultRequest{slot}, FreezeCfg: &wire.DelayRequest{Mu: 1}}))
	if err := c.fetchRules(t.Context()); err != nil {
		t.Fatal(err)
	}
	reapStaleSlots(demo, nil, nil, 30*time.Second, nil)
	if ids := demo.ActiveIDs(); len(ids) != 0 {
		t.Fatalf("slots = %v; a hard error must not re-confirm the held slots", ids)
	}
}

// TestRegister_AdvertisesThePollCadence: manteion counts an instance stale
// after twice its advertised poll interval and dead after five times; a held
// poll comes back about once per hold, so the instance advertises the longer
// of its poll interval and its hold.
func TestRegister_AdvertisesThePollCadence(t *testing.T) {
	for _, tc := range []struct {
		interval, wait time.Duration
		want           int64
	}{
		{10 * time.Second, 0, 10000},
		{10 * time.Second, 30 * time.Second, 30000},
		{time.Minute, 30 * time.Second, 60000},
	} {
		f := newFakeManteion(t)
		c := f.client(applyTargets{Evaluator: newTestEvaluator()})
		c.cfg.pollInterval, c.cfg.pollWait = tc.interval, tc.wait
		if err := c.register(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := f.registered()[0].PollIntervalMs; got != tc.want {
			t.Errorf("interval %v, wait %v: poll_interval_ms = %d, want %d", tc.interval, tc.wait, got, tc.want)
		}
	}
}

// TestConnectManteion_LongPollsByDefault: the default configuration
// long-polls with wait=30. The startup fetch is not held (no wait), so
// connecting never waits out a hold; the loop then goes straight into the
// held poll.
func TestConnectManteion_LongPollsByDefault(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":1,"etag":"1-a","rules":[],"active_faults":[]}`
	f.setRules(respond304("1", "1-a"))
	cfg := defaultManteionConfig("svc")
	cfg.url, cfg.instanceID, cfg.initTimeout = f.srv.URL, "pod-1", 5*time.Second
	cfg.targets = applyTargets{Evaluator: newTestEvaluator()}
	cfg.logger = slog.New(slog.DiscardHandler)
	c, err := connectManteion(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	polls := f.waitPolls(t, 2)
	want := []pollSeen{{Version: "1", IfNoneMatch: `"1-a"`}, {Version: "1", IfNoneMatch: `"1-a"`, Wait: "30"}}
	if !reflect.DeepEqual(polls[:2], want) {
		t.Fatalf("polls = %+v, want the startup fetch without a hold, then the loop's poll held for 30s", polls[:2])
	}
}

// TestConnectManteion_OnlyAHeldPollGetsTheLongTimeout: manteion answers a
// held poll after the hold and a little more (it re-materializes the
// document), so a held poll goes through the poll client, whose timeout
// outlasts the hold with room to spare, never through the short-call
// client. A poll without a hold -- the startup fetch -- keeps the short-call
// timeout: a server that sits on it cannot stall connecting past it.
func TestConnectManteion_OnlyAHeldPollGetsTheLongTimeout(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":1,"etag":"1-a","rules":[],"active_faults":[]}`
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		wait, _ := strconv.Atoi(r.URL.Query().Get("wait"))
		select { // the hold, then half a second more
		case <-time.After(time.Duration(wait)*time.Second + 500*time.Millisecond):
		case <-r.Context().Done():
			return
		}
		respond304("1", "1-a")(w, r)
	})
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second, pollWait: time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator()},
		httpClient: &http.Client{Timeout: 200 * time.Millisecond},
		logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if c.lastPollAt.Load() != 0 {
		t.Fatal("the startup fetch outlived the short-call timeout; a poll without a hold must keep it")
	}
	polls := f.waitPolls(t, 3) // the startup fetch, a held poll answered, the next held poll
	if polls[0].Wait != "" || polls[1].Wait != "1" || c.Status() != manteionConnected || c.lastPollAt.Load() == 0 {
		t.Fatalf("polls=%+v status=%v, want an unheld startup fetch, then held polls answered without a timeout", polls, c.Status())
	}
}

// TestConnectManteion_TheLoopsFirstFailureIsTheLastWord: connecting records
// the client connected before it starts the poll loop, whose first poll -- a
// held poll goes out at once -- may fail at once. That failure is then the
// last word on the status: health reads degraded until a poll succeeds, not
// connected because connecting finished after the failure.
func TestConnectManteion_TheLoopsFirstFailureIsTheLastWord(t *testing.T) {
	f := newFakeManteion(t)
	f.register = `{"status":"registered","version":1,"etag":"1-a","rules":[],"active_faults":[]}`
	f.setRules(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("wait") != "" { // the loop's polls fail; the startup fetch does not
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		respond304("1", "1-a")(w, r)
	})
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second, pollWait: 30 * time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator()},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	f.waitPolls(t, 2) // the startup fetch, then the loop's first poll, which fails
	// The loop's next poll is 20 s away (pollInterval·2), so nothing but the
	// first failure can set the status within this wait.
	for deadline := time.Now().Add(5 * time.Second); c.Status() != manteionDegraded; {
		if time.Now().After(deadline) {
			t.Fatalf("status = %v after the loop's first poll failed, want degraded", c.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------- status report retry ----------

// startStatusRetry runs c's status retry on a context of its own until stop
// (also registered as a cleanup) cancels it and waits for it.
func startStatusRetry(t *testing.T, c *ManteionClient) (stop func()) {
	t.Helper()
	c.statusRetry = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	c.wg.Go(func() { c.retryStatus(ctx) })
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); c.wg.Wait() }) }
	t.Cleanup(stop)
	return stop
}

// TestConnectManteion_RetriesALostCommittedReportWithoutAPoll: the replay
// set's committed report goes out while the loop sits in a held poll; when
// it does not reach manteion it is re-sent on its own a couple of seconds
// later -- not a whole hold later, when the loop's before-poll resend would
// carry it: manteion's convergence barrier gives an instance 30 s, as long
// as a hold.
func TestConnectManteion_RetriesALostCommittedReportWithoutAPoll(t *testing.T) {
	entries := replayEntries(3)
	art := newFakeArtifact(t, entries)
	release := art.hold(t) // the fetch completes once the loop is in its held poll
	ref := refFor(art, entries)
	ref.URL = art.srv.URL + ref.URL
	ref.ExperimentID, ref.PhaseID = "exp-1", "phase-1"
	doc := wire.RuleSync{Version: 3, Etag: "3-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}, ReplaySet: &ref}
	reg, err := json.Marshal(wire.RegisterResponse{Status: "registered", RuleSync: doc})
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeManteion(t)
	f.register = string(reg)
	f.setRules(newHoldingRules(doc).serve)
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second, pollWait: 30 * time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator(), CacheBox: newTestCacheBox(t)},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	f.waitPolls(t, 2) // the startup fetch, then the loop's poll, held
	f.mu.Lock()
	f.failStatus = 1 // the committed report is lost
	f.mu.Unlock()
	release()

	statuses := f.waitStatuses(t, 3) // within 5 s: the register's, the lost one, the resend
	polls, _ := f.snapshot()
	f.mu.Lock()
	gap := f.statusAt[2].Sub(f.statusAt[1])
	f.mu.Unlock()
	committed := &wire.ReplaySetStatus{ID: ref.ID, State: wire.ReplaySetCommitted}
	if !reflect.DeepEqual(statuses[1].ReplaySet, committed) || !reflect.DeepEqual(statuses[2], statuses[1]) {
		t.Fatalf("reports after the commit = %+v and %+v, want the committed report re-sent unchanged", statuses[1], statuses[2])
	}
	if want := (pollSeen{Version: "3", IfNoneMatch: `"3-a"`, Wait: "30"}); len(polls) != 2 || polls[1] != want {
		t.Fatalf("polls = %+v, want the startup fetch and one poll held since: the resend must not wait for a poll", polls)
	}
	if gap < statusRetryBase {
		t.Fatalf("re-sent %v after the lost report, want a backoff of at least %v first", gap, statusRetryBase)
	}
	for deadline := time.Now().Add(5 * time.Second); c.statusIsPending(); {
		if time.Now().After(deadline) {
			t.Fatal("the delivered resend left the report pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRetryStatus_BacksOffDoublingToThePollWait: while the report keeps
// failing, the retry waits statusRetryBase, doubling up to the poll wait --
// the poll interval when there is no hold -- and the first report that
// lands ends the retries.
func TestRetryStatus_BacksOffDoublingToThePollWait(t *testing.T) {
	statusRetryBase = 10 * time.Millisecond
	t.Cleanup(func() { statusRetryBase = 2 * time.Second })
	for _, tc := range []struct {
		name                   string
		pollInterval, pollWait time.Duration
	}{
		{"held polls", 10 * time.Second, 40 * time.Millisecond},
		{"no hold", 40 * time.Millisecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeManteion(t)
			f.failStatus = 8 // the first report and seven retries fail
			c := f.client(applyTargets{Evaluator: newTestEvaluator()})
			c.cfg.pollInterval, c.cfg.pollWait = tc.pollInterval, tc.pollWait
			stop := startStatusRetry(t, c)
			c.sendStatus(t.Context())
			f.waitStatuses(t, 9)
			for deadline := time.Now().Add(5 * time.Second); c.statusIsPending(); {
				if time.Now().After(deadline) {
					t.Fatal("the delivered retry left the report pending")
				}
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(160 * time.Millisecond) // four capped backoffs: no report may follow the one that landed
			stop()

			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.statusAt) != 9 {
				t.Fatalf("got %d reports, want 9: the first, seven failed retries and the one that landed", len(f.statusAt))
			}
			wants := []time.Duration{10, 20, 40, 40, 40, 40, 40, 40}
			for i, w := range wants {
				if gap := f.statusAt[i+1].Sub(f.statusAt[i]); gap < w*time.Millisecond {
					t.Errorf("retry %d came %v after the failure before it, want at least %v", i+1, gap, w*time.Millisecond)
				}
			}
			// Capped: 10+20+6·40 ms = 270 ms. Uncapped doubling: 10+20+...+1280 ms = 2.55 s.
			if total := f.statusAt[8].Sub(f.statusAt[0]); total > 1500*time.Millisecond {
				t.Errorf("eight retries took %v, want the backoff capped at 40ms", total)
			}
		})
	}
}

// TestClose_EndsTheStatusRetry: the status retry is one of the client's
// goroutines on its poll context, so Close ends it: nothing is re-sent once
// Close has returned, however long manteion keeps refusing the report.
func TestClose_EndsTheStatusRetry(t *testing.T) {
	statusRetryBase = 100 * time.Millisecond
	t.Cleanup(func() { statusRetryBase = 2 * time.Second })
	f := newFakeManteion(t)
	f.statusCode = http.StatusInternalServerError // every report is refused
	f.register = `{"status":"registered","version":1,"etag":"1-a","rules":[],"active_faults":[]}`
	f.setRules(newHoldingRules(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}).serve)
	c, err := connectManteion(t.Context(), manteionConfig{
		url: f.srv.URL, serviceName: "svc", instanceID: "pod-1",
		initTimeout: 5 * time.Second, pollInterval: 10 * time.Second, pollWait: 30 * time.Second,
		targets:    applyTargets{Evaluator: newTestEvaluator()},
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) }) // a failed wait must not leave the held poll to srv.Close
	// The register's report, the loop's resend before its held poll, the
	// first retry; the next retry is 200 ms away.
	f.waitStatuses(t, 3)
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, statuses := f.snapshot()
	time.Sleep(700 * time.Millisecond) // the next two backoffs
	if _, after := f.snapshot(); len(after) != len(statuses) {
		t.Fatalf("%d reports before Close returned, %d after: the retry outlived Close", len(statuses), len(after))
	}
}
