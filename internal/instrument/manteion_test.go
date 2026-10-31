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
// records what every poll asked for and every status report it received.
type fakeManteion struct {
	srv        *httptest.Server
	mu         sync.Mutex
	polls      []pollSeen
	statuses   []wire.StatusReport
	rawStatus  [][]byte
	statusCode int                                          // response to POST /api/v1/sdk/status; 0 = 204
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
			code := f.statusCode
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
