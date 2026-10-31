package interceptor

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

func inlineLatencyDecision(name string, d time.Duration) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Reason: "test", Mode: evaluator.Inline,
		Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: d}, Delay: d}}
}

// TestStartTransient_ForcesBackgroundThroughRegistry: at Transient the
// response is already complete, so inline mode is meaningless -- every
// fault, inline types included, runs in Background mode through the
// registry: stoppable by rule name, deduplicated by StartPolicy, reported
// to the injection hook under the "transient" point.
func TestStartTransient_ForcesBackgroundThroughRegistry(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var points []string
	i := New(nil, trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, point string) { points = append(points, point) }))
	req := evaluator.Request{Point: evaluator.Transient}

	// The delay far outlasts the test: the fault can only end early if the
	// registry stops it (an expired delay would also satisfy a done-channel
	// check, which is how a no-op Stop used to pass).
	cr, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", 30*time.Second))
	if err != nil || cr.Handle == nil {
		t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
	}
	if cr.Decision.Mode != evaluator.Background {
		t.Fatalf("mode = %v, want Background", cr.Decision.Mode)
	}
	if len(points) != 1 || points[0] != "transient" {
		t.Fatalf("injection hook points = %v, want [transient]", points)
	}
	// Registry path: a second start under the same rule joins instead of starting.
	again, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", 30*time.Second))
	if err != nil || again.Handle != nil {
		t.Fatalf("second start: handle=%v err=%v, want a deduplicated join", again.Handle, err)
	}
	// Stoppable by rule name, as rule removal requires.
	reg.Stop("after")
	select {
	case res := <-cr.Handle.Done():
		if !errors.Is(res.Err, context.Canceled) {
			t.Fatalf("stopped transient fault ended with err = %v, want context.Canceled", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transient fault did not stop through the registry")
	}
}

// TestStartTransient_OutlivesRequestAndKeepsTraceContext: the fault must not
// die with the request, but its span must still be parented on the request's
// context (values survive context.WithoutCancel; cancellation does not).
// Both the registry and the direct path must behave this way.
func TestStartTransient_OutlivesRequestAndKeepsTraceContext(t *testing.T) {
	type key struct{}
	for _, withRegistry := range []bool{true, false} {
		t.Run(map[bool]string{true: "registry", false: "direct"}[withRegistry], func(t *testing.T) {
			rec := &recordingTracer{}
			var opts []Option
			if withRegistry {
				reg := NewFaultRegistry()
				t.Cleanup(reg.Close)
				opts = append(opts, WithRegistry(reg))
			}
			i := New(nil, rec, opts...)
			reqCtx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace-parent"))

			cr, err := i.StartTransient(reqCtx, evaluator.Request{Point: evaluator.Transient}, inlineLatencyDecision("t", 50*time.Millisecond))
			if err != nil || cr.Handle == nil {
				t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
			}
			cancel() // the request is gone
			if res := <-cr.Handle.Done(); res.Err != nil {
				t.Fatalf("fault died with the request: %v", res.Err)
			}
			if got := rec.startCtx.Value(key{}); got != "trace-parent" {
				t.Fatalf("span context lost the request's values: got %v", got)
			}
			if rec.startCtx.Done() != nil {
				t.Fatal("span context must be detached from request cancellation")
			}
		})
	}
}

// transientRule matches the Transient point with the given labels.
func transientRule(name string, labels map[string]string, f fault.Fault) evaluator.StaticRule {
	return evaluator.StaticRule{Name: name, Point: evaluator.Transient, Labels: labels,
		Decision: evaluator.Decision{Name: name, Reason: "test", Mode: evaluator.Inline, Fault: f}}
}

func shortLatency() fault.Fault {
	return &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}
}

// afterHandlerFault records whether the handler had completed when Start ran.
type afterHandlerFault struct {
	handlerDone  *atomic.Bool
	startedAfter atomic.Bool
	started      chan struct{}
}

func (f *afterHandlerFault) Validate() error { return nil }
func (f *afterHandlerFault) Start(ctx context.Context) (*fault.Handle, error) {
	f.startedAfter.Store(f.handlerDone.Load())
	_, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() { defer cancel(); h.Send(fault.Result{}) }()
	close(f.started)
	return h, nil
}

// slowStartFault blocks inside Start, like the sidecar's agent fault (a
// unix-socket round trip) or the I/O stress (internal/fault/resource/io),
// which writes all its files before Start returns. Through the registry,
// Start also runs under the registry lock, so a slow one must stay off the
// response path.
type slowStartFault struct {
	delay   time.Duration
	started chan struct{}
}

func (f *slowStartFault) Validate() error { return nil }
func (f *slowStartFault) Start(ctx context.Context) (*fault.Handle, error) {
	time.Sleep(f.delay)
	_, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() { defer cancel(); h.Send(fault.Result{}) }()
	close(f.started)
	return h, nil
}

// hookCalls collects injection-hook calls. The Transient start runs on its
// own goroutine, so tests wait on the hook (bounded) instead of reading hook
// or registry state right after ServeHTTP returns.
type hookCalls chan string

func newHookCalls() hookCalls { return make(hookCalls, 16) }

func (h hookCalls) hook() InjectionHook { return func(_, point string) { h <- point } }

// next returns the point of the next hook call, waiting up to d for it.
func (h hookCalls) next(d time.Duration) (string, bool) {
	select {
	case p := <-h:
		return p, true
	case <-time.After(d):
		return "", false
	}
}

// TestIngressMiddleware_TransientFiresAfterResponse: the Transient point is
// evaluated once the handler has written its response, with the response
// status as a label, and the fault starts under the "transient" point.
func TestIngressMiddleware_TransientFiresAfterResponse(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var handlerDone atomic.Bool
	f := &afterHandlerFault{handlerDone: &handlerDone, started: make(chan struct{})}
	hooks := newHookCalls()
	i := New(evaluator.NewStaticEvaluator(transientRule("after", map[string]string{trace.AttrHTTPStatusCode: "201"}, f)),
		trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("made"))
		handlerDone.Store(true)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/orders", nil))
	if rec.Code != http.StatusCreated || rec.Body.String() != "made" {
		t.Fatalf("response = %d %q, want 201 made", rec.Code, rec.Body.String())
	}
	select {
	case <-f.started:
	case <-time.After(time.Second):
		t.Fatal("transient fault did not start")
	}
	if !f.startedAfter.Load() {
		t.Fatal("transient fault started before the handler finished writing")
	}
	// The hook follows the start on the same goroutine, so wait for it.
	if p, ok := hooks.next(time.Second); !ok || p != "transient" {
		t.Fatalf("injection hook = %q (called=%v), want transient", p, ok)
	}
	if p, ok := hooks.next(50 * time.Millisecond); ok {
		t.Fatalf("injection hook called again (%q), want exactly one call", p)
	}
}

// TestIngressMiddleware_TransientDoesNotDelayResponse: a rule authored in
// inline mode still runs in the background at Transient, tracked under its
// rule name.
func TestIngressMiddleware_TransientDoesNotDelayResponse(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	slow := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: 300 * time.Millisecond}, Delay: 300 * time.Millisecond}
	hooks := newHookCalls()
	i := New(evaluator.NewStaticEvaluator(transientRule("slow", nil, slow)), trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))

	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("response took %v; the transient fault must not block it", elapsed)
	}
	// The start is asynchronous; the hook fires once the fault is registered.
	if _, ok := hooks.next(time.Second); !ok {
		t.Fatal("transient fault did not start")
	}
	startedAnother := false
	_, deduped, err := reg.StartOrJoin("slow", evaluator.DeduplicateByRule, func(context.Context) (*fault.Handle, error) {
		startedAnother = true
		return nil, errors.New("unexpected start")
	})
	if startedAnother || !deduped || err != nil {
		t.Fatalf("startedAnother=%v deduped=%v err=%v, want a join on the running transient fault", startedAnother, deduped, err)
	}
	reg.Stop("slow")
}

// TestIngressMiddleware_TransientSlowStartDoesNotDelayResponse: the start
// itself is off the response path, not just the fault's run. A fault whose
// Start blocks (and, through the registry, holds its lock while it does) must
// not hold the response back; it still starts afterwards.
func TestIngressMiddleware_TransientSlowStartDoesNotDelayResponse(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	slow := &slowStartFault{delay: 300 * time.Millisecond, started: make(chan struct{})}
	hooks := newHookCalls()
	i := New(evaluator.NewStaticEvaluator(transientRule("slow-start", nil, slow)),
		trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))

	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("response took %v; a blocking transient Start must not delay it", elapsed)
	}
	select {
	case <-slow.started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow-start transient fault never started")
	}
	if p, ok := hooks.next(time.Second); !ok || p != "transient" {
		t.Fatalf("injection hook = %q (called=%v), want transient", p, ok)
	}
}

// TestIngressMiddleware_TransientMatchesResponseStatus: the status label is
// the response's status; a rule for another status does not fire.
func TestIngressMiddleware_TransientMatchesResponseStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{{503, true}, {200, false}} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			reg := NewFaultRegistry()
			t.Cleanup(reg.Close)
			hooks := newHookCalls()
			i := New(evaluator.NewStaticEvaluator(transientRule("on-503", map[string]string{trace.AttrHTTPStatusCode: "503"}, shortLatency())),
				trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
			h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
			// The start runs on its own goroutine: wait for it when a fault is
			// expected; when none is, give a stray start time to show itself.
			wait := 50 * time.Millisecond
			if tc.want {
				wait = time.Second
			}
			if _, got := hooks.next(wait); got != tc.want {
				t.Fatalf("status %d: fired=%v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

// TestIngressMiddleware_TransientAfterInlineError: an injected error
// response is a completed request too; Transient sees its status and the
// handler still never runs.
func TestIngressMiddleware_TransientAfterInlineError(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	hooks := newHookCalls()
	i := New(evaluator.NewStaticEvaluator(
		evaluator.StaticRule{Name: "err", Point: evaluator.Ingress, Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: 503, Message: "down"}}},
		transientRule("after-503", map[string]string{trace.AttrHTTPStatusCode: "503"}, shortLatency()),
	), trace.Noop(), WithRegistry(reg), WithInjectionHook(hooks.hook()))
	var reached atomic.Bool
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 503 || reached.Load() {
		t.Fatalf("code=%d reached=%v, want the injected 503 without the handler", rec.Code, reached.Load())
	}
	// "ingress" is reported on the request goroutine; "transient" follows from
	// the start goroutine.
	var points []string
	for range 2 {
		p, ok := hooks.next(time.Second)
		if !ok {
			break
		}
		points = append(points, p)
	}
	if len(points) != 2 || points[0] != "ingress" || points[1] != "transient" {
		t.Fatalf("points = %v, want [ingress transient]", points)
	}
}

// panicStartFault panics inside Start, as a buggy fault implementation, the
// tracer or the injection hook could. It counts the starts that reach it.
type panicStartFault struct{ attempts atomic.Int32 }

func (f *panicStartFault) Validate() error { return nil }
func (f *panicStartFault) Start(context.Context) (*fault.Handle, error) {
	f.attempts.Add(1)
	panic("boom")
}

// syncBuffer is a goroutine-safe sink for the standard logger.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestIngressMiddleware_TransientStartPanicIsContained: the start runs on its
// own goroutine, beyond net/http's per-connection recover, so a panic in
// Fault.Start (or the tracer, or the injection hook) would take the host
// process down. It must be recovered and logged, the response must complete
// normally, and a later request must still work: its start reaches
// Fault.Start too, so neither the registry lock nor the middleware was left
// wedged by the first panic.
func TestIngressMiddleware_TransientStartPanicIsContained(t *testing.T) {
	var logs syncBuffer
	prev := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(prev) })

	reg := NewFaultRegistry()
	t.Cleanup(func() {
		// Close takes the registry lock: if a panic had left it held, fail
		// instead of hanging the package.
		done := make(chan struct{})
		go func() { reg.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("registry Close hung: a panicking Start left its lock held")
		}
	})
	boom := &panicStartFault{}
	i := New(evaluator.NewStaticEvaluator(transientRule("boom", nil, boom)), trace.Noop(), WithRegistry(reg))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	}))

	for n := 1; n <= 2; n++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
		if rec.Code != http.StatusAccepted || rec.Body.String() != "ok" {
			t.Fatalf("request %d: response = %d %q, want 202 ok", n, rec.Code, rec.Body.String())
		}
	}

	// Each start panics on its own goroutine; both must be recovered and logged.
	const want = "atropos: transient start panic: boom"
	deadline := time.Now().Add(2 * time.Second)
	for strings.Count(logs.String(), want) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("recovered panics logged = %d (Fault.Start reached %d times), want 2 of each; log:\n%s",
				strings.Count(logs.String(), want), boom.attempts.Load(), logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := boom.attempts.Load(); got != 2 {
		t.Fatalf("Fault.Start reached %d times, want 2", got)
	}
}

// TestStatusRecorder pins the status the Transient label is built from: 200
// until the handler writes, the first WriteHeader wins, a bare Write is 200,
// and Unwrap exposes the underlying writer for http.ResponseController.
func TestStatusRecorder(t *testing.T) {
	w := httptest.NewRecorder()
	r := NewStatusRecorder(w)
	if r.Status() != 200 {
		t.Fatalf("default status = %d, want 200", r.Status())
	}
	r.WriteHeader(404)
	r.WriteHeader(500)
	if r.Status() != 404 || w.Code != 404 {
		t.Fatalf("status = %d (underlying %d), want the first WriteHeader (404)", r.Status(), w.Code)
	}
	bare := NewStatusRecorder(httptest.NewRecorder())
	_, _ = bare.Write([]byte("x"))
	if bare.Status() != 200 {
		t.Fatalf("status after bare Write = %d, want 200", bare.Status())
	}
	if r.Unwrap() != w {
		t.Fatal("Unwrap must return the underlying writer")
	}
}

// statusLog records every status code forwarded to the writer it wraps.
type statusLog struct {
	http.ResponseWriter
	codes []int
}

func (s *statusLog) WriteHeader(code int) {
	s.codes = append(s.codes, code)
	s.ResponseWriter.WriteHeader(code)
}

// TestStatusRecorder_InformationalStatusIsNotFinal: net/http sends a 1xx
// other than 101 as an informational response and lets the handler go on to
// write the final status (103 Early Hints, then 500), so the recorder must
// forward it without recording it -- the Transient label and the request
// metrics read the final status. 101 Switching Protocols is the one 1xx that
// is final.
func TestStatusRecorder_InformationalStatusIsNotFinal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		do        func(http.ResponseWriter)
		want      int
		forwarded []int
	}{
		{"103 then 500", func(w http.ResponseWriter) { w.WriteHeader(103); w.WriteHeader(500) }, 500, []int{103, 500}},
		{"100 and 103 then 201", func(w http.ResponseWriter) { w.WriteHeader(100); w.WriteHeader(103); w.WriteHeader(201) }, 201, []int{100, 103, 201}},
		{"103 then an implicit 200", func(w http.ResponseWriter) { w.WriteHeader(103); _, _ = w.Write([]byte("x")) }, 200, []int{103}},
		{"101 is final", func(w http.ResponseWriter) { w.WriteHeader(101); w.WriteHeader(500) }, 101, []int{101, 500}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			under := &statusLog{ResponseWriter: httptest.NewRecorder()}
			r := NewStatusRecorder(under)
			tc.do(r)
			if r.Status() != tc.want {
				t.Fatalf("recorded status = %d, want %d", r.Status(), tc.want)
			}
			if !slices.Equal(under.codes, tc.forwarded) {
				t.Fatalf("forwarded WriteHeader calls = %v, want %v (every call is forwarded, in order)", under.codes, tc.forwarded)
			}
		})
	}
}
