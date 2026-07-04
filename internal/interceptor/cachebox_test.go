package interceptor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

// failRoundTripper fails the test immediately if RoundTrip is ever invoked.
// Used to assert that a fail-closed miss (INV-1) never reaches the live
// downstream.
type failRoundTripper struct {
	t *testing.T
}

func (f failRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	f.t.Fatalf("unexpected live RoundTrip call for %s %s -- a fail-closed miss must never reach the downstream", r.Method, r.URL)
	return nil, errors.New("unreachable")
}

// errBodyReader always fails to read, simulating a request-body buffering
// error under a key strategy that needs the body.
type errBodyReader struct{}

func (errBodyReader) Read([]byte) (int, error) { return 0, errors.New("simulated read error") }
func (errBodyReader) Close() error             { return nil }

// newTestInterceptor builds an interceptor with the given rules and a fresh
// cache-box backed by an in-memory store. Returns the interceptor and the
// cache-box so tests can inspect the store.
func newTestInterceptor(t *testing.T, rules ...evaluator.StaticRule) (*Interceptor, *cachebox.CacheBox) {
	t.Helper()
	eval := evaluator.NewStaticEvaluator(rules...)
	cb := cachebox.New(cachebox.Config{
		Store:       cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 100}),
		KeyStrategy: cachebox.KeyStrategyExactWithHost,
	})
	t.Cleanup(func() { cb.Stop() })
	return New(eval, trace.Noop(), WithCacheBox(cb)), cb
}

// cacheBoxRule builds a StaticRule that matches any egress request to host
// and returns the given CacheBoxAction.
func cacheBoxRule(host string, action evaluator.CacheBoxAction) evaluator.StaticRule {
	return evaluator.StaticRule{
		Name:  "test-" + action.String(),
		Point: evaluator.Egress,
		Labels: map[string]string{
			trace.AttrHTTPHost: host,
		},
		Decision: evaluator.Decision{
			Reason:   "test cache-box rule",
			CacheBox: action,
		},
	}
}

// countingHandler returns an http.HandlerFunc that increments a counter on
// each request and returns a fixed body + latency.
func countingHandler(counter *atomic.Int64, body string, latency time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		if latency > 0 {
			time.Sleep(latency)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, body)
	}
}

func TestHandleCacheBox_PassthroughRecords(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(countingHandler(&hits, "server-body", 0))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	host := u.Host
	_ = host
	i, cb := newTestInterceptor(t, cacheBoxRule(u.Host, evaluator.CacheBoxPassthrough))
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	resp, err := client.Get(srv.URL + "/items?id=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "server-body" {
		t.Fatalf("caller got wrong body: %q", body)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected 1 hit on server, got %d", hits.Load())
	}
	if resp.Header.Get("X-Atropos-Cache-Key") == "" {
		t.Fatal("expected cache key header")
	}
	if resp.Header.Get("X-Atropos-Cache-Latency-Us") == "" {
		t.Fatal("expected cache latency header")
	}

	// Wait for the async recorder drain.
	cb.Stop()
	if cb.Store().Len() != 1 {
		t.Fatalf("expected 1 entry in store, got %d", cb.Store().Len())
	}
}

// TestHandleCacheBox_ReplayServesFromCache pins the post-ATRO-4 contract:
// replay serves only from an installed ReplaySet, never from whatever a
// passthrough happened to record locally (that would be the "cache"
// behavior this refactor deliberately removes -- see design doc §5
// critique). failRoundTripper proves the live downstream is never touched.
func TestHandleCacheBox_ReplayServesFromCache(t *testing.T) {
	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExactWithHost})
	t.Cleanup(func() { cb.Stop() })

	req, _ := http.NewRequest(http.MethodGet, "http://cache-hit.test/items?id=42", nil)
	key := cb.DeriveKey(req, nil)
	cb.InstallReplaySet("exp-1:phase-1", map[string]*cachebox.Entry{
		key: {
			Key:             key,
			StatusCode:      200,
			Header:          http.Header{},
			Body:            []byte("server-body"),
			ObservedLatency: 5 * time.Millisecond,
			RecordedAt:      time.Now(),
		},
	})

	i := New(
		evaluator.NewStaticEvaluator(cacheBoxRule("cache-hit.test", evaluator.CacheBoxReplay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	resp, err := client.Get("http://cache-hit.test/items?id=42")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "server-body" {
		t.Fatalf("replay body mismatch: %q", body)
	}
	if resp.Header.Get("X-Atropos-Cache-Mode") != "replay" {
		t.Fatalf("missing replay mode header: %v", resp.Header)
	}
}

func TestHandleCacheBox_ReplayMissFailsClosed(t *testing.T) {
	before := cachebox.MissStats()
	i, cb := newTestInterceptor(t, cacheBoxRule("cache-miss.test", evaluator.CacheBoxReplay))
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	resp, err := client.Get("http://cache-miss.test/items?id=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Atropos-Cache-Miss") != "1" {
		t.Fatal("expected X-Atropos-Cache-Miss: 1 header")
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q, want application/problem+json", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("miss body is not JSON: %v (%s)", err, body)
	}
	if decoded["error"] != "cachebox_miss" {
		t.Fatalf("unexpected error body: %s", body)
	}

	cb.Stop()
	if cb.Store().Len() != 0 {
		t.Fatalf("fail-closed miss must not record; store has %d entries", cb.Store().Len())
	}
	after := cachebox.MissStats()
	if after.KeyAbsent != before.KeyAbsent+1 {
		t.Fatalf("expected KeyAbsent counter to increment by 1: before=%d after=%d", before.KeyAbsent, after.KeyAbsent)
	}
}

func TestHandleCacheBox_ReplayDelayMissFailsClosed(t *testing.T) {
	i, cb := newTestInterceptor(t, cacheBoxRule("cache-miss.test", evaluator.CacheBoxReplayDelay))
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	start := time.Now()
	resp, err := client.Get("http://cache-miss.test/items?id=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Atropos-Cache-Miss") != "1" {
		t.Fatal("expected X-Atropos-Cache-Miss: 1 header")
	}
	if got := resp.Header.Get("X-Atropos-Cache-Mode"); got != "replay_with_delay" {
		t.Fatalf("mode header = %q, want replay_with_delay", got)
	}
	// A miss must never sleep -- there is no recorded latency to replay.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("miss took too long, looks like it slept: %s", elapsed)
	}

	cb.Stop()
	if cb.Store().Len() != 0 {
		t.Fatalf("fail-closed miss must not record; store has %d entries", cb.Store().Len())
	}
}

func TestHandleCacheBox_UnknownActionFailsClosed(t *testing.T) {
	i, cb := newTestInterceptor(t, cacheBoxRule("cache-miss.test", evaluator.CacheBoxAction(99)))
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	resp, err := client.Get("http://cache-miss.test/x")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Atropos-Cache-Miss") != "1" {
		t.Fatal("expected X-Atropos-Cache-Miss: 1 header for an unrecognized action")
	}

	cb.Stop()
	if cb.Store().Len() != 0 {
		t.Fatalf("unknown-action fail-closed miss must not record; store has %d entries", cb.Store().Len())
	}
}

func TestHandleCacheBox_BodyBufferErrorUnderReplayFailsClosed(t *testing.T) {
	before := cachebox.MissStats()
	cb := cachebox.New(cachebox.Config{
		Store:       cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 10}),
		KeyStrategy: cachebox.KeyStrategyExactWithBody,
	})
	t.Cleanup(func() { cb.Stop() })

	i := New(
		evaluator.NewStaticEvaluator(cacheBoxRule("cache-miss.test", evaluator.CacheBoxReplay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	req, err := http.NewRequest(http.MethodPost, "http://cache-miss.test/search", errBodyReader{})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Atropos-Cache-Miss") != "1" {
		t.Fatal("expected X-Atropos-Cache-Miss: 1 header")
	}

	cb.Stop()
	if cb.Store().Len() != 0 {
		t.Fatalf("body-buffer-error fail-closed miss must not record; store has %d entries", cb.Store().Len())
	}
	after := cachebox.MissStats()
	if after.BodyBufferFailed != before.BodyBufferFailed+1 {
		t.Fatalf("expected BodyBufferFailed counter to increment by 1: before=%d after=%d", before.BodyBufferFailed, after.BodyBufferFailed)
	}
}

// TestHandleCacheBox_ReplayDelaySleepsAtLeastObservedLatency pins the
// replay_with_delay contract: the sleep duration comes from the installed
// entry's ObservedLatency. Populating that field realistically (via a real
// passthrough call) is orthogonal to what this test exercises, so the entry
// is installed directly with a known latency -- see
// TestHandleCacheBox_ReplayServesFromCache for why passthrough no longer
// feeds replay.
func TestHandleCacheBox_ReplayDelaySleepsAtLeastObservedLatency(t *testing.T) {
	const observed = 80 * time.Millisecond

	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExactWithHost})
	t.Cleanup(func() { cb.Stop() })

	req, _ := http.NewRequest(http.MethodGet, "http://cache-hit.test/slow", nil)
	key := cb.DeriveKey(req, nil)
	cb.InstallReplaySet("exp-1:phase-1", map[string]*cachebox.Entry{
		key: {
			Key:             key,
			StatusCode:      200,
			Header:          http.Header{},
			Body:            []byte("slow"),
			ObservedLatency: observed,
			RecordedAt:      time.Now(),
		},
	})

	i := New(
		evaluator.NewStaticEvaluator(cacheBoxRule("cache-hit.test", evaluator.CacheBoxReplayDelay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	client := &http.Client{Transport: i.EgressTransport(failRoundTripper{t: t})}

	start := time.Now()
	resp, err := client.Get("http://cache-hit.test/slow")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "slow" {
		t.Fatalf("replay_with_delay body: %q", body)
	}
	// Allow a wide lower bound -- we want to see the sleep, not exact timing.
	if elapsed < observed*3/4 {
		t.Fatalf("replay_with_delay did not sleep: elapsed=%s, expected >= %s", elapsed, observed*3/4)
	}
}

func TestHandleCacheBox_QueryParamVariance(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, r.URL.RawQuery)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	i, cb := newTestInterceptor(t, cacheBoxRule(u.Host, evaluator.CacheBoxPassthrough))
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	for _, q := range []string{"id=1", "id=2", "id=3"} {
		resp, err := client.Get(srv.URL + "/x?" + q)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	cb.Stop()

	if cb.Store().Len() != 3 {
		t.Fatalf("expected 3 distinct cache entries, got %d", cb.Store().Len())
	}
}

func TestHandleCacheBox_ReplayDelayCancellable(t *testing.T) {
	// Cache an entry with a huge observed latency, then replay with a
	// context that is canceled almost immediately.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "whatever")
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cb := cachebox.New(cachebox.Config{
		Store:       cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 10}),
		KeyStrategy: cachebox.KeyStrategyExactWithHost,
	})
	t.Cleanup(func() { cb.Stop() })

	// Install an entry with a huge latency into the replay set. The key must
	// match what DeriveKey will produce for the replay request.
	req, _ := http.NewRequest("GET", srv.URL+"/x", nil)
	req.Host = u.Host
	req.URL.Host = u.Host
	key := cb.DeriveKey(req, nil)
	cb.InstallReplaySet("exp-1:phase-1", map[string]*cachebox.Entry{
		key: {
			Key:             key,
			StatusCode:      200,
			Header:          http.Header{},
			Body:            []byte("cached"),
			ObservedLatency: 5 * time.Second,
			RecordedAt:      time.Now(),
		},
	})

	i := New(
		evaluator.NewStaticEvaluator(cacheBoxRule(u.Host, evaluator.CacheBoxReplayDelay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	doReq, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/x", nil)
	doReq.Host = u.Host
	start := time.Now()
	_, err := client.Do(doReq)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("replay_with_delay did not respect context cancellation: %s", elapsed)
	}
}

func TestHandleCacheBox_OversizeNotCached(t *testing.T) {
	big := strings.Repeat("x", 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	cb := cachebox.New(cachebox.Config{
		Store:        cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: 10}),
		KeyStrategy:  cachebox.KeyStrategyExactWithHost,
		MaxBodyBytes: 100, // tiny cap so the 2KB response is oversize
	})
	t.Cleanup(func() { cb.Stop() })
	i := New(
		evaluator.NewStaticEvaluator(cacheBoxRule(u.Host, evaluator.CacheBoxPassthrough)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	resp, err := client.Get(srv.URL + "/big")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) != len(big) {
		t.Fatalf("truncated body for caller: got %d, want %d", len(body), len(big))
	}
	cb.Stop()
	if cb.Store().Len() != 0 {
		t.Fatalf("oversized response should not be cached, store has %d", cb.Store().Len())
	}
}

func TestHandleCacheBox_FallsThroughWithoutRule(t *testing.T) {
	// Ensure that fault-rule semantics are not affected by the cache-box
	// plumbing: a normal request with no matching rule goes to the server.
	var hits atomic.Int64
	srv := httptest.NewServer(countingHandler(&hits, "ok", 0))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	// No rules.
	cb := cachebox.New(cachebox.Config{})
	t.Cleanup(func() { cb.Stop() })
	i := New(evaluator.NewStaticEvaluator(), trace.Noop(), WithCacheBox(cb))
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	resp, err := client.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if hits.Load() != 1 {
		t.Fatalf("expected server hit, got %d", hits.Load())
	}
	_ = u
}

// TestInterceptor_UsesRuleStrategy pins the ATRO-3 done-criteria: the
// matched rule's CacheBoxContext.KeyStrategy is authoritative end-to-end,
// overriding the CacheBox's construction-time default in both directions.
func TestInterceptor_UsesRuleStrategy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "body")
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	// Construction-time default is "exact" -- if the rule's context isn't
	// honored, the derived key would be "exact:"-prefixed instead of "v2:".
	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
	t.Cleanup(func() { cb.Stop() })

	v2Rule := evaluator.StaticRule{
		Name:   "v2-rule",
		Point:  evaluator.Egress,
		Labels: map[string]string{trace.AttrHTTPHost: u.Host},
		Decision: evaluator.Decision{
			Reason:   "test",
			CacheBox: evaluator.CacheBoxPassthrough,
			CacheBoxContext: &cachebox.CacheBoxContext{
				ExperimentID: "exp-1", PhaseID: "phase-1",
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}
	iV2 := New(evaluator.NewStaticEvaluator(v2Rule), trace.Noop(), WithCacheBox(cb))
	clientV2 := &http.Client{Transport: iV2.EgressTransport(http.DefaultTransport)}
	respV2, err := clientV2.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, respV2.Body)
	respV2.Body.Close()
	if key := respV2.Header.Get("X-Atropos-Cache-Key"); !strings.HasPrefix(key, "v2:") {
		t.Fatalf("expected canonical_v2-shaped key from rule context, got %q", key)
	}

	// Flip it: a CacheBox whose construction-time default is canonical_v2
	// (the package default), paired with a rule whose context pins "exact"
	// instead. If the rule wins in both directions, this proves the context
	// is authoritative rather than just "used when the default agrees."
	cbV2Default := cachebox.New(cachebox.Config{}) // default is canonical_v2
	t.Cleanup(func() { cbV2Default.Stop() })

	exactRule := evaluator.StaticRule{
		Name:   "exact-rule",
		Point:  evaluator.Egress,
		Labels: map[string]string{trace.AttrHTTPHost: u.Host},
		Decision: evaluator.Decision{
			Reason:   "test",
			CacheBox: evaluator.CacheBoxPassthrough,
			CacheBoxContext: &cachebox.CacheBoxContext{
				ExperimentID: "exp-1", PhaseID: "phase-1",
				KeyStrategy: "exact", StrategyVersion: 1,
			},
		},
	}
	iExact := New(evaluator.NewStaticEvaluator(exactRule), trace.Noop(), WithCacheBox(cbV2Default))
	clientExact := &http.Client{Transport: iExact.EgressTransport(http.DefaultTransport)}
	respExact, err := clientExact.Get(srv.URL + "/y")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, respExact.Body)
	respExact.Body.Close()
	if key := respExact.Header.Get("X-Atropos-Cache-Key"); !strings.HasPrefix(key, "exact:") {
		t.Fatalf("expected exact-shaped key from rule context despite a canonical_v2 CacheBox default, got %q", key)
	}
}
