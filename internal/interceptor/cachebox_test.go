package interceptor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"atropos-go/internal/cachebox"
	"atropos-go/internal/evaluator"
	"atropos-go/internal/trace"
)

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

func TestHandleCacheBox_ReplayServesFromCache(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(countingHandler(&hits, "server-body", 0))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)

	// First interceptor: record with passthrough.
	iRec, cb := newTestInterceptor(t, cacheBoxRule(u.Host, evaluator.CacheBoxPassthrough))
	clientRec := &http.Client{Transport: iRec.EgressTransport(http.DefaultTransport)}
	resp, err := clientRec.Get(srv.URL + "/items?id=42")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Drain recorder so the entry is in the store.
	cb.Stop()

	// Now build a replay interceptor around the SAME cache-box so we can hit
	// the cached entry. We use a new interceptor so we can swap rules without
	// racing the old one.
	iReplay := New(
		evaluator.NewStaticEvaluator(cacheBoxRule(u.Host, evaluator.CacheBoxReplay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	clientReplay := &http.Client{Transport: iReplay.EgressTransport(http.DefaultTransport)}

	hitsBefore := hits.Load()
	resp2, err := clientReplay.Get(srv.URL + "/items?id=42")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	if string(body) != "server-body" {
		t.Fatalf("replay body mismatch: %q", body)
	}
	if hits.Load() != hitsBefore {
		t.Fatalf("server saw %d new hits during replay (expected 0)", hits.Load()-hitsBefore)
	}
	if resp2.Header.Get("X-Atropos-Cache-Mode") != "replay" {
		t.Fatalf("missing replay mode header: %v", resp2.Header)
	}
}

func TestHandleCacheBox_ReplayMissFallsBackAndRecords(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(countingHandler(&hits, "fresh-body", 0))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	i, cb := newTestInterceptor(t, cacheBoxRule(u.Host, evaluator.CacheBoxReplay))
	client := &http.Client{Transport: i.EgressTransport(http.DefaultTransport)}

	// Cache is empty; replay should fall through to the real server.
	resp, err := client.Get(srv.URL + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "fresh-body" {
		t.Fatalf("fallback body mismatch: %q", body)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected server hit on cache miss, got %d", hits.Load())
	}

	// The miss should have been recorded for next time.
	cb.Stop()
	if cb.Store().Len() != 1 {
		t.Fatalf("expected miss fallback to record an entry, store has %d", cb.Store().Len())
	}
}

func TestHandleCacheBox_ReplayDelaySleepsAtLeastObservedLatency(t *testing.T) {
	const observed = 80 * time.Millisecond
	var hits atomic.Int64
	srv := httptest.NewServer(countingHandler(&hits, "slow", observed))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)

	// Record phase: passthrough so we capture a real observed latency.
	iRec, cb := newTestInterceptor(t, cacheBoxRule(u.Host, evaluator.CacheBoxPassthrough))
	clientRec := &http.Client{Transport: iRec.EgressTransport(http.DefaultTransport)}
	resp, err := clientRec.Get(srv.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	cb.Stop()

	// Replay phase: replay with delay. We time the call and expect it to be
	// close to the observed latency.
	iReplay := New(
		evaluator.NewStaticEvaluator(cacheBoxRule(u.Host, evaluator.CacheBoxReplayDelay)),
		trace.Noop(),
		WithCacheBox(cb),
	)
	clientReplay := &http.Client{Transport: iReplay.EgressTransport(http.DefaultTransport)}

	hitsBefore := hits.Load()
	start := time.Now()
	resp2, err := clientReplay.Get(srv.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	body, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(body) != "slow" {
		t.Fatalf("replay_with_delay body: %q", body)
	}
	if hits.Load() != hitsBefore {
		t.Fatalf("replay_with_delay should not hit server")
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

	// Pre-populate an entry with a huge latency. The key must match what
	// DeriveKey will produce for the replay request.
	req, _ := http.NewRequest("GET", srv.URL+"/x", nil)
	req.Host = u.Host
	req.URL.Host = u.Host
	key := cb.DeriveKey(req, nil)
	cb.Store().Put(key, &cachebox.Entry{
		Key:             key,
		StatusCode:      200,
		Header:          http.Header{},
		Body:            []byte("cached"),
		ObservedLatency: 5 * time.Second,
		RecordedAt:      time.Now(),
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
