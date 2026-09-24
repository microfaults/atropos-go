package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// upstream records what it received.
type upstream struct {
	mu    sync.Mutex
	hosts []string
	uris  []string
	hdrs  []http.Header
	ts    *httptest.Server
}

func newUpstream(t *testing.T, body string) *upstream {
	u := &upstream{}
	u.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.hosts = append(u.hosts, r.Host)
		u.uris = append(u.uris, r.RequestURI)
		u.hdrs = append(u.hdrs, r.Header.Clone())
		u.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	t.Cleanup(u.ts.Close)
	return u
}

func (u *upstream) calls() int { u.mu.Lock(); defer u.mu.Unlock(); return len(u.hosts) }

func TestEgress_ExplicitForwardsAndRewritesHost(t *testing.T) {
	ti := newTestInstrument(t)
	up := newUpstream(t, `{"live":true}`)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		Egress: []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up.ts)}}})

	resp, body := doGet(t, http.DefaultClient, "http://"+p.EgressAddrs()[0]+"/api/items?b=2&a=1", nil)
	if resp.StatusCode != 200 || body != `{"live":true}` {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.hosts) != 1 || up.hosts[0] != hostPort(up.ts) {
		t.Fatalf("upstream Host = %v, want %s (explicit mode rewrites the loopback host)", up.hosts, hostPort(up.ts))
	}
	if up.uris[0] != "/api/items?b=2&a=1" {
		t.Fatalf("upstream URI = %s", up.uris[0])
	}
}

func TestEgress_InlineLatencyApplies(t *testing.T) {
	ti := newTestInstrument(t)
	up := newUpstream(t, `ok`)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		Egress: []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up.ts)}}})
	ti.rules(t, latencyRule("egress", "150ms", nil))

	start := time.Now()
	resp, _ := doGet(t, http.DefaultClient, "http://"+p.EgressAddrs()[0]+"/x", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("egress latency fault not applied through the proxy: %v", el)
	}
	ti.rules(t, `[]`)
	start = time.Now()
	doGet(t, http.DefaultClient, "http://"+p.EgressAddrs()[0]+"/x", nil)
	if el := time.Since(start); el >= 150*time.Millisecond {
		t.Fatalf("latency persisted after the rule was cleared: %v", el)
	}
}

// TestEgress_CacheKeyIdenticalToInProcess is the INV-2 golden test: the key
// the sidecar derives for a request equals the key the in-process host
// derives for the same client-side request.
func TestEgress_CacheKeyIdenticalToInProcess(t *testing.T) {
	ti := newTestInstrument(t)
	up := newUpstream(t, `{"v":1}`)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		Egress: []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up.ts)}}})
	ti.rules(t, cacheBoxRule("passthrough"))
	hdr := http.Header{"Accept": {"application/json"}}

	// Through the sidecar: the app dials the loopback port.
	viaProxy, _ := doGet(t, http.DefaultClient, "http://"+p.EgressAddrs()[0]+"/api/items?b=2&a=1", hdr)
	k1 := viaProxy.Header.Get(interceptor.HeaderCacheKey)

	// In-process: the same client-side request through the shared transport.
	direct := &http.Client{Transport: instrument.EgressTransport(http.DefaultTransport, ti.resolve)}
	viaInProcess, _ := doGet(t, direct, up.ts.URL+"/api/items?b=2&a=1", hdr)
	k2 := viaInProcess.Header.Get(interceptor.HeaderCacheKey)

	if k1 == "" || k1 != k2 {
		t.Fatalf("cache key via sidecar %q != in-process %q", k1, k2)
	}
	if !strings.HasPrefix(k1, "v2:") {
		t.Fatalf("expected a canonical_v2 key, got %q", k1)
	}
	if up.calls() != 2 {
		t.Fatalf("upstream calls = %d, want 2 (both passthrough)", up.calls())
	}
}

func TestEgress_ExtractsTraceContextAndBaggage(t *testing.T) {
	// A recording tracer provider: with the default no-op provider the
	// otelhttp client span would carry the caller's span id unchanged.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	ti := newTestInstrument(t)
	up := newUpstream(t, `ok`)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		Egress: []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up.ts)}}})
	// The rule matches only the "browse" workflow, which arrives as W3C
	// baggage exactly as it does in-process.
	ti.rules(t, latencyRule("egress", "120ms", map[string]string{"atropos.cachebox.workflow": "browse"}))
	url := "http://" + p.EgressAddrs()[0] + "/x"

	start := time.Now()
	doGet(t, http.DefaultClient, url, http.Header{"Baggage": {"atropos.workflow=browse"}})
	if el := time.Since(start); el < 120*time.Millisecond {
		t.Fatalf("workflow-scoped rule did not match baggage-carrying request: %v", el)
	}
	start = time.Now()
	doGet(t, http.DefaultClient, url, http.Header{"Baggage": {"atropos.workflow=checkout"}})
	if el := time.Since(start); el >= 120*time.Millisecond {
		t.Fatalf("workflow-scoped rule matched the wrong workflow: %v", el)
	}

	// Trace continuity: the upstream sees the caller's trace id under a new
	// (client) span id.
	in := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	doGet(t, http.DefaultClient, url, http.Header{"Traceparent": {in}})
	up.mu.Lock()
	got := up.hdrs[len(up.hdrs)-1].Get("Traceparent")
	up.mu.Unlock()
	if !strings.HasPrefix(got, "00-0af7651916cd43dd8448eb211c80319c-") || strings.Contains(got, "b7ad6b7169203331") {
		t.Fatalf("upstream traceparent = %q, want same trace id, new parent span", got)
	}
}
