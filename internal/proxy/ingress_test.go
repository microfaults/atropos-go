package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestIngress_ForwardsToAppPreservingHost(t *testing.T) {
	ti := newTestInstrument(t)
	var mu sync.Mutex
	var gotHost, gotURI string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHost, gotURI = r.Host, r.RequestURI
		mu.Unlock()
		w.Write([]byte("app"))
	}))
	t.Cleanup(app.Close)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Ingress: &IngressListener{Listen: "127.0.0.1:0", Upstream: hostPort(app)}})

	req, _ := http.NewRequest("GET", "http://"+p.IngressAddr()+"/cart/42?x=1", nil)
	req.Host = "frontend:8080"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if resp.StatusCode != 200 || gotHost != "frontend:8080" || gotURI != "/cart/42?x=1" {
		t.Fatalf("status=%d host=%q uri=%q", resp.StatusCode, gotHost, gotURI)
	}
}

func TestIngress_InlineLatencyApplies(t *testing.T) {
	ti := newTestInstrument(t)
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("app")) }))
	t.Cleanup(app.Close)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Ingress: &IngressListener{Listen: "127.0.0.1:0", Upstream: hostPort(app)}})
	ti.rules(t, latencyRule("ingress", "150ms", nil))

	start := time.Now()
	resp, _ := doGet(t, http.DefaultClient, "http://"+p.IngressAddr()+"/x", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("ingress latency fault not applied: %v", el)
	}
}

// TestIngress_InjectsSidecarSpanContext: the app receives the sidecar's
// SERVER span as its parent, and that span's parent is the caller's.
func TestIngress_InjectsSidecarSpanContext(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	ti := newTestInstrument(t)
	var mu sync.Mutex
	var appTraceparent string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		appTraceparent = r.Header.Get("Traceparent")
		mu.Unlock()
	}))
	t.Cleanup(app.Close)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Ingress: &IngressListener{Listen: "127.0.0.1:0", Upstream: hostPort(app)}})

	const traceID, callerSpan = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	doGet(t, http.DefaultClient, "http://"+p.IngressAddr()+"/x", http.Header{"Traceparent": {"00-" + traceID + "-" + callerSpan + "-01"}})

	mu.Lock()
	got := appTraceparent
	mu.Unlock()
	parts := strings.Split(got, "-")
	if len(parts) != 4 || parts[1] != traceID || parts[2] == callerSpan {
		t.Fatalf("app traceparent = %q, want caller's trace id with the sidecar span as parent", got)
	}
	var found bool
	for _, s := range exporter.GetSpans() {
		if s.SpanContext.SpanID().String() == parts[2] {
			found = true
			if s.Parent.SpanID().String() != callerSpan {
				t.Fatalf("sidecar span parent = %s, want caller span %s", s.Parent.SpanID(), callerSpan)
			}
		}
	}
	if !found {
		t.Fatalf("no exported span with id %s (the app's parent must be the sidecar's server span)", parts[2])
	}
}
