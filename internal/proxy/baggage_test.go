package proxy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// Ruling 6 for the sidecar: both hops re-inject through the global
// propagator, which never filters, so a foreign baggage member reaches the
// app (ingress) and the upstream (egress) exactly as the caller sent it.
func TestProxy_ForeignBaggageForwardedOnBothHops(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	const sent = "tenant=acme;region=eu,atropos.workflow=checkout"
	check := func(hop, header string) {
		t.Helper()
		b, err := baggage.Parse(header)
		if err != nil {
			t.Fatalf("%s baggage %q: %v", hop, header, err)
		}
		if b.Len() != 2 || b.Member("tenant").String() != "tenant=acme;region=eu" || b.Member("atropos.workflow").Value() != "checkout" {
			t.Fatalf("%s baggage = %q, want %q", hop, header, sent)
		}
	}

	ti := newTestInstrument(t)
	var mu sync.Mutex
	var appBaggage string
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		appBaggage = r.Header.Get("Baggage")
		mu.Unlock()
	}))
	t.Cleanup(app.Close)
	up := newUpstream(t, `ok`)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		Ingress: &IngressListener{Listen: "127.0.0.1:0", Upstream: hostPort(app)},
		Egress:  []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up.ts)}}})

	doGet(t, http.DefaultClient, "http://"+p.IngressAddr()+"/x", http.Header{"Baggage": {sent}})
	mu.Lock()
	check("ingress", appBaggage)
	mu.Unlock()

	doGet(t, http.DefaultClient, "http://"+p.EgressAddrs()[0]+"/x", http.Header{"Baggage": {sent}})
	up.mu.Lock()
	got := up.hdrs[len(up.hdrs)-1].Get("Baggage")
	up.mu.Unlock()
	check("egress", got)
}
