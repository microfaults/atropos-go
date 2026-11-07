package instrument

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
)

// Ruling 6 / Review Focus 3, end to end: a request arrives with a foreign
// baggage member (and a workflow member) through the full ingress
// composition; the handler's outbound call, matched by a cache-box rule with
// an experiment, goes through the full egress composition. The upstream sees
// the foreign member exactly as it arrived, the workflow member, and the two
// atropos experiment members -- nothing the service received is dropped.
func TestBaggage_ForeignMembersPreservedIngressToEgress(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	cb := cachebox.New(cachebox.Config{
		Store:       cachebox.NewRecordBuffer(cachebox.RecordBufferConfig{MaxEntries: 10}),
		KeyStrategy: cachebox.KeyStrategyExactWithHost,
	})
	t.Cleanup(cb.Stop)
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "rec", Point: evaluator.Egress,
		Labels: map[string]string{trace.AttrHTTPHost: "dep"},
		Decision: evaluator.Decision{Name: "rec", Reason: "test", CacheBox: evaluator.CacheBoxPassthrough,
			CacheBoxContext: &cachebox.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "ph-1", KeyStrategy: "exact_with_host"}},
	}), nil, interceptor.WithCacheBox(cb))

	var upstream http.Header
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		upstream = r.Header.Clone()
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	client := EgressTransport(base, func() *interceptor.Interceptor { return ic })
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out := httptest.NewRequest("GET", "http://dep/x", nil).WithContext(r.Context())
		resp, err := client.RoundTrip(out)
		if err != nil {
			t.Errorf("egress: %v", err)
			return
		}
		resp.Body.Close()
	}), "svc", ic)

	in := httptest.NewRequest("GET", "http://svc/front", nil)
	in.Header.Set("Baggage", "tenant=acme;region=eu,atropos.workflow=checkout")
	h.ServeHTTP(httptest.NewRecorder(), in)

	got, err := baggage.Parse(upstream.Get("Baggage"))
	if err != nil {
		t.Fatalf("upstream baggage %q: %v", upstream.Get("Baggage"), err)
	}
	if got.Member("tenant").String() != "tenant=acme;region=eu" {
		t.Fatalf("foreign member = %q, want it unchanged (baggage %q)", got.Member("tenant").String(), got.String())
	}
	if got.Member(trace.BaggageWorkflow).Value() != "checkout" || got.Member(trace.BaggageExperimentID).Value() != "exp-1" || got.Member(trace.BaggagePhaseID).Value() != "ph-1" || got.Len() != 4 {
		t.Fatalf("upstream baggage = %q, want tenant, workflow and the two experiment members", got.String())
	}
}
