package instrument

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func errorRule(point evaluator.InjectionPoint, status int, msg string) evaluator.StaticRule {
	return evaluator.StaticRule{
		Name: "err", Point: point,
		Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: status, Message: msg}},
	}
}

// TestIngressMiddleware_ErrorFaultShortCircuits pins the documented inline
// error semantics: the configured status and message ARE the response and
// the business handler never runs.
func TestIngressMiddleware_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(errorRule(evaluator.Ingress, 503, "injected fault")), nil)
	var reached atomic.Bool
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }), "svc", ic)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "injected fault") {
		t.Fatalf("code=%d body=%q, want 503 + message", rec.Code, rec.Body.String())
	}
	if rec.Header().Get(interceptor.HeaderFault) != "error" {
		t.Fatal("synthetic error response must be marked X-Atropos-Fault: error")
	}
	if reached.Load() {
		t.Fatal("handler must not run when an inline error fault fires")
	}
}

// TestEgressTransport_ErrorFaultShortCircuits: at egress the synthetic
// response stands in for the upstream, which is never called.
func TestEgressTransport_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(errorRule(evaluator.Egress, 502, "boom")), nil)
	var live atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		live.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	tr := EgressTransport(base, func() *interceptor.Interceptor { return ic })

	resp, err := tr.RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 502 || !strings.Contains(string(body), "boom") || live.Load() != 0 {
		t.Fatalf("status=%d body=%q live=%d", resp.StatusCode, body, live.Load())
	}
	if resp.Header.Get(interceptor.HeaderFault) != "error" {
		t.Fatal("synthetic error response must be marked X-Atropos-Fault: error")
	}
}

// TestEgressTransport_LatencyFaultStillForwards guards the other inline
// faults: a latency fault delays, then the upstream is called normally.
func TestEgressTransport_LatencyFaultStillForwards(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "lat", Point: evaluator.Egress,
		Decision: evaluator.Decision{Name: "lat", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: 20e6}, Delay: 20e6}},
	}), nil)
	var live atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		live.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	resp, err := EgressTransport(base, func() *interceptor.Interceptor { return ic }).RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil || resp.StatusCode != 200 || live.Load() != 1 {
		t.Fatalf("err=%v status=%v live=%d", err, resp, live.Load())
	}
}

// TestIngressMiddleware_TransientThroughFullComposition: otelhttp + metrics +
// fault middleware; the transient rule sees the status the handler wrote and
// the injection hook is called with the transient point.
func TestIngressMiddleware_TransientThroughFullComposition(t *testing.T) {
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	points := make(chan string, 4)
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "after-404", Point: evaluator.Transient, Labels: map[string]string{trace.AttrHTTPStatusCode: "404"},
		Decision: evaluator.Decision{Name: "after-404", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}},
	}), nil, interceptor.WithRegistry(reg), interceptor.WithInjectionHook(func(_ context.Context, _, p string) { points <- p }))
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }), "svc", ic)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/missing", nil))
	if rec.Code != 404 {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	// The transient start runs on its own goroutine: wait for the hook.
	select {
	case p := <-points:
		if p != "transient" {
			t.Fatalf("injection point = %q, want transient", p)
		}
	case <-time.After(time.Second):
		t.Fatal("transient fault did not start")
	}
	select {
	case p := <-points:
		t.Fatalf("unexpected second injection point %q, want exactly [transient]", p)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestIngressMiddleware_FlushReachesClientBeforeHandlerReturns: an SSE-style
// handler flushes an event while it is still running. The writer it sees is
// the fault middleware's recorder, wrapped by the metrics recorder, wrapped
// by otelhttp, wrapped by the server's: each layer must forward Flush,
// or the client waits for the handler to return and streaming is silently
// buffered.
func TestIngressMiddleware_FlushReachesClientBeforeHandlerReturns(t *testing.T) {
	ic := interceptor.New(nil, nil)
	returned := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		fl, ok := w.(http.Flusher)
		if !ok {
			t.Errorf("the handler's writer %T is not an http.Flusher", w)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		fl.Flush()
		select {
		case <-release: // the client has seen the first event
		case <-time.After(2 * time.Second): // bound the failure: the flush never arrived
		}
		_, _ = io.WriteString(w, "data: two\n\n")
	}), "svc", ic))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil || first != "data: one\n" {
		t.Fatalf("first event = %q, err=%v, want %q", first, err, "data: one\n")
	}
	select {
	case <-returned:
		t.Fatal("the handler had already returned when the first event arrived: Flush did not reach the client through the ingress stack")
	default:
	}
	close(release)
	rest, err := io.ReadAll(br)
	if err != nil || string(rest) != "\ndata: two\n\n" {
		t.Fatalf("rest of the stream = %q, err=%v, want the blank line and the second event", rest, err)
	}
}

// TestIngressMiddleware_HijackThroughFullComposition: a handler that takes
// over the connection via http.ResponseController gets it, through every
// layer's Unwrap, and can write a raw response on it (websocket upgrades and
// raw TCP handoffs rely on this).
func TestIngressMiddleware_HijackThroughFullComposition(t *testing.T) {
	ic := interceptor.New(nil, nil)
	hijackErr := make(chan error, 1)
	srv := httptest.NewServer(IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			hijackErr <- err
			return
		}
		defer conn.Close()
		_, err = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nhijacked")
		if err == nil {
			err = rw.Flush()
		}
		hijackErr <- err
	}), "svc", ic))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v (the handler could not hijack: %v)", err, drain(hijackErr))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "hijacked" {
		t.Fatalf("response = %d %q, want the raw 200 hijacked written on the hijacked connection (hijack error: %v)",
			resp.StatusCode, body, drain(hijackErr))
	}
	if err := <-hijackErr; err != nil {
		t.Fatalf("hijack/write: %v", err)
	}
}

// drain returns the error waiting on ch, or nil if none is.
func drain(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	default:
		return nil
	}
}

func matchLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, p := range pairs {
		if v, ok := want[p.GetName()]; !ok || v != p.GetValue() {
			return false
		}
	}
	return true
}

func installSampledProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample())))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return exp
}

func histogramExemplarLabel(t *testing.T, name string, labels map[string]string, key string) string {
	t.Helper()
	families, _ := metrics.Registry.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matchLabels(m.GetLabel(), labels) {
				continue
			}
			for _, b := range m.GetHistogram().GetBucket() {
				for _, l := range b.GetExemplar().GetLabel() {
					if l.GetName() == key {
						return l.GetValue()
					}
				}
			}
		}
	}
	return ""
}

// counterMetric returns the counter series of name with exactly labels, nil
// when there is none.
func counterMetric(t *testing.T, name string, labels map[string]string) *dto.Counter {
	t.Helper()
	families, _ := metrics.Registry.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), labels) {
				return m.GetCounter()
			}
		}
	}
	return nil
}

func exemplarLabel(e *dto.Exemplar, key string) string {
	for _, l := range e.GetLabel() {
		if l.GetName() == key {
			return l.GetValue()
		}
	}
	return ""
}

func TestIngressMiddleware_ExemplarCarriesTraceID(t *testing.T) {
	exp := installSampledProvider(t)
	ic := interceptor.New(nil, nil)
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), "exemplar-svc", ic)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want the server span", len(spans))
	}
	got := histogramExemplarLabel(t, "http_server_request_duration_seconds", map[string]string{"method": "GET", "status_code": "204", "service": "exemplar-svc"}, "trace_id")
	if got != spans[0].SpanContext.TraceID().String() {
		t.Fatalf("exemplar trace_id = %q, want %s", got, spans[0].SpanContext.TraceID())
	}
	// The measurement sits inside the otelhttp span, so the exemplar names
	// the server span itself, not its parent.
	if got := histogramExemplarLabel(t, "http_server_request_duration_seconds", map[string]string{"method": "GET", "status_code": "204", "service": "exemplar-svc"}, "span_id"); got != spans[0].SpanContext.SpanID().String() {
		t.Fatalf("exemplar span_id = %q, want the server span %s", got, spans[0].SpanContext.SpanID())
	}
}

// Review Focus 2.
func TestExemplar_NilWhenUnsampled(t *testing.T) {
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{1}, SpanID: oteltrace.SpanID{1}})
	if ex := metrics.Exemplar(oteltrace.ContextWithSpanContext(context.Background(), sc)); ex != nil {
		t.Fatalf("exemplar = %v for an unsampled span", ex)
	}
	if ex := metrics.Exemplar(context.Background()); ex != nil {
		t.Fatalf("exemplar = %v for no span", ex)
	}
	sampled := sc.WithTraceFlags(oteltrace.FlagsSampled)
	if ex := metrics.Exemplar(oteltrace.ContextWithSpanContext(context.Background(), sampled)); ex["trace_id"] != sampled.TraceID().String() || ex["span_id"] != sampled.SpanID().String() {
		t.Fatalf("exemplar = %v", ex)
	}
}

// A nil exemplar leaves the series' existing exemplar in place and never
// panics; a non-nil one replaces it.
func TestExemplarHelpers_NilKeepsTheExemplar(t *testing.T) {
	c := metrics.CacheBoxHitsTotal.WithLabelValues("exemplar-helper-svc")
	h := metrics.HTTPClientRequestDuration.WithLabelValues("GET", "200", "exemplar-helper.test")
	first := metrics.Exemplar(oteltrace.ContextWithSpanContext(context.Background(), oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{9}, SpanID: oteltrace.SpanID{9}, TraceFlags: oteltrace.FlagsSampled})))
	metrics.IncWithExemplar(c, first)
	metrics.ObserveWithExemplar(h, 0.001, first)
	metrics.IncWithExemplar(c, nil)
	metrics.ObserveWithExemplar(h, 0.001, nil)

	got := counterMetric(t, "atropos_cachebox_hits_total", map[string]string{"service": "exemplar-helper-svc"})
	if got.GetValue() != 2 || exemplarLabel(got.GetExemplar(), "trace_id") != first["trace_id"] {
		t.Fatalf("counter = %v, want 2 with the first exemplar kept", got)
	}
	if ex := histogramExemplarLabel(t, "http_client_request_duration_seconds", map[string]string{"method": "GET", "status_code": "200", "target": "exemplar-helper.test"}, "trace_id"); ex != first["trace_id"] {
		t.Fatalf("histogram exemplar trace_id = %q, want the first one kept", ex)
	}
}

// The egress measurement sits inside the otelhttp client span: the duration
// exemplar names the client span, and the cache-box counter is labelled with
// the interceptor's service.
func TestEgressTransport_ExemplarAndCacheBoxServiceLabel(t *testing.T) {
	exp := installSampledProvider(t)
	ic := interceptor.New(nil, nil, interceptor.WithService("egress-exemplar-svc"))
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{interceptor.HeaderCacheMode: {"replay"}}, Body: http.NoBody, Request: r}, nil
	})
	client := &http.Client{Transport: EgressTransport(base, func() *interceptor.Interceptor { return ic })}
	resp, err := client.Get("http://egress-exemplar.test/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	spans := exp.GetSpans()
	if len(spans) != 1 || spans[0].SpanKind != oteltrace.SpanKindClient {
		t.Fatalf("spans = %v, want the client span", spans)
	}
	client0 := spans[0].SpanContext
	labels := map[string]string{"method": "GET", "status_code": "200", "target": "egress-exemplar.test"}
	if got := histogramExemplarLabel(t, "http_client_request_duration_seconds", labels, "span_id"); got != client0.SpanID().String() {
		t.Fatalf("egress exemplar span_id = %q, want the client span %s", got, client0.SpanID())
	}
	hit := counterMetric(t, "atropos_cachebox_hits_total", map[string]string{"service": "egress-exemplar-svc"})
	if hit.GetValue() != 1 || exemplarLabel(hit.GetExemplar(), "trace_id") != client0.TraceID().String() {
		t.Fatalf("cache-box hits{service=egress-exemplar-svc} = %v, want 1 with the client span's trace", hit)
	}
}

func TestMetricsHandler_OpenMetricsExposesExemplars(t *testing.T) {
	installSampledProvider(t)
	ic := interceptor.New(nil, nil)
	IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), "om-svc", ic).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/openmetrics-text") {
		t.Fatalf("content-type = %q", rec.Header().Get("Content-Type"))
	}
	// The exemplar's labels render in map order (trace_id and span_id in
	// either order), so look for a sample line of this test's series that
	// carries both.
	var exemplar string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "http_server_request_duration_seconds_bucket{") && strings.Contains(line, `service="om-svc"`) &&
			strings.Contains(line, " # {") && strings.Contains(line, `trace_id="`) && strings.Contains(line, `span_id="`) {
			exemplar = line
			t.Logf("%s", line)
		}
	}
	if exemplar == "" || !strings.Contains(body, "# EOF") {
		t.Fatalf("OpenMetrics body lacks exemplars/EOF:\n%s", body)
	}
}

func TestFaultInjectionHook_LabelsServiceAndExemplar(t *testing.T) {
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID{7}, SpanID: oteltrace.SpanID{7}, TraceFlags: oteltrace.FlagsSampled})
	metrics.FaultInjectionHook("hook-svc")(oteltrace.ContextWithSpanContext(context.Background(), sc), "*inline.Latency", "ingress")
	families, _ := metrics.Registry.Gather()
	for _, f := range families {
		if f.GetName() != "atropos_fault_injections_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), map[string]string{"fault_type": "*inline.Latency", "injection_point": "ingress", "service": "hook-svc"}) {
				if m.GetCounter().GetValue() != 1 || m.GetCounter().GetExemplar() == nil {
					t.Fatalf("counter = %v", m.GetCounter())
				}
				return
			}
		}
	}
	t.Fatal("atropos_fault_injections_total{service=hook-svc} not found")
}

// The core labels its injections with its service and passes the request
// context to the hook, so a fault started by a sampled request carries that
// request's exemplar.
func TestCore_FaultInjectionsCarryServiceAndRequestExemplar(t *testing.T) {
	exp := installSampledProvider(t)
	core := NewCore(CoreConfig{Service: "core-hook-svc", Instance: "pod-1"})
	t.Cleanup(core.Stop)
	core.host.SetRules([]evaluator.StaticRule{{Name: "lat", Point: evaluator.Ingress,
		Decision: evaluator.Decision{Name: "lat", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}}}})
	if got := core.Interceptor().Service(); got != "core-hook-svc" {
		t.Fatalf("interceptor service = %q", got)
	}
	IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), "core-hook-svc", core.Interceptor()).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	var server oteltrace.SpanContext
	for _, s := range exp.GetSpans() {
		if s.SpanKind == oteltrace.SpanKindServer {
			server = s.SpanContext
		}
	}
	c := counterMetric(t, "atropos_fault_injections_total", map[string]string{"fault_type": "*inline.Latency", "injection_point": "ingress", "service": "core-hook-svc"})
	if c.GetValue() != 1 || exemplarLabel(c.GetExemplar(), "span_id") != server.SpanID().String() {
		t.Fatalf("fault injections{service=core-hook-svc} = %v, want 1 with the server span %s", c, server.SpanID())
	}
}
