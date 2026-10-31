package atropos

// Hermetic verification of the one-call embed (httptest only, no live
// control plane):
//
//   - an embed with MANTEION_URL unset serves business + /metrics +
//     /atropos/health + /admin/fault and returns a working shutdown;
//   - business requests pass through the fault/OTel middleware while
//     control/observability paths do not;
//   - the record→replay path stays fail-closed end to end (a frozen miss
//     is a counted synthetic 503, never a live call);
//   - the staged preload commit is what makes replay serve.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// serveOffline embeds a trivial business handler with no control plane and
// returns the test server, the span exporter, and a cleanup-registered
// shutdown.
func serveOffline(t *testing.T, cfg Config) (*httptest.Server, *tracetest.InMemoryExporter) {
	t.Helper()
	t.Setenv("MANTEION_URL", "") // hermetic: never touch a real control plane

	exporter := tracetest.NewInMemoryExporter()
	cfg.TracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	if cfg.Service == "" {
		cfg.Service = "serve-test"
	}

	h, shutdown, err := Serve(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts, exporter
}

func mustGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func postJSON(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestServe_RequiresService(t *testing.T) {
	_, _, err := Serve(context.Background(), Config{})
	if err == nil {
		t.Fatal("expected error for missing Service")
	}
}

// With no BYO provider Serve builds the SDK provider itself; its resource
// tells a backend the spans come from the in-process host, under the instance
// id it registers with.
func TestServe_ResourceNamesTheInProcessHost(t *testing.T) {
	for _, key := range []string{"MANTEION_URL", "ATROPOS_HOST", "COLLECTOR_SERVICE_ADDR", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"} {
		t.Setenv(key, "") // hermetic: no control plane, nothing for the exporter shim to map
	}
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	_, shutdown, err := Serve(context.Background(), Config{Service: "serve-test", InstanceID: "inst-serve", Handler: http.NewServeMux()})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := otel.Tracer("serve-test").Start(context.Background(), "probe")
	defer span.End()
	ro, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatalf("span %T is not an SDK span", span)
	}
	if v, ok := ro.Resource().Set().Value("atropos.host"); !ok || v.AsString() != "in-process" {
		t.Fatalf("atropos.host = %v (present=%v), want in-process", v, ok)
	}
	if v, ok := ro.Resource().Set().Value("service.instance.id"); !ok || v.AsString() != "inst-serve" {
		t.Fatalf("service.instance.id = %v (present=%v), want the registration identity inst-serve", v, ok)
	}
}

// logEntry is one record a logCapture was handed.
type logEntry struct {
	ctx   context.Context
	level slog.Level
	msg   string
	attrs map[string]any
}

// logCapture keeps every record the loggers it hands out receive, for the tests
// that check where the SDK's log lines go.
type logCapture struct {
	mu      sync.Mutex
	entries []logEntry
}

func (c *logCapture) logger() *slog.Logger { return slog.New(&captureHandler{c: c}) }

// messages returns the message of every record captured so far, in order.
func (c *logCapture) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := make([]string, len(c.entries))
	for i, e := range c.entries {
		msgs[i] = e.msg
	}
	return msgs
}

// find returns the first record whose message starts with prefix.
func (c *logCapture) find(prefix string) (logEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if strings.HasPrefix(e.msg, prefix) {
			return e, true
		}
	}
	return logEntry{}, false
}

// captureHandler is the slog.Handler behind logCapture.logger. Handlers derived
// with WithAttrs share the capture, and groups are ignored.
type captureHandler struct {
	c     *logCapture
	attrs []slog.Attr
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	e := logEntry{ctx: ctx, level: r.Level, msg: r.Message, attrs: map[string]any{}}
	for _, a := range h.attrs {
		e.attrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.c.mu.Lock()
	h.c.entries = append(h.c.entries, e)
	h.c.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &captureHandler{c: h.c, attrs: append(slices.Clone(h.attrs), attrs...)}
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

// The SDK's own log lines go to Config.Logger: Serve hands the logger
// InitTelemetry returns to everything it starts. One line from each of Serve's
// two code paths pins it: the offline warning is written by the manteion
// connection through the logger Serve passes it, the passive-mode notice by
// Serve itself.
func TestServe_LogsThroughConfigLogger(t *testing.T) {
	t.Run("embedded host, offline", func(t *testing.T) {
		t.Setenv("ATROPOS_HOST", "")
		capture := &logCapture{}
		serveOffline(t, Config{Service: "log-test", Logger: capture.logger()})
		if _, ok := capture.find("manteion: running in offline mode"); !ok {
			t.Fatalf("Config.Logger got %q, want the manteion offline-mode warning", capture.messages())
		}
	})

	t.Run("passive beside the sidecar", func(t *testing.T) {
		t.Setenv("ATROPOS_HOST", "sidecar")
		t.Setenv("MANTEION_URL", "")
		capture := &logCapture{}
		_, shutdown, err := Serve(context.Background(), Config{
			Service:        "log-test-passive",
			Logger:         capture.logger(),
			TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter())),
		})
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
		t.Cleanup(func() { _ = shutdown(context.Background()) })
		e, ok := capture.find("atropos: ATROPOS_HOST=sidecar")
		if !ok {
			t.Fatalf("Config.Logger got %q, want the passive-mode notice", capture.messages())
		}
		if e.level != slog.LevelInfo || e.attrs["service"] != "log-test-passive" {
			t.Fatalf("passive-mode notice = %v %v, want an Info record for service log-test-passive", e.level, e.attrs)
		}
	})
}

// recordedLogs is the in-memory sdklog.Exporter the end-to-end log test reads.
type recordedLogs struct {
	mu   sync.Mutex
	recs []sdklog.Record
}

func (e *recordedLogs) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.recs = append(e.recs, r.Clone())
	}
	return nil
}

func (e *recordedLogs) Shutdown(context.Context) error   { return nil }
func (e *recordedLogs) ForceFlush(context.Context) error { return nil }

// find returns the first record exported with the given body.
func (e *recordedLogs) find(body string) (sdklog.Record, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.recs {
		if r.Body().AsString() == body {
			return r, true
		}
	}
	return sdklog.Record{}, false
}

// The whole chain behind request-scoped logging, end to end: a request through
// Serve's middleware gets a server span; the armed fault is invalid, so the
// interceptor's ingress check fails and logs through the host's logger, under
// the request's context. With the SDK's default logger on a BYO logger provider
// the bridge stamps the exported record with that server span's trace and span.
func TestServe_CheckErrorLogCarriesTheRequestTrace(t *testing.T) {
	t.Setenv("ATROPOS_HOST", "")
	prev := global.GetLoggerProvider()
	t.Cleanup(func() {
		if global.GetLoggerProvider() != prev {
			global.SetLoggerProvider(prev)
		}
	})
	logs := &recordedLogs{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(logs)))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })

	biz := http.NewServeMux()
	biz.HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "done") })
	ts, spans := serveOffline(t, Config{Service: "log-trace-test", Handler: biz, LoggerProvider: lp})

	// A latency with neither delay nor jitter decodes, but fails Validate when
	// the interceptor starts it.
	if code, body := postJSON(t, ts.URL+"/admin/fault", `{"category":"inline","fault_type":"latency","params":{"delay":"0s"}}`); code != http.StatusCreated {
		t.Fatalf("arm fault: code=%d body=%s", code, body)
	}
	spans.Reset()
	if code, body := mustGet(t, ts.URL+"/work"); code != 200 || body != "done" {
		t.Fatalf("/work: code=%d body=%q, want the handler to run despite the failed check", code, body)
	}

	var server *tracetest.SpanStub
	got := spans.GetSpans()
	for i := range got {
		if got[i].SpanKind == oteltrace.SpanKindServer {
			server = &got[i]
		}
	}
	if server == nil {
		t.Fatalf("no server span exported: %d spans", len(got))
	}
	rec, ok := logs.find("atropos: ingress check error")
	if !ok {
		t.Fatal("the failed ingress check was not logged through the host's logger and the OTel bridge")
	}
	if rec.TraceID() != server.SpanContext.TraceID() || rec.SpanID() != server.SpanContext.SpanID() {
		t.Fatalf("log record trace/span = %s/%s, want the request's server span %s/%s",
			rec.TraceID(), rec.SpanID(), server.SpanContext.TraceID(), server.SpanContext.SpanID())
	}
}

func TestServe_OfflineServesBusinessAndControlSurface(t *testing.T) {
	biz := http.NewServeMux()
	biz.HandleFunc("GET /products", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "catalog")
	})
	ts, _ := serveOffline(t, Config{
		Service: "productcatalogservice",
		Version: "0.1.0",
		Routes:  []Route{{Method: "GET", Path: "/products"}},
		Handler: biz,
	})

	if code, body := mustGet(t, ts.URL+"/products"); code != 200 || body != "catalog" {
		t.Fatalf("business route: code=%d body=%q", code, body)
	}

	code, body := mustGet(t, ts.URL+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics: code=%d", code)
	}
	for _, metric := range []string{"http_server_requests_total", "go_goroutines"} {
		if !strings.Contains(body, metric) {
			t.Errorf("/metrics missing %s", metric)
		}
	}

	code, body = mustGet(t, ts.URL+"/atropos/health")
	if code != 200 {
		t.Fatalf("/atropos/health: code=%d", code)
	}
	var health HealthStatus
	if err := json.Unmarshal([]byte(body), &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Status != "offline" {
		t.Errorf("health status = %q, want offline (MANTEION_URL unset)", health.Status)
	}

	code, body = mustGet(t, ts.URL+"/admin/fault")
	if code != 200 || !strings.Contains(body, `"active":false`) {
		t.Fatalf("/admin/fault: code=%d body=%q", code, body)
	}

	if code, _ := mustGet(t, ts.URL+"/admin/cachebox"); code != 200 {
		t.Fatalf("/admin/cachebox: code=%d", code)
	}
	if code, _ := mustGet(t, ts.URL+"/admin/rules"); code != 200 {
		t.Fatalf("/admin/rules: code=%d", code)
	}
	if code, _ := mustGet(t, ts.URL+"/cachebox/fidelity?experiment_id=e&phase_id=p"); code != 200 {
		t.Fatalf("/cachebox/fidelity: code=%d", code)
	}

	// Unknown business path falls through the middleware to the handler's 404.
	if code, _ := mustGet(t, ts.URL+"/nope"); code != 404 {
		t.Fatalf("unknown path: code=%d, want 404", code)
	}
}

func TestServe_NilHandlerServesControlOnly(t *testing.T) {
	ts, _ := serveOffline(t, Config{Service: "grpc-bed"})

	if code, _ := mustGet(t, ts.URL+"/atropos/health"); code != 200 {
		t.Fatalf("/atropos/health: code=%d", code)
	}
	if code, _ := mustGet(t, ts.URL+"/some/business/path"); code != 404 {
		t.Fatalf("business path with nil Handler: code=%d, want 404", code)
	}
}

// TestServe_BusinessThroughMiddleware_ControlBypassed pins the routing split:
// an armed admin fault delays business traffic (which also gets OTel server
// spans) while the control/observability endpoints see neither the fault nor
// the middleware.
func TestServe_BusinessThroughMiddleware_ControlBypassed(t *testing.T) {
	biz := http.NewServeMux()
	biz.HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "done")
	})
	ts, exporter := serveOffline(t, Config{Service: "bypass-test", Handler: biz})

	// Arm a 300ms inline latency fault through the admin endpoint. The demo
	// evaluator matches every request that reaches the middleware.
	code, _ := postJSON(t, ts.URL+"/admin/fault",
		`{"category":"inline","fault_type":"latency","params":{"delay":"300ms"}}`)
	if code != http.StatusCreated {
		t.Fatalf("arm fault: code=%d", code)
	}

	exporter.Reset()
	start := time.Now()
	code, _ = mustGet(t, ts.URL+"/work")
	elapsed := time.Since(start)
	if code != 200 {
		t.Fatalf("/work: code=%d", code)
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("business request took %v, expected >=250ms from armed fault", elapsed)
	}
	spansAfterBusiness := len(exporter.GetSpans())
	if spansAfterBusiness == 0 {
		t.Fatal("business request must produce OTel spans (middleware wraps it)")
	}
	foundFault := false
	for _, s := range exporter.GetSpans() {
		if s.Name == "atropos.fault.inject" {
			foundFault = true
		}
	}
	if !foundFault {
		t.Fatal("expected atropos.fault.inject span for the business request")
	}

	// Control paths: no fault delay, no middleware spans — even with the
	// fault still armed.
	for _, path := range []string{"/metrics", "/atropos/health", "/admin/fault", "/admin/cachebox"} {
		start := time.Now()
		code, _ := mustGet(t, ts.URL+path)
		elapsed := time.Since(start)
		if code != 200 {
			t.Fatalf("%s: code=%d", path, code)
		}
		if elapsed >= 250*time.Millisecond {
			t.Fatalf("%s took %v with fault armed — control paths must bypass the fault middleware", path, elapsed)
		}
	}
	if got := len(exporter.GetSpans()); got != spansAfterBusiness {
		t.Fatalf("control paths produced %d new spans — they must bypass the OTel middleware", got-spansAfterBusiness)
	}

	// Disarm; business is fast again.
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/admin/fault", nil)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != 200 {
		t.Fatalf("disarm fault: err=%v", err)
	} else {
		resp.Body.Close()
	}
	start = time.Now()
	if code, _ := mustGet(t, ts.URL+"/work"); code != 200 {
		t.Fatalf("/work after disarm: code=%d", code)
	}
	if elapsed := time.Since(start); elapsed >= 250*time.Millisecond {
		t.Fatalf("business request still slow (%v) after disarm", elapsed)
	}
}

// TestServe_TransientFaultSpanJoinsTheRequestTrace pins the Transient span's
// parenting with the production tracer (the interceptor unit tests use a
// recording stub): a Transient rule pushed through /admin/rules fires once
// the response is complete, and its fault span is a child of the request's
// server span, in the same trace, tagged with the transient injection point.
func TestServe_TransientFaultSpanJoinsTheRequestTrace(t *testing.T) {
	biz := http.NewServeMux()
	biz.HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "done")
	})
	ts, exporter := serveOffline(t, Config{Service: "transient-span-test", Handler: biz})

	code, body := postJSON(t, ts.URL+"/admin/rules", `[{
		"name": "after-200",
		"injection_point": "transient",
		"mode": "inline",
		"labels": {"atropos.http.status_code": "200"},
		"fault": {"category": "inline", "fault_type": "latency", "params": {"delay": "20ms"}}
	}]`)
	if code != http.StatusNoContent {
		t.Fatalf("push transient rule: code=%d body=%q", code, body)
	}

	exporter.Reset()
	if code, body := mustGet(t, ts.URL+"/work"); code != 200 || body != "done" {
		t.Fatalf("/work: code=%d body=%q", code, body)
	}

	// The fault starts on its own goroutine and its span ends after the 20ms
	// delay, so wait for both spans to be exported.
	var faultSpan, serverSpan *tracetest.SpanStub
	deadline := time.Now().Add(3 * time.Second)
	for {
		faultSpan, serverSpan = nil, nil
		spans := exporter.GetSpans()
		for i := range spans {
			switch {
			case spans[i].Name == trace.SpanFaultInject:
				faultSpan = &spans[i]
			case spans[i].SpanKind == oteltrace.SpanKindServer:
				serverSpan = &spans[i]
			}
		}
		if faultSpan != nil && serverSpan != nil {
			break
		}
		if time.Now().After(deadline) {
			names := make([]string, 0, len(spans))
			for _, s := range spans {
				names = append(names, s.Name)
			}
			t.Fatalf("after 3s the exporter has spans %v; want the %q span and the server span", names, trace.SpanFaultInject)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !faultSpan.Parent.IsValid() || faultSpan.Parent.SpanID() != serverSpan.SpanContext.SpanID() {
		t.Fatalf("fault span parent = %s, want the server span %s", faultSpan.Parent.SpanID(), serverSpan.SpanContext.SpanID())
	}
	if faultSpan.SpanContext.TraceID() != serverSpan.SpanContext.TraceID() {
		t.Fatalf("fault span trace = %s, want the request's trace %s", faultSpan.SpanContext.TraceID(), serverSpan.SpanContext.TraceID())
	}
	point := ""
	for _, a := range faultSpan.Attributes {
		if string(a.Key) == trace.AttrFaultInjectionPoint {
			point = a.Value.AsString()
		}
	}
	if point != "transient" {
		t.Fatalf("%s = %q, want transient", trace.AttrFaultInjectionPoint, point)
	}
}

// ruleJSON builds the /admin/rules payload for a single egress cache-box rule
// scoped to (exp-1, phase-1) with the canonical_v2 strategy.
func ruleJSON(mode string) string {
	return fmt.Sprintf(`[{
		"name": "cachebox-%s",
		"injection_point": "egress",
		"mode": "inline",
		"cachebox": {
			"mode": %q,
			"context": {
				"experiment_id": "exp-1",
				"phase_id": "phase-1",
				"key_strategy": "canonical_v2",
				"strategy_version": 2
			}
		}
	}]`, mode, mode)
}

func fidelitySnapshot(t *testing.T, baseURL string) FidelitySnapshot {
	t.Helper()
	code, body := mustGet(t, baseURL+"/cachebox/fidelity?experiment_id=exp-1&phase_id=phase-1")
	if code != 200 {
		t.Fatalf("fidelity endpoint: code=%d", code)
	}
	var snap FidelitySnapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("decode fidelity snapshot: %v", err)
	}
	return snap
}

// TestServe_RecordThenReplayFailClosed drives the full loop through the
// embed's public surface: a passthrough rule records an egress response;
// switching the rule to replay WITHOUT a committed preload must fail closed —
// a counted synthetic 503, with the live downstream never called (INV-1).
func TestServe_RecordThenReplayFailClosed(t *testing.T) {
	ts, _ := serveOffline(t, Config{Service: "failclosed-test"})

	var liveCalls atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		liveCalls.Add(1)
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"live":true}`)),
		}, nil
	})
	client := &http.Client{Transport: EgressTransport(base)}

	// Phase 1: record. The passthrough rule forwards to the live downstream
	// and records under the rule-authoritative key.
	if code, body := postJSON(t, ts.URL+"/admin/rules", ruleJSON("passthrough")); code != http.StatusNoContent {
		t.Fatalf("install passthrough rule: code=%d body=%s", code, body)
	}
	resp, err := client.Get("http://downstream.test/api/items")
	if err != nil {
		t.Fatalf("recorded egress call: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("passthrough status = %d", resp.StatusCode)
	}
	if got := liveCalls.Load(); got != 1 {
		t.Fatalf("live calls after passthrough = %d, want 1", got)
	}
	if resp.Header.Get("X-Atropos-Cache-Key") == "" {
		t.Fatal("passthrough response must carry the derived cache key")
	}
	if snap := fidelitySnapshot(t, ts.URL); snap.RecordEnqueued != 1 {
		t.Fatalf("record_enqueued = %d, want 1", snap.RecordEnqueued)
	}

	// Phase 2: freeze. Same context, replay mode, but nothing was preload-
	// committed — recording alone must never feed replay (INV-4), so the
	// lookup misses and fails closed.
	if code, body := postJSON(t, ts.URL+"/admin/rules", ruleJSON("replay")); code != http.StatusNoContent {
		t.Fatalf("install replay rule: code=%d body=%s", code, body)
	}
	resp, err = client.Get("http://downstream.test/api/items")
	if err != nil {
		t.Fatalf("frozen egress call: %v", err)
	}
	missBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("frozen miss status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Atropos-Cache-Miss") != "1" {
		t.Fatal("frozen miss must carry X-Atropos-Cache-Miss: 1")
	}
	if !strings.Contains(string(missBody), "cachebox_miss") {
		t.Fatalf("miss body = %s, want cachebox_miss problem document", missBody)
	}
	if got := liveCalls.Load(); got != 1 {
		t.Fatalf("live calls after frozen miss = %d — a frozen service must NEVER make a live call on a miss", got)
	}

	snap := fidelitySnapshot(t, ts.URL)
	if snap.ReplayMisses != 1 {
		t.Fatalf("replay_misses = %d, want 1 — every miss is counted", snap.ReplayMisses)
	}
	if snap.MissReasons.NotCommitted != 1 {
		t.Fatalf("miss_reasons.not_committed = %d, want 1", snap.MissReasons.NotCommitted)
	}
}

// TestServe_PreloadCommitThenReplayHit completes the loop: a staged preload
// (begin/chunk/commit, W4) with the W5 checksum installs the replay set, and
// the frozen service then serves from cache — still without live calls.
func TestServe_PreloadCommitThenReplayHit(t *testing.T) {
	ts, _ := serveOffline(t, Config{Service: "replayhit-test"})

	var liveCalls atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		liveCalls.Add(1)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: http.NoBody}, nil
	})
	client := &http.Client{Transport: EgressTransport(base)}

	// The entry's key must be exactly what the interceptor will derive for
	// the frozen request (rule-authoritative canonical_v2, no key headers).
	target := "http://downstream.test/api/items"
	u, _ := url.Parse(target)
	deriveReq := &http.Request{Method: "GET", URL: u, Header: http.Header{}}
	key := cachebox.Derive(cachebox.KeyStrategyCanonicalV2, nil, deriveReq, nil)

	entry := &cachebox.Entry{
		Key:             key,
		StatusCode:      200,
		Header:          http.Header{"Content-Type": {"application/json"}},
		Body:            []byte(`{"replayed":true}`),
		ObservedLatency: 5 * time.Millisecond,
		RecordedAt:      time.Now().UTC(),
		ExperimentID:    "exp-1",
		PhaseID:         "phase-1",
		KeyStrategy:     "canonical_v2",
		StrategyVersion: 2,
	}
	checksum := cachebox.SetChecksum([]*cachebox.Entry{entry})

	begin := `{"experiment_id":"exp-1","phase_id":"phase-1","source_phase_id":"phase-0",
		"total_entries":1,"total_chunks":1,"key_strategy":"canonical_v2","strategy_version":2}`
	if code, body := postJSON(t, ts.URL+"/cachebox/preload/begin", begin); code != 200 {
		t.Fatalf("preload begin: code=%d body=%s", code, body)
	}
	chunkPayload, _ := json.Marshal(map[string]any{
		"experiment_id": "exp-1", "phase_id": "phase-1", "chunk_seq": 1,
		"entries": []CacheBoxWireEntry{cachebox.EntryToWire(entry)},
	})
	if code, body := postJSON(t, ts.URL+"/cachebox/preload/chunk", string(chunkPayload)); code != 200 {
		t.Fatalf("preload chunk: code=%d body=%s", code, body)
	}
	commit := fmt.Sprintf(`{"experiment_id":"exp-1","phase_id":"phase-1","total_entries":1,"checksum":%q}`, checksum)
	code, body := postJSON(t, ts.URL+"/cachebox/preload/commit", commit)
	if code != 200 || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("preload commit: code=%d body=%s", code, body)
	}

	if code, body := postJSON(t, ts.URL+"/admin/rules", ruleJSON("replay")); code != http.StatusNoContent {
		t.Fatalf("install replay rule: code=%d body=%s", code, body)
	}

	resp, err := client.Get(target)
	if err != nil {
		t.Fatalf("frozen egress call: %v", err)
	}
	replayBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 || string(replayBody) != `{"replayed":true}` {
		t.Fatalf("replay hit: code=%d body=%s", resp.StatusCode, replayBody)
	}
	if resp.Header.Get("X-Atropos-Cache-Mode") != "replay" {
		t.Fatalf("X-Atropos-Cache-Mode = %q, want replay", resp.Header.Get("X-Atropos-Cache-Mode"))
	}
	if got := liveCalls.Load(); got != 0 {
		t.Fatalf("live calls under replay = %d, want 0", got)
	}
}

// TestServe_OneIdentityEverywhere pins invariant 5 against a stub control
// plane: the identity in the register payload, the cache-push ingest
// envelope, and the fidelity endpoint is one and the same string.
func TestServe_OneIdentityEverywhere(t *testing.T) {
	type captured struct {
		// mu guards the register fields: the stub answers the status report
		// 404, so the SDK registers again while the test reads them.
		mu          sync.Mutex
		registerID  string
		routes      int
		version     string
		ingestInst  atomic.Value // string
		deregisters atomic.Int32
	}
	var got captured

	manteion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/sdk/init":
			w.WriteHeader(200)
		case r.URL.Path == "/api/v1/sdk/register" && r.Method == http.MethodPost:
			var reg RegisterRequest
			_ = json.NewDecoder(r.Body).Decode(&reg)
			got.mu.Lock()
			got.registerID = reg.ID
			got.routes = len(reg.Routes)
			got.version = reg.Version
			got.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"status":"registered","version":0,"rules":[],"active_faults":[]}`)
		case strings.HasPrefix(r.URL.Path, "/api/v1/sdk/register/") && r.Method == http.MethodDelete:
			got.deregisters.Add(1)
			w.WriteHeader(200)
		case r.URL.Path == "/api/v1/sdk/rules":
			w.WriteHeader(http.StatusNotModified)
		case r.URL.Path == "/api/v1/cache/ingest":
			var env struct {
				Instance string `json:"instance"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &env)
			got.ingestInst.Store(env.Instance)
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(404)
		}
	}))
	defer manteion.Close()

	exporter := tracetest.NewInMemoryExporter()
	h, shutdown, err := Serve(context.Background(), Config{
		Service:     "identity-test",
		Version:     "0.3.7",
		InstanceID:  "pod-42",
		ManteionURL: manteion.URL,
		Routes:      []Route{{Method: "GET", Path: "/x"}},
		TracerProvider: sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(exporter),
		),
	})
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	ts := httptest.NewServer(h)
	defer ts.Close()

	got.mu.Lock()
	registerID, routes, version := got.registerID, got.routes, got.version
	got.mu.Unlock()
	if registerID != "pod-42" {
		t.Errorf("register id = %q, want pod-42", registerID)
	}
	if routes != 1 {
		t.Errorf("register routes = %d, want 1", routes)
	}
	if version != "0.3.7" {
		t.Errorf("register version = %q, want 0.3.7", version)
	}

	code, body := mustGet(t, ts.URL+"/atropos/health")
	if code != 200 || !strings.Contains(body, `"status":"connected"`) {
		t.Fatalf("health after connect: code=%d body=%s", code, body)
	}

	snap := fidelitySnapshot(t, ts.URL)
	if snap.InstanceID != "pod-42" {
		t.Errorf("fidelity instance_id = %q, want pod-42", snap.InstanceID)
	}
	if snap.Service != "identity-test" {
		t.Errorf("fidelity service = %q", snap.Service)
	}

	// Drive one recorded entry through the push client so the ingest
	// envelope's instance is observable.
	if code, _ := postJSON(t, ts.URL+"/admin/rules", ruleJSON("passthrough")); code != http.StatusNoContent {
		t.Fatal("install passthrough rule")
	}
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte("x")))}, nil
	})
	client := &http.Client{Transport: EgressTransport(base)}
	resp, err := client.Get("http://downstream.test/y")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Shutdown drains the recorder into the push client and flushes the
	// final batch synchronously — after it, the envelope must have arrived.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if inst, _ := got.ingestInst.Load().(string); inst != "pod-42" {
		t.Errorf("ingest envelope instance = %q, want pod-42", inst)
	}
	if got.deregisters.Load() != 1 {
		t.Errorf("deregisters = %d, want 1", got.deregisters.Load())
	}
	// Shutdown is idempotent.
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("second shutdown: %v", err)
	}
}

// TestServe_ManteionConfiguredButUnreachableFails pins the fail-hard choice:
// a configured-but-unreachable control plane is an error, not a silent
// offline run.
func TestServe_ManteionConfiguredButUnreachableFails(t *testing.T) {
	t.Setenv("MANTEION_INIT_TIMEOUT", "150ms")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exporter := tracetest.NewInMemoryExporter()
	_, _, err := Serve(ctx, Config{
		Service:        "unreachable-test",
		ManteionURL:    "http://127.0.0.1:1", // nothing listens here
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)),
	})
	if err == nil {
		t.Fatal("expected Serve to fail when MANTEION_URL is set but unreachable")
	}
}

// TestServe_SidecarModeIsPassive pins the mixed-fleet guard (design §5.12):
// when the pod's atroposd owns the instrument, the embed serves business
// traffic with tracing only — no registration (an unreachable MANTEION_URL
// is not even contacted), no rules, no control surface.
func TestServe_SidecarModeIsPassive(t *testing.T) {
	t.Setenv("ATROPOS_HOST", "sidecar")
	t.Setenv("MANTEION_URL", "http://127.0.0.1:9") // closed port: must not be dialed
	t.Setenv("MANTEION_INIT_TIMEOUT", "100ms")

	mux := http.NewServeMux()
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("done")) })
	exporter := tracetest.NewInMemoryExporter()
	h, shutdown, err := Serve(context.Background(), Config{
		Service: "passive-test", Handler: mux,
		TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample())),
	})
	if err != nil {
		t.Fatalf("Serve in sidecar mode must not fail on an unreachable control plane: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	if code, body := mustGet(t, ts.URL+"/work"); code != 200 || body != "done" {
		t.Fatalf("business handler: code=%d body=%q", code, body)
	}
	if len(exporter.GetSpans()) == 0 {
		t.Fatal("passive mode must still trace")
	}
	for _, p := range []string{"/admin/fault", "/admin/rules", "/admin/cachebox", "/cachebox/fidelity?experiment_id=e&phase_id=p", "/atropos/health"} {
		if code, _ := mustGet(t, ts.URL+p); code != 404 {
			t.Fatalf("%s = %d, want 404: the sidecar serves the control surface, not the embed", p, code)
		}
	}
	if hs := Health(); hs.Status != "offline" || hs.Host != "in-process" {
		t.Fatalf("Health() = %+v", hs)
	}
}
