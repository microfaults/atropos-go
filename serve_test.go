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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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
			got.registerID = reg.ID
			got.routes = len(reg.Routes)
			got.version = reg.Version
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

	if got.registerID != "pod-42" {
		t.Errorf("register id = %q, want pod-42", got.registerID)
	}
	if got.routes != 1 {
		t.Errorf("register routes = %d, want 1", got.routes)
	}
	if got.version != "0.3.7" {
		t.Errorf("register version = %q, want 0.3.7", got.version)
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
