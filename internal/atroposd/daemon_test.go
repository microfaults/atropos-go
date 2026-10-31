package atroposd

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/agent"
	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// runDaemon runs Run in the background, offline and hermetic, and returns
// the bound addresses once the daemon reports ready.
func runDaemon(t *testing.T, cfg Config) Addrs {
	t.Helper()
	t.Setenv("MANTEION_URL", "")
	t.Setenv("MANTEION_ADVERTISE_ADDR", "")
	ready := make(chan Addrs, 1)
	cfg.OnReady = func(a Addrs) { ready <- a }
	cfg.TracerProvider = noop.NewTracerProvider()
	if cfg.ControlListen == "" {
		cfg.ControlListen = "127.0.0.1:0"
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	select {
	case a := <-ready:
		return a
	case err := <-done:
		t.Fatalf("Run exited before ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("daemon not ready in time")
	}
	return Addrs{}
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

func getJSON(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func cacheBoxRule(mode string) string {
	return fmt.Sprintf(`[{"name":"cachebox-%s","injection_point":"egress","mode":"inline",
		"cachebox":{"mode":%q,"context":{"experiment_id":"exp-1","phase_id":"phase-1",
		"key_strategy":"canonical_v2","strategy_version":2}}}]`, mode, mode)
}

func fidelity(t *testing.T, control string) wire.FidelitySnapshot {
	t.Helper()
	code, body := getJSON(t, control+"/cachebox/fidelity?experiment_id=exp-1&phase_id=phase-1")
	if code != 200 {
		t.Fatalf("fidelity: code=%d", code)
	}
	var snap wire.FidelitySnapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

// TestRun_RecordFailClosedPreloadReplay is the M1 exit criterion through
// the sidecar, mirroring the in-process serve_test loop: a passthrough rule
// records the app's egress call; switching to replay without a committed
// preload fails closed (counted 503, upstream untouched); a staged preload
// with the W5 checksum installs the replay set; the frozen call then serves
// from cache without touching the upstream.
func TestRun_RecordFailClosedPreloadReplay(t *testing.T) {
	var liveCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"live":true}`))
	}))
	t.Cleanup(up.Close)
	upstream := strings.TrimPrefix(up.URL, "http://")

	addrs := runDaemon(t, Config{Service: "recommendationservice", Egress: []EgressRoute{{Listen: "127.0.0.1:0", Upstream: upstream}}})
	control := "http://" + addrs.Control
	egress := "http://" + addrs.Egress[0]
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	// Phase 1: record.
	if code, body := postJSON(t, control+"/admin/rules", cacheBoxRule("passthrough")); code != http.StatusNoContent {
		t.Fatalf("install passthrough rule: code=%d body=%s", code, body)
	}
	resp, err := client.Get(egress + "/api/items")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Atropos-Cache-Key") == "" {
		t.Fatalf("passthrough: status=%d key=%q", resp.StatusCode, resp.Header.Get("X-Atropos-Cache-Key"))
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls = %d, want 1", liveCalls.Load())
	}
	if snap := fidelity(t, control); snap.RecordEnqueued != 1 {
		t.Fatalf("record_enqueued = %d, want 1", snap.RecordEnqueued)
	}

	// Phase 2: freeze without a committed preload → fail closed.
	if code, body := postJSON(t, control+"/admin/rules", cacheBoxRule("replay")); code != http.StatusNoContent {
		t.Fatalf("install replay rule: code=%d body=%s", code, body)
	}
	resp, err = client.Get(egress + "/api/items")
	if err != nil {
		t.Fatal(err)
	}
	missBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("X-Atropos-Cache-Miss") != "1" || !strings.Contains(string(missBody), "cachebox_miss") {
		t.Fatalf("frozen miss: status=%d miss=%q body=%s", resp.StatusCode, resp.Header.Get("X-Atropos-Cache-Miss"), missBody)
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls after frozen miss = %d — a frozen service must NEVER call the upstream", liveCalls.Load())
	}
	if snap := fidelity(t, control); snap.ReplayMisses != 1 || snap.MissReasons.NotCommitted != 1 {
		t.Fatalf("fidelity after miss = %+v", snap)
	}

	// Phase 3: staged preload (W4/W5) keyed exactly as the sidecar keys the
	// app's request: the client-side view of GET http://<upstream>/api/items.
	u, _ := url.Parse("http://" + upstream + "/api/items")
	key := cachebox.Derive(cachebox.KeyStrategyCanonicalV2, nil, &http.Request{Method: "GET", URL: u, Header: http.Header{}}, nil)
	entry := &cachebox.Entry{
		Key: key, StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"replayed":true}`), ObservedLatency: 5 * time.Millisecond, RecordedAt: time.Now().UTC(),
		ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2,
	}
	checksum := cachebox.SetChecksum([]*cachebox.Entry{entry})
	begin := `{"experiment_id":"exp-1","phase_id":"phase-1","source_phase_id":"phase-0","total_entries":1,"total_chunks":1,"key_strategy":"canonical_v2","strategy_version":2}`
	if code, body := postJSON(t, control+"/cachebox/preload/begin", begin); code != 200 {
		t.Fatalf("preload begin: code=%d body=%s", code, body)
	}
	chunk, _ := json.Marshal(map[string]any{"experiment_id": "exp-1", "phase_id": "phase-1", "chunk_seq": 1,
		"entries": []wire.CacheBoxWireEntry{cachebox.EntryToWire(entry)}})
	if code, body := postJSON(t, control+"/cachebox/preload/chunk", string(chunk)); code != 200 {
		t.Fatalf("preload chunk: code=%d body=%s", code, body)
	}
	commit := fmt.Sprintf(`{"experiment_id":"exp-1","phase_id":"phase-1","total_entries":1,"checksum":%q}`, checksum)
	if code, body := postJSON(t, control+"/cachebox/preload/commit", commit); code != 200 || !strings.Contains(body, `"ok":true`) {
		t.Fatalf("preload commit: code=%d body=%s", code, body)
	}

	// Phase 4: replay hit, upstream still untouched.
	resp, err = client.Get(egress + "/api/items")
	if err != nil {
		t.Fatal(err)
	}
	replayBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(replayBody) != `{"replayed":true}` || resp.Header.Get("X-Atropos-Cache-Mode") != "replay" {
		t.Fatalf("replay hit: status=%d mode=%q body=%s", resp.StatusCode, resp.Header.Get("X-Atropos-Cache-Mode"), replayBody)
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls under replay = %d, want 1 (unchanged)", liveCalls.Load())
	}
	if snap := fidelity(t, control); snap.ReplayHits != 1 || !snap.Preload.Committed {
		t.Fatalf("fidelity after hit = %+v", snap)
	}
}

func TestRun_IngressLatencyThroughSidecar(t *testing.T) {
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("app")) }))
	t.Cleanup(app.Close)
	addrs := runDaemon(t, Config{Service: "frontend", IngressListen: "127.0.0.1:0", InboundUpstream: strings.TrimPrefix(app.URL, "http://")})
	control := "http://" + addrs.Control
	rule := `[{"name":"lat","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"latency","params":{"delay":"150ms"}}}]`
	if code, body := postJSON(t, control+"/admin/rules", rule); code != http.StatusNoContent {
		t.Fatalf("install rule: code=%d body=%s", code, body)
	}
	start := time.Now()
	code, body := getJSON(t, "http://"+addrs.Ingress+"/x")
	if code != 200 || body != "app" {
		t.Fatalf("code=%d body=%q", code, body)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("ingress latency not applied through the sidecar: %v", el)
	}
	if code, _ := getJSON(t, control+"/atropos/health"); code != 200 {
		t.Fatalf("health = %d", code)
	}
}

func TestRun_ManteionConfiguredButUnreachableFails(t *testing.T) {
	t.Setenv("MANTEION_INIT_TIMEOUT", "150ms")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := Run(ctx, Config{
		Service: "svc", ManteionURL: "http://127.0.0.1:9", ControlListen: "127.0.0.1:0",
		Egress: []EgressRoute{{Listen: "127.0.0.1:0", Upstream: "dep:80"}}, TracerProvider: noop.NewTracerProvider(),
	}, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "connect manteion") {
		t.Fatalf("expected a fail-closed startup error, got %v", err)
	}
}

func TestRun_RejectsBadConfig(t *testing.T) {
	err := Run(context.Background(), Config{Service: "", Capture: "explicit"}, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "service is required") {
		t.Fatalf("got %v", err)
	}
}

// With no BYO provider Run builds the SDK provider itself; its resource tells
// a backend the spans come from the sidecar.
func TestRun_ResourceNamesTheSidecarHost(t *testing.T) {
	for _, key := range []string{"MANTEION_URL", "MANTEION_ADVERTISE_ADDR", "COLLECTOR_SERVICE_ADDR", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_PROTOCOL"} {
		t.Setenv(key, "") // hermetic: no control plane, nothing for the exporter shim to map
	}
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	ready := make(chan Addrs, 1)
	stopped := make(chan struct{})
	var runErr error
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(stopped)
		runErr = Run(ctx, Config{
			Service: "svc", ControlListen: "127.0.0.1:0",
			Egress:  []EgressRoute{{Listen: "127.0.0.1:0", Upstream: "dep:80"}},
			OnReady: func(a Addrs) { ready <- a },
		}, slog.New(slog.DiscardHandler))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	select {
	case <-ready:
	case <-stopped:
		t.Fatalf("Run exited before ready: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("daemon not ready in time")
	}

	_, span := otel.Tracer("daemon-test").Start(context.Background(), "probe")
	defer span.End()
	ro, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		t.Fatalf("span %T is not an SDK span", span)
	}
	if v, ok := ro.Resource().Set().Value("atropos.host"); !ok || v.AsString() != "sidecar" {
		t.Fatalf("atropos.host = %v (present=%v), want sidecar", v, ok)
	}
}

func TestRun_TransparentRequiresCaptureMarker(t *testing.T) {
	t.Setenv("MANTEION_URL", "")
	err := Run(context.Background(), Config{
		Service: "svc", Capture: CaptureTransparent, ControlListen: "127.0.0.1:0", EgressListen: "127.0.0.1:0",
		CaptureMarker: filepath.Join(t.TempDir(), "capture.ok"), TracerProvider: noop.NewTracerProvider(),
	}, slog.New(slog.DiscardHandler))
	if err == nil {
		t.Fatal("a transparent sidecar without the capture marker must not start")
	}
	if runtime.GOOS == "linux" && !strings.Contains(err.Error(), "marker") {
		t.Fatalf("got %v, want the missing-marker error", err)
	}
	if runtime.GOOS != "linux" && !strings.Contains(err.Error(), "requires linux") {
		t.Fatalf("got %v, want the platform error", err)
	}
}

// TestRun_TransparentRecordThenReplay drives the sidecar in transparent
// mode with an injected original destination: the app "dials"
// productcatalogservice:3550 (its Host header), the redirect lands on the
// sidecar, the recording is keyed on that name, and the replay set built
// for the same client-side request serves the frozen call.
func TestRun_TransparentRecordThenReplay(t *testing.T) {
	var liveCalls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveCalls.Add(1)
		w.Write([]byte(`{"live":true}`))
	}))
	t.Cleanup(up.Close)
	upstream := strings.TrimPrefix(up.URL, "http://")

	addrs := runDaemon(t, Config{
		Service: "frontend", Capture: CaptureTransparent, EgressListen: "127.0.0.1:0",
		OriginalDst: func(net.Conn) (string, error) { return upstream, nil },
	})
	control := "http://" + addrs.Control
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	call := func() *http.Response {
		req, _ := http.NewRequest("GET", "http://"+addrs.TransparentEgress+"/api/items", nil)
		req.Host = "productcatalogservice:3550"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp
	}

	if code, body := postJSON(t, control+"/admin/rules", cacheBoxRule("passthrough")); code != http.StatusNoContent {
		t.Fatalf("install passthrough rule: code=%d body=%s", code, body)
	}
	if resp := call(); resp.StatusCode != 200 || resp.Header.Get("X-Atropos-Cache-Key") == "" {
		t.Fatalf("passthrough: status=%d", resp.StatusCode)
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls = %d", liveCalls.Load())
	}

	u, _ := url.Parse("http://productcatalogservice:3550/api/items")
	key := cachebox.Derive(cachebox.KeyStrategyCanonicalV2, nil, &http.Request{Method: "GET", URL: u, Header: http.Header{}}, nil)
	entry := &cachebox.Entry{Key: key, StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"replayed":true}`), ObservedLatency: time.Millisecond, RecordedAt: time.Now().UTC(),
		ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2}
	checksum := cachebox.SetChecksum([]*cachebox.Entry{entry})
	postJSON(t, control+"/cachebox/preload/begin", `{"experiment_id":"exp-1","phase_id":"phase-1","source_phase_id":"phase-0","total_entries":1,"total_chunks":1,"key_strategy":"canonical_v2","strategy_version":2}`)
	chunk, _ := json.Marshal(map[string]any{"experiment_id": "exp-1", "phase_id": "phase-1", "chunk_seq": 1, "entries": []wire.CacheBoxWireEntry{cachebox.EntryToWire(entry)}})
	postJSON(t, control+"/cachebox/preload/chunk", string(chunk))
	if code, body := postJSON(t, control+"/cachebox/preload/commit", fmt.Sprintf(`{"experiment_id":"exp-1","phase_id":"phase-1","total_entries":1,"checksum":%q}`, checksum)); code != 200 {
		t.Fatalf("commit: code=%d body=%s", code, body)
	}
	if code, body := postJSON(t, control+"/admin/rules", cacheBoxRule("replay")); code != http.StatusNoContent {
		t.Fatalf("install replay rule: code=%d body=%s", code, body)
	}
	if resp := call(); resp.StatusCode != 200 || resp.Header.Get("X-Atropos-Cache-Mode") != "replay" {
		t.Fatalf("replay hit: status=%d mode=%q", resp.StatusCode, resp.Header.Get("X-Atropos-Cache-Mode"))
	}
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls under replay = %d, want 1", liveCalls.Load())
	}
	if code, body := getJSON(t, control+"/atropos/health"); code != 200 || !strings.Contains(body, `"capture":"transparent"`) || !strings.Contains(body, `"host":"sidecar"`) {
		t.Fatalf("health: code=%d body=%s", code, body)
	}
}

// TestRun_DelegatesResourceFaultsToAgent: with the node agent reachable, a
// resource rule applied to the sidecar arms eagerly through the agent —
// here a cpu quota on the app container's cgroup — and rule removal
// restores it.
func TestRun_DelegatesResourceFaultsToAgent(t *testing.T) {
	cg := t.TempDir()
	for name, content := range map[string]string{"cpu.max": "max 100000\n", "memory.max": "max\n", "memory.high": "max\n", "cgroup.procs": ""} {
		if err := os.WriteFile(filepath.Join(cg, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var sb [4]byte
	rand.Read(sb[:])
	sock := filepath.Join(os.TempDir(), "atropos-agent-"+hex.EncodeToString(sb[:])+".sock")
	t.Cleanup(func() { os.Remove(sock) })
	srv := &agent.Server{
		Resolver: agent.ResolverFunc(func(podUID, container string) (agent.Target, error) {
			if podUID != "pod-x" || container != "server" {
				return agent.Target{}, os.ErrNotExist
			}
			return agent.Target{PodUID: podUID, Container: container, ContainerID: "c1", CgroupDir: cg}, nil
		}),
		Journal: agent.Journal{Dir: filepath.Join(t.TempDir(), "journal")},
		Self:    os.Args[0],
		Peer:    func(net.Conn) (string, error) { return "pod-x", nil },
		Host:    agent.Capacity{CPUs: 1, MemoryBytes: 1 << 30},
		Logger:  slog.New(slog.DiscardHandler),
	}
	ln, err := agent.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	actx, acancel := context.WithCancel(context.Background())
	agentDone := make(chan struct{})
	go func() { defer close(agentDone); _ = srv.Serve(actx, ln) }()
	t.Cleanup(func() { acancel(); <-agentDone })

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(up.Close)
	addrs := runDaemon(t, Config{
		Service: "cartservice", Egress: []EgressRoute{{Listen: "127.0.0.1:0", Upstream: strings.TrimPrefix(up.URL, "http://")}},
		AgentSocket: sock, PodUID: "pod-x", AppContainer: "server",
	})
	control := "http://" + addrs.Control

	rule := `[{"name":"cpu-quota","injection_point":"ingress","mode":"background",
		"fault":{"category":"resource","fault_type":"cpu","duration_ms":10000,"params":{"target_load":0.5,"mechanism":"quota"}}}]`
	if code, body := postJSON(t, control+"/admin/rules", rule); code != http.StatusNoContent {
		t.Fatalf("install rule: code=%d body=%s", code, body)
	}
	read := func() string { b, _ := os.ReadFile(filepath.Join(cg, "cpu.max")); return strings.TrimSpace(string(b)) }
	deadline := time.Now().Add(5 * time.Second)
	for read() != "50000 100000" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := read(); got != "50000 100000" {
		t.Fatalf("cpu.max after eager arming through the agent = %q, want 50000 100000", got)
	}

	if code, body := postJSON(t, control+"/admin/rules", `[]`); code != http.StatusNoContent {
		t.Fatalf("clear rules: code=%d body=%s", code, body)
	}
	deadline = time.Now().Add(5 * time.Second)
	for read() != "max 100000" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := read(); got != "max 100000" {
		t.Fatalf("cpu.max after rule removal = %q, want restored", got)
	}
}

// polled is one rules poll the fake manteion saw: what it asked for, whether
// it was held, and the answer.
type polled struct {
	Version, IfNoneMatch, Wait, InstanceID string
	Held                                   bool // parked until a bump or the end of the wait
	Code                                   int  // 0 until answered
}

// desiredStateManteion is a fake manteion speaking desired-state v1. Register
// answers the materialized document; the rules poll is conditional and held
// the way manteion's is (a poll with a wait whose If-None-Match names the
// document is parked until bump or the wait elapses, then answered from the
// document as it is then: 304 when the ETag still matches, else 200); status
// reports are recorded; the replay-set artifact is served with Range support
// once release is called.
type desiredStateManteion struct {
	srv      *httptest.Server
	artifact []byte
	gate     chan struct{}
	once     sync.Once

	mu           sync.Mutex
	doc          wire.RuleSync
	changed      chan struct{} // closed by the next bump
	registers    []wire.RegisterRequest
	polls        []polled
	statuses     []wire.StatusReport
	artifactGets int
}

func newDesiredStateManteion(t *testing.T, doc wire.RuleSync, artifact []byte) *desiredStateManteion {
	t.Helper()
	m := &desiredStateManteion{doc: doc, changed: make(chan struct{}), artifact: artifact, gate: make(chan struct{})}
	m.srv = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.srv.Close)
	t.Cleanup(m.release) // ahead of srv.Close, which waits for a gated handler
	return m
}

func (m *desiredStateManteion) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/api/v1/sdk/init":
		w.WriteHeader(http.StatusOK)
	case r.URL.Path == "/api/v1/sdk/register" && r.Method == http.MethodPost:
		var reg wire.RegisterRequest
		_ = json.NewDecoder(r.Body).Decode(&reg)
		m.mu.Lock()
		m.registers = append(m.registers, reg)
		body, err := json.Marshal(wire.RegisterResponse{Status: "registered", RuleSync: m.doc})
		m.mu.Unlock()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write(body)
	case strings.HasPrefix(r.URL.Path, "/api/v1/sdk/register/") && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusOK)
	case r.URL.Path == "/api/v1/sdk/rules":
		m.poll(w, r)
	case r.URL.Path == "/api/v1/sdk/status" && r.Method == http.MethodPost:
		var rep wire.StatusReport
		_ = json.NewDecoder(r.Body).Decode(&rep)
		m.mu.Lock()
		m.statuses = append(m.statuses, rep)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, "/api/v1/cache/artifacts/"):
		m.mu.Lock()
		m.artifactGets++
		m.mu.Unlock()
		select {
		case <-m.gate:
		case <-r.Context().Done():
			return
		}
		http.ServeContent(w, r, "replay-set.ndjson", time.Time{}, bytes.NewReader(m.artifact))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (m *desiredStateManteion) poll(w http.ResponseWriter, r *http.Request) {
	q, inm := r.URL.Query(), r.Header.Get("If-None-Match")
	m.mu.Lock()
	i := len(m.polls)
	m.polls = append(m.polls, polled{Version: q.Get("version"), IfNoneMatch: inm, Wait: q.Get("wait"), InstanceID: q.Get("instance_id")})
	doc, changed := m.doc, m.changed
	m.mu.Unlock()
	if wait, _ := strconv.Atoi(q.Get("wait")); wait > 0 && inm == `"`+doc.Etag+`"` {
		m.mu.Lock()
		m.polls[i].Held = true
		m.mu.Unlock()
		select {
		case <-changed:
		case <-time.After(time.Duration(wait) * time.Second):
		case <-r.Context().Done():
			return
		}
		m.mu.Lock()
		doc = m.doc
		m.mu.Unlock()
	}
	code := http.StatusOK
	if inm == `"`+doc.Etag+`"` {
		code = http.StatusNotModified
	}
	m.mu.Lock()
	m.polls[i].Code = code
	m.mu.Unlock()
	w.Header().Set("ETag", `"`+doc.Etag+`"`)
	w.Header().Set("X-Atropos-Generation", strconv.FormatUint(doc.Version, 10))
	if code == http.StatusNotModified {
		w.WriteHeader(code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}

// bump makes doc the desired state and wakes every held poll, as a generation
// bump does.
func (m *desiredStateManteion) bump(doc wire.RuleSync) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doc = doc
	close(m.changed)
	m.changed = make(chan struct{})
}

// release starts serving the replay-set artifact.
func (m *desiredStateManteion) release() { m.once.Do(func() { close(m.gate) }) }

// snapshot copies what the fake has seen.
func (m *desiredStateManteion) snapshot() (polls []polled, statuses []wire.StatusReport, registers []wire.RegisterRequest, artifactGets int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]polled(nil), m.polls...), append([]wire.StatusReport(nil), m.statuses...),
		append([]wire.RegisterRequest(nil), m.registers...), m.artifactGets
}

// waitFor waits up to 5 s for cond to hold over what the fake has seen.
func (m *desiredStateManteion) waitFor(t *testing.T, what string, cond func(polls []polled, statuses []wire.StatusReport) bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; {
		polls, statuses, _, _ := m.snapshot()
		if cond(polls, statuses) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: polls %+v, status reports %s", what, polls, asJSON(statuses))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// asJSON renders v for a failure message (status reports hold pointers).
func asJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return string(b)
}

// TestRun_ConvergesWithManteionDesiredState drives the sidecar against a fake
// manteion speaking desired-state v1. The register answer is the
// materialized state -- generation 42, a 150ms ingress latency rule and a
// replay set -- applied through the shared apply path before the sidecar is
// ready, and reported with the replay set fetching; the set is fetched from
// manteion's artifact endpoint, installed and reported committed. The loop
// long-polls conditionally on what it holds: a generation bump answers the
// held poll with the new document, whose rules replace the old ones, and the
// sidecar reports generation 43; a bump that leaves its document as it was
// answers with a 304, whose generation it adopts without applying or
// reporting anything.
func TestRun_ConvergesWithManteionDesiredState(t *testing.T) {
	entry := &cachebox.Entry{
		Key: "v2:items", StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
		Body: []byte(`{"replayed":true}`), ObservedLatency: 5 * time.Millisecond, RecordedAt: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC),
		ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2,
	}
	line, err := json.Marshal(cachebox.EntryToWire(entry))
	if err != nil {
		t.Fatal(err)
	}
	artifact := append(line, '\n')
	digest := sha256.Sum256(artifact)
	ref := &wire.ReplaySetRef{
		ID: "sha256:" + hex.EncodeToString(digest[:]), ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2",
		Count: 1, SHA256: cachebox.SetChecksum([]*cachebox.Entry{entry}), SizeBytes: int64(len(artifact)),
		URL: "/api/v1/cache/artifacts/" + hex.EncodeToString(digest[:]),
	}
	compiled := func(rules string) []wire.CompiledRule {
		var out []wire.CompiledRule
		if err := json.Unmarshal([]byte(rules), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	latency := compiled(`[{"name":"lat","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"latency","params":{"delay":"150ms"}}}]`)
	teapot := compiled(`[{"name":"teapot","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"error","params":{"status_code":418}}}]`)

	m := newDesiredStateManteion(t, wire.RuleSync{Version: 42, Etag: "42-aaaa", Rules: latency, ActiveFaults: []wire.FaultRequest{}, ReplaySet: ref}, artifact)
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("app")) }))
	t.Cleanup(app.Close)
	addrs := runDaemon(t, Config{Service: "frontend", ManteionURL: m.srv.URL, IngressListen: "127.0.0.1:0", InboundUpstream: strings.TrimPrefix(app.URL, "http://")})
	control, ingress := "http://"+addrs.Control, "http://"+addrs.Ingress+"/x"
	health := func(want ...string) {
		t.Helper()
		code, body := getJSON(t, control+"/atropos/health")
		for _, w := range want {
			if code != 200 || !strings.Contains(body, w) {
				t.Fatalf("health: code=%d body=%s, want %s", code, body, w)
			}
		}
	}

	// Register: the materialized state is applied before the sidecar is
	// ready, and reported with the replay set still fetching (manteion has
	// not served the artifact yet).
	start := time.Now()
	if code, body := getJSON(t, ingress); code != 200 || body != "app" {
		t.Fatalf("ingress: code=%d body=%q", code, body)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("the register-time rule was not applied through the shared apply path: %v", el)
	}
	_, statuses, registers, _ := m.snapshot()
	if len(registers) != 1 || registers[0].PollIntervalMs != 30000 {
		t.Fatalf("registers = %+v, want one advertising the 30s long-poll cadence", registers)
	}
	report := func(gen uint64, etag, replaySetState string) wire.StatusReport {
		return wire.StatusReport{InstanceID: registers[0].ID, Service: "frontend", AppliedGeneration: gen, Etag: etag,
			Unsupported: []wire.UnsupportedRule{}, Errors: []string{}, ReplaySet: &wire.ReplaySetStatus{ID: ref.ID, State: replaySetState}}
	}
	if want := []wire.StatusReport{report(42, "42-aaaa", wire.ReplaySetFetching)}; !reflect.DeepEqual(statuses, want) {
		t.Fatalf("status reports after register = %s, want %s", asJSON(statuses), asJSON(want))
	}

	// The startup fetch is conditional on 42 without a hold; the loop's poll
	// is conditional on 42 and held. Meanwhile manteion serves the artifact:
	// the replay set is installed and its commit reported.
	m.waitFor(t, "the loop's poll held on 42-aaaa", func(p []polled, _ []wire.StatusReport) bool { return len(p) >= 2 && p[1].Held })
	polls, _, _, _ := m.snapshot()
	if got, want := polls[0], (polled{Version: "42", IfNoneMatch: `"42-aaaa"`, InstanceID: registers[0].ID, Code: http.StatusNotModified}); got != want {
		t.Fatalf("startup fetch = %+v, want %+v", got, want)
	}
	if got, want := polls[1], (polled{Version: "42", IfNoneMatch: `"42-aaaa"`, Wait: "30", InstanceID: registers[0].ID, Held: true}); got != want {
		t.Fatalf("loop poll = %+v, want %+v", got, want)
	}
	m.release()
	m.waitFor(t, "the committed report", func(_ []polled, s []wire.StatusReport) bool { return len(s) >= 2 })
	_, statuses, _, _ = m.snapshot()
	if want := report(42, "42-aaaa", wire.ReplaySetCommitted); !reflect.DeepEqual(statuses[1], want) {
		t.Fatalf("status report after the fetch = %s, want %s", asJSON(statuses[1]), asJSON(want))
	}
	if snap := fidelity(t, control); !snap.Preload.Committed || snap.Preload.Entries != 1 || snap.Preload.Checksum != ref.SHA256 {
		t.Fatalf("preload state for (exp-1, phase-1) = %+v, want the replay set committed", snap.Preload)
	}
	health(`"status":"connected"`, `"rule_version":42`, `"rule_count":1`)

	// A generation bump answers the held poll with the new document: its
	// rules replace the old ones, the replay set stays committed without a
	// second fetch, generation 43 is reported, and the loop goes straight
	// back into a poll held on 43.
	m.bump(wire.RuleSync{Version: 43, Etag: "43-bbbb", Rules: teapot, ActiveFaults: []wire.FaultRequest{}, ReplaySet: ref})
	m.waitFor(t, "the report of 43 and a poll held on it", func(p []polled, s []wire.StatusReport) bool {
		return len(s) >= 3 && len(p) >= 3 && p[2].Held
	})
	polls, statuses, _, artifactGets := m.snapshot()
	if got := polls[1]; got.Code != http.StatusOK {
		t.Fatalf("poll held on 42 = %+v, want it answered 200 by the bump", got)
	}
	if want := report(43, "43-bbbb", wire.ReplaySetCommitted); len(statuses) != 3 || !reflect.DeepEqual(statuses[2], want) {
		t.Fatalf("status reports after the bump = %s, want a third, %s", asJSON(statuses), asJSON(want))
	}
	if got, want := polls[2], (polled{Version: "43", IfNoneMatch: `"43-bbbb"`, Wait: "30", InstanceID: registers[0].ID, Held: true}); got != want {
		t.Fatalf("poll after the bump = %+v, want %+v", got, want)
	}
	if artifactGets != 1 {
		t.Fatalf("artifact fetched %d times, want once: the committed set is kept", artifactGets)
	}
	if code, _ := getJSON(t, ingress); code != http.StatusTeapot {
		t.Fatalf("ingress after the bump = %d, want 418 from the new rule set", code)
	}
	health(`"rule_version":43`, `"rule_count":1`)

	// A bump that leaves this instance's document as it was (the same ETag)
	// answers the held poll with a 304: generation 44 is adopted without an
	// apply or a report, and the next poll is conditional on it.
	m.bump(wire.RuleSync{Version: 44, Etag: "43-bbbb", Rules: teapot, ActiveFaults: []wire.FaultRequest{}, ReplaySet: ref})
	m.waitFor(t, "a poll held on 44", func(p []polled, _ []wire.StatusReport) bool { return len(p) >= 4 && p[3].Held })
	polls, statuses, _, _ = m.snapshot()
	if got := polls[2]; got.Code != http.StatusNotModified {
		t.Fatalf("poll held on 43 = %+v, want it answered 304 by the bump", got)
	}
	if got, want := polls[3], (polled{Version: "44", IfNoneMatch: `"43-bbbb"`, Wait: "30", InstanceID: registers[0].ID, Held: true}); got != want {
		t.Fatalf("poll after the 304 = %+v, want %+v", got, want)
	}
	if len(statuses) != 3 {
		t.Fatalf("status reports after the 304 = %s, want none added: a 304 applies nothing", asJSON(statuses))
	}
	if code, _ := getJSON(t, ingress); code != http.StatusTeapot {
		t.Fatalf("ingress after the 304 = %d, want the rules kept", code)
	}
	health(`"rule_version":44`, `"rule_count":1`)
}
