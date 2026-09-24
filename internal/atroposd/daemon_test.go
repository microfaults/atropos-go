package atroposd

import (
	"context"
	"crypto/rand"
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
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/agent"
	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

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
