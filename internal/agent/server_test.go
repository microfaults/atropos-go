package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func shortSocket(t *testing.T) string {
	t.Helper()
	var b [4]byte
	rand.Read(b[:])
	p := filepath.Join(os.TempDir(), "atropos-agent-"+hex.EncodeToString(b[:])+".sock")
	t.Cleanup(func() { os.Remove(p) })
	return p
}

type testAgent struct {
	srv    *Server
	sock   string
	client *http.Client
	dir    string // target cgroup dir
}

func startTestAgent(t *testing.T, peer string) *testAgent {
	t.Helper()
	dir := cgroupDir(t, map[string]string{"cpu.max": "max 100000\n", "memory.max": "1073741824\n", "memory.high": "max\n", "cgroup.procs": ""})
	srv := &Server{
		Resolver: ResolverFunc(func(podUID, container string) (Target, error) {
			if container != "server" {
				return Target{}, os.ErrNotExist
			}
			return Target{PodUID: podUID, Container: container, ContainerID: "c1", CgroupDir: dir}, nil
		}),
		Journal: Journal{Dir: filepath.Join(t.TempDir(), "journal")},
		Self:    os.Args[0],
		Peer:    func(net.Conn) (string, error) { return peer, nil },
		Host:    Capacity{CPUs: 1, MemoryBytes: 1 << 30},
		Logger:  slog.New(slog.DiscardHandler),
	}
	sock := shortSocket(t)
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("agent server did not stop")
		}
	})
	return &testAgent{srv: srv, sock: sock, client: socketClient(sock), dir: dir}
}

func (a *testAgent) post(t *testing.T, ctx context.Context, req StartRequest) (*http.Response, string) {
	t.Helper()
	body, _ := json.Marshal(req)
	hr, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://agent/v1/faults", bytes.NewReader(body))
	resp, err := a.client.Do(hr)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(b)
	}
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no id line")
	}
	var sr StartResponse
	json.Unmarshal(sc.Bytes(), &sr)
	return resp, sr.ID
}

func quotaReq(pod string, load float64, durationMs int64) StartRequest {
	return StartRequest{PodUID: pod, Container: "server", Name: "cpu-quota", Fault: wire.FaultRequest{
		Category: "resource", FaultType: "cpu", DurationMs: durationMs,
		Params: json.RawMessage(`{"target_load":` + strconvF(load) + `,"mechanism":"quota"}`)}}
}

func strconvF(f float64) string { b, _ := json.Marshal(f); return string(b) }

func TestServer_RefusesAnotherPod(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	resp, body := a.post(t, context.Background(), quotaReq("pod-b", 0.5, 1000))
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(body, "own pod") {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("a refused request must not touch the cgroup: %q", got)
	}
}

func TestServer_QuotaAppliesAndRestoresOnDelete(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	resp, id := a.post(t, context.Background(), quotaReq("pod-a", 0.5, 10000))
	defer resp.Body.Close()
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "50000 100000" {
		t.Fatalf("cpu.max while armed = %q", got)
	}
	if qs, _ := a.srv.Journal.Load(); len(qs) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(qs))
	}

	lr, _ := http.NewRequest(http.MethodGet, "http://agent/v1/faults", nil)
	lresp, _ := a.client.Do(lr)
	var st []Status
	json.NewDecoder(lresp.Body).Decode(&st)
	lresp.Body.Close()
	if len(st) != 1 || st[0].ID != id || st[0].Mechanism != "quota" {
		t.Fatalf("list = %+v", st)
	}

	dr, _ := http.NewRequest(http.MethodDelete, "http://agent/v1/faults/"+id, nil)
	dresp, err := a.client.Do(dr)
	if err != nil || dresp.StatusCode != 200 {
		t.Fatalf("delete: %v %v", err, dresp)
	}
	dresp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line after stop")
	}
	var res Result
	json.Unmarshal(sc.Bytes(), &res)
	if res.Error != "" {
		t.Fatalf("result = %+v", res)
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max after stop = %q", got)
	}
	if qs, _ := a.srv.Journal.Load(); len(qs) != 0 {
		t.Fatal("journal must be empty after restore")
	}
}

func TestServer_QuotaRestoresWhenTheSidecarDisconnects(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	ctx, cancel := context.WithCancel(context.Background())
	resp, _ := a.post(t, ctx, quotaReq("pod-a", 0.5, 10000))
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "50000 100000" {
		t.Fatalf("cpu.max while armed = %q", got)
	}
	cancel() // the sidecar is gone
	resp.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for readTrim(t, filepath.Join(a.dir, "cpu.max")) != "max 100000" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("quota must be restored when the caller disappears, got %q", got)
	}
}

func TestServer_QuotaExpiresByDuration(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	resp, _ := a.post(t, context.Background(), quotaReq("pod-a", 0.5, 200))
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line")
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max after expiry = %q", got)
	}
}

func TestServer_BurnRunsInTargetCgroup(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	req := StartRequest{PodUID: "pod-a", Container: "server", Name: "cpu-burn", Fault: cpuBurn(200)}
	resp, id := a.post(t, context.Background(), req)
	defer resp.Body.Close()
	if id == "" {
		t.Fatal("no id")
	}
	if strings.TrimSpace(readTrim(t, filepath.Join(a.dir, "cgroup.procs"))) == "" {
		t.Fatal("the stress child must be attached to the target cgroup")
	}
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line")
	}
	var res Result
	json.Unmarshal(sc.Bytes(), &res)
	if res.Error != "" || res.ActualDurationMs < 150 {
		t.Fatalf("result = %+v", res)
	}
}

func TestServer_RejectsBadRequests(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	cases := []StartRequest{
		{PodUID: "pod-a", Container: "server", Fault: wire.FaultRequest{Category: "inline", FaultType: "latency", DurationMs: 100}},
		{PodUID: "pod-a", Container: "server", Fault: wire.FaultRequest{Category: "resource", FaultType: "cpu", DurationMs: 0}},
		{PodUID: "pod-a", Container: "nope", Fault: cpuBurn(100)},
		{PodUID: "pod-a", Container: "server", Fault: wire.FaultRequest{Category: "resource", FaultType: "io", DurationMs: 100, Params: json.RawMessage(`{"mechanism":"quota"}`)}},
	}
	for i, c := range cases {
		resp, body := a.post(t, context.Background(), c)
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("case %d must be rejected, got 200 (%s)", i, body)
		}
	}
}
