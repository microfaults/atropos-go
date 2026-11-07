package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	runner *recRunner
	netns  string // the fake /proc's netns path of pid 4242
	cancel context.CancelFunc
	served <-chan struct{} // closed when Serve has returned
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
	nt, rec, netns, _ := newTestNetem(t)
	nt.Logger = srv.Logger
	srv.Netem = nt
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
	return &testAgent{srv: srv, sock: sock, client: socketClient(sock), dir: dir, runner: rec, netns: netns, cancel: cancel, served: done}
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

// seedPID makes the target cgroup name pid 4242, the pid newTestNetem's fake /proc knows.
func (a *testAgent) seedPID(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(a.dir, "cgroup.procs"), []byte("4242\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func netemReq(pod, faultType, target, direction, params string, durationMs int64) StartRequest {
	return StartRequest{PodUID: pod, Container: "server", Name: "netem-" + faultType, Fault: wire.FaultRequest{
		Category: "network", FaultType: faultType, DurationMs: durationMs,
		Network: &wire.NetworkEnvelope{Host: "inline", Target: target, Direction: direction},
		Params:  json.RawMessage(params)}}
}

func TestServer_CapabilitiesEndpoint(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	get := func() string {
		r, _ := http.NewRequest(http.MethodGet, "http://agent/v1/capabilities", nil)
		resp, err := a.client.Do(r)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("GET /v1/capabilities: %v %v", err, resp)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return strings.TrimSpace(string(b))
	}
	if got := get(); got != `{"mechanisms":["burn","quota","netem"]}` {
		t.Fatalf("capabilities = %s", got)
	}
	a.srv.Netem = nil
	if got := get(); got != `{"mechanisms":["burn","quota"]}` {
		t.Fatalf("capabilities without netem = %s", got)
	}
}

func TestServer_NetemAppliesTargetedPlanAndRestoresOnDelete(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, id := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "productcatalogservice:3550", "upstream", `{"loss":10,"correlation":25}`, 10000))
	defer resp.Body.Close()
	if id == "" {
		t.Fatal("no id")
	}
	ns := "--net=" + a.netns
	want := []string{
		"nsenter " + ns + " ip -json link show",
		"nsenter " + ns + " tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"nsenter " + ns + " tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"nsenter " + ns + " tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
	}
	if got := a.runner.lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 1 || es[0].ID != id || es[0].PodUID != "pod-a" {
		t.Fatalf("journal = %+v", es)
	}
	lr, _ := http.NewRequest(http.MethodGet, "http://agent/v1/faults", nil)
	lresp, _ := a.client.Do(lr)
	var st []Status
	json.NewDecoder(lresp.Body).Decode(&st)
	lresp.Body.Close()
	if len(st) != 1 || st[0].ID != id || st[0].Category != "network" || st[0].FaultType != "packet_loss" || st[0].Mechanism != "netem" || st[0].Dev != "eth0" || st[0].Netns != a.netns {
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
	if l := a.runner.lines(); l[len(l)-1] != "nsenter "+ns+" tc qdisc del dev eth0 root" {
		t.Fatalf("last command = %q, want the root qdisc deleted", l[len(l)-1])
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after restore")
	}
}

func TestServer_NetemLatencyWholePodExpiresByDuration(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, _ := a.post(t, context.Background(), netemReq("pod-a", "latency", "", "", `{"delay":"100ms","jitter":"20ms","mechanism":"netem"}`, 200))
	defer resp.Body.Close()
	if !a.runner.has("tc qdisc add dev eth0 root handle 1: netem delay 100ms 20ms") {
		t.Fatalf("commands = %q", a.runner.lines())
	}
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line")
	}
	if !a.runner.has("tc qdisc del dev eth0 root") {
		t.Fatal("expiry must delete the qdisc")
	}
}

func TestServer_NetemRestoresWhenTheSidecarDisconnects(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	ctx, cancel := context.WithCancel(context.Background())
	resp, _ := a.post(t, ctx, netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000))
	cancel() // the sidecar is gone
	resp.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !a.runner.has("tc qdisc del dev eth0 root") && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !a.runner.has("tc qdisc del dev eth0 root") {
		t.Fatal("netem must be restored when the caller disappears")
	}
}

func TestServer_NetemRefusesASecondFaultInTheSameNamespace(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, _ := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000))
	defer resp.Body.Close()
	before := len(a.runner.lines())
	resp2, body := a.post(t, context.Background(), netemReq("pod-a", "latency", "", "", `{"delay":"1ms","mechanism":"netem"}`, 10000))
	if resp2.StatusCode != http.StatusConflict || !strings.Contains(body, "already applied") {
		t.Fatalf("second netem: status=%d body=%q", resp2.StatusCode, body)
	}
	if len(a.runner.lines()) != before {
		t.Fatal("a refused request must not touch the namespace")
	}
}

func TestServer_NetemRejectsBadRequests(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	cases := []struct {
		name string
		req  StartRequest
		code int
	}{
		{"latency without mechanism is a toxic, not an agent fault", netemReq("pod-a", "latency", "", "", `{"delay":"1ms"}`, 1000), http.StatusBadRequest},
		{"downstream is ingress", netemReq("pod-a", "packet_loss", "x", "downstream", `{"loss":10}`, 1000), http.StatusBadRequest},
		{"duration required", netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 0), http.StatusBadRequest},
		{"another pod", netemReq("pod-b", "packet_loss", "", "", `{"loss":10}`, 1000), http.StatusForbidden},
	}
	for _, c := range cases {
		resp, body := a.post(t, context.Background(), c.req)
		if resp.StatusCode != c.code {
			t.Errorf("%s: status=%d body=%q, want %d", c.name, resp.StatusCode, body, c.code)
		}
	}
	if len(a.runner.lines()) != 0 {
		t.Fatalf("rejected requests must run nothing, ran %q", a.runner.lines())
	}
	a.srv.Netem = nil
	if resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 1000)); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "not available") {
		t.Fatalf("netem without tc: status=%d body=%q", resp.StatusCode, body)
	}
}

// A start whose rollback del fails keeps its journal entry and the namespace
// reservation (the qdisc may still be applied): the fault is not listed, and
// DELETE /v1/faults/{id} finds the entry through the journal and retries the
// restore, which runs the del and frees the namespace.
func TestServer_NetemKeptEntryAfterFailedRollbackIsRestoredThroughTheJournal(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	a.runner.mu.Lock()
	a.runner.fail["tc filter add"] = errors.New("exit status 2")
	a.runner.fail["tc qdisc del"] = errors.New("exit status 3")
	a.runner.mu.Unlock()
	resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "productcatalogservice:3550", "upstream", `{"loss":10}`, 10000))
	if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(body, "rollback: exit status 3") {
		t.Fatalf("start: status=%d body=%q, want 500 with the rollback failure", resp.StatusCode, body)
	}
	lr, _ := http.NewRequest(http.MethodGet, "http://agent/v1/faults", nil)
	lresp, _ := a.client.Do(lr)
	var st []Status
	json.NewDecoder(lresp.Body).Decode(&st)
	lresp.Body.Close()
	if len(st) != 0 {
		t.Fatalf("a failed start must not be listed: %+v", st)
	}
	es, _ := a.srv.Netem.Journal.Load()
	if len(es) != 1 {
		t.Fatalf("journal = %+v, want the kept entry", es)
	}
	id := es[0].ID
	// The namespace stays reserved while the entry is kept.
	if resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000)); resp.StatusCode != http.StatusConflict {
		t.Fatalf("netem while the entry is kept: status=%d body=%q, want 409", resp.StatusCode, body)
	}

	// The del keeps failing: DELETE answers 500 and the entry stays.
	dr, _ := http.NewRequest(http.MethodDelete, "http://agent/v1/faults/"+id, nil)
	dresp, err := a.client.Do(dr)
	if err != nil || dresp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("delete with a failing del: %v %v", err, dresp)
	}
	dresp.Body.Close()
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 1 {
		t.Fatalf("journal = %+v, want the entry kept after a failed restore", es)
	}

	a.runner.mu.Lock()
	delete(a.runner.fail, "tc qdisc del")
	delete(a.runner.fail, "tc filter add")
	a.runner.mu.Unlock()
	before := len(a.runner.lines())
	dr, _ = http.NewRequest(http.MethodDelete, "http://agent/v1/faults/"+id, nil)
	dresp, err = a.client.Do(dr)
	if err != nil || dresp.StatusCode != http.StatusOK {
		t.Fatalf("delete through the journal: %v %v", err, dresp)
	}
	dresp.Body.Close()
	if l := a.runner.lines(); len(l) != before+2 || l[before] != "nsenter --net="+a.netns+" tc qdisc show dev eth0 root" || l[before+1] != "nsenter --net="+a.netns+" tc qdisc del dev eth0 root" {
		t.Fatalf("commands after the delete = %q, want the probe, then the root qdisc deleted", l[before:])
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 0 {
		t.Fatalf("journal = %+v, want empty after the restore", es)
	}
	resp, id2 := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || id2 == "" {
		t.Fatalf("netem after the restore: status=%d, want the namespace free", resp.StatusCode)
	}
	// An id neither running nor journaled is still unknown.
	dr, _ = http.NewRequest(http.MethodDelete, "http://agent/v1/faults/nope", nil)
	if dresp, err := a.client.Do(dr); err != nil || dresp.StatusCode != http.StatusNotFound {
		t.Fatalf("delete of an unknown id: %v %v", err, dresp)
	} else {
		dresp.Body.Close()
	}
}

// Critical 2: two DELETEs of one kept id race. The first restore is held
// inside its tc del; the second finds the restore in flight and answers 200
// at once without running tc. Exactly one del runs in all.
func TestServer_NetemConcurrentDeletesOfAKeptEntryRunOneDel(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	a.runner.mu.Lock()
	a.runner.fail["tc filter add"] = errors.New("exit status 2")
	a.runner.fail["tc qdisc del"] = errors.New("exit status 3")
	a.runner.mu.Unlock()
	if resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "productcatalogservice:3550", "upstream", `{"loss":10}`, 10000)); resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("start: status=%d body=%q, want 500", resp.StatusCode, body)
	}
	es, _ := a.srv.Netem.Journal.Load()
	if len(es) != 1 {
		t.Fatalf("journal = %+v, want the kept entry", es)
	}
	id := es[0].ID

	entered, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool // only the first del is held; any later one passes
	a.runner.mu.Lock()
	delete(a.runner.fail, "tc filter add")
	delete(a.runner.fail, "tc qdisc del")
	a.runner.seen = func(line string) {
		if strings.Contains(line, "tc qdisc del") && held.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	a.runner.mu.Unlock()
	before := len(a.runner.lines())

	del := func() int {
		dr, _ := http.NewRequest(http.MethodDelete, "http://agent/v1/faults/"+id, nil)
		dresp, err := a.client.Do(dr)
		if err != nil {
			t.Errorf("delete: %v", err)
			return 0
		}
		dresp.Body.Close()
		return dresp.StatusCode
	}
	first := make(chan int, 1)
	go func() { first <- del() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first restore never reached its tc del")
	}
	if code := del(); code != http.StatusOK {
		t.Fatalf("second delete while the first is in flight: status %d, want 200", code)
	}
	close(release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first delete: status %d, want 200", code)
	}
	dels := 0
	for _, l := range a.runner.lines()[before:] {
		if strings.Contains(l, "tc qdisc del") {
			dels++
		}
	}
	if dels != 1 {
		t.Fatalf("tc qdisc del ran %d times, want exactly 1: %q", dels, a.runner.lines()[before:])
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 0 {
		t.Fatalf("journal = %+v, want empty", es)
	}
}

// Important 4: cancelling Serve's context (SIGTERM) releases every running
// fault before Serve returns, so main cannot exit with a quota or a qdisc
// applied.
func TestServer_ShutdownReleasesRunningFaultsBeforeServeReturns(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	qresp, _ := a.post(t, context.Background(), quotaReq("pod-a", 0.5, 60000))
	defer qresp.Body.Close()
	nresp, _ := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 60000))
	defer nresp.Body.Close()
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "50000 100000" {
		t.Fatalf("cpu.max while armed = %q", got)
	}
	a.cancel()
	select {
	case <-a.served:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return")
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max when Serve returned = %q, want the quota restored", got)
	}
	if !a.runner.has("tc qdisc del dev eth0 root") {
		t.Fatalf("the netem qdisc must be deleted before Serve returns, ran %q", a.runner.lines())
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 0 {
		t.Fatalf("netem journal = %+v, want empty", es)
	}
	if qs, _ := a.srv.Journal.Load(); len(qs) != 0 {
		t.Fatalf("quota journal = %+v, want empty", qs)
	}
}

func TestServer_NetemRefusesAHostNetworkPodWith400(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	proc := a.srv.Netem.ProcRoot
	os.Remove(filepath.Join(proc, "1", "ns", "net"))
	fakeNetnsLink(t, proc, 1, podNetnsID)
	resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 1000))
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "host network namespace") {
		t.Fatalf("status=%d body=%q, want 400 naming the host network namespace", resp.StatusCode, body)
	}
}
