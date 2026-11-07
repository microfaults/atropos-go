package agent

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

func TestAgentFault_StartStopThroughTheSocket(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	if !Reachable(a.sock) {
		t.Fatal("agent must be reachable")
	}
	f := &AgentFault{FaultConfig: fault.FaultConfig{Duration: 10 * time.Second}, Sock: a.sock, PodUID: "pod-a", Container: "server", Name: "cpu-quota",
		Req: quotaReq("pod-a", 0.5, 10000).Fault}
	h, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for readTrim(t, filepath.Join(a.dir, "cpu.max")) != "50000 100000" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "50000 100000" {
		t.Fatalf("cpu.max while armed = %q", got)
	}
	h.Stop()
	select {
	case <-h.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("stopped agent fault did not deliver a result")
	}
	deadline = time.Now().Add(3 * time.Second)
	for readTrim(t, filepath.Join(a.dir, "cpu.max")) != "max 100000" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := readTrim(t, filepath.Join(a.dir, "cpu.max")); got != "max 100000" {
		t.Fatalf("cpu.max after stop = %q", got)
	}
}

func TestAgentFault_RejectedStartIsAnError(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	f := &AgentFault{FaultConfig: fault.FaultConfig{Duration: time.Second}, Sock: a.sock, PodUID: "pod-b", Container: "server", Req: quotaReq("pod-b", 0.5, 1000).Fault}
	if _, err := f.Start(context.Background()); err == nil {
		t.Fatal("a refused start must be an error, not a silent no-op")
	}
	if Reachable(shortSocket(t)) {
		t.Fatal("a missing socket must not be reachable")
	}
}

func TestCapabilities_ReportsTheAgentsMechanisms(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	ac, err := Capabilities(a.sock)
	if err != nil || strings.Join(ac.Mechanisms, ",") != "burn,quota,netem" {
		t.Fatalf("capabilities = %v, %v", ac, err)
	}
	if _, err := Capabilities(shortSocket(t)); err == nil {
		t.Fatal("a missing socket must be an error")
	}
}

func TestCapabilities_LegacyAgentReportsBurnAndQuota(t *testing.T) {
	sock := shortSocket(t)
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/faults", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("[]")) })
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	ac, err := Capabilities(sock)
	if err != nil || strings.Join(ac.Mechanisms, ",") != "burn,quota" {
		t.Fatalf("legacy agent: %v, %v (a 404 means burn and quota)", ac, err)
	}
}

func TestNetworkFactory_ValidatesAtDecodeAndBuildsAnAgentFault(t *testing.T) {
	h := NetworkFactory("/tmp/x.sock", "pod-a", "server")
	req := netReq("packet_loss", "productcatalogservice:3550", "upstream", 0, `{"loss":10}`)
	f, err := h("drop-catalog", &req, fault.FaultConfig{Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	af, ok := f.(*AgentFault)
	if !ok || af.Name != "drop-catalog" || af.PodUID != "pod-a" || af.Container != "server" || af.Req.FaultType != "packet_loss" || af.Req.Network.Target != "productcatalogservice:3550" {
		t.Fatalf("fault = %#v", f)
	}
	bad := netReq("latency", "", "", 0, `{"delay":"1ms"}`)
	if _, err := h("toxic", &bad, fault.FaultConfig{Duration: time.Second}); err == nil || !strings.Contains(err.Error(), "toxic") {
		t.Fatalf("a non-netem latency must fail decode through the factory, got %v", err)
	}
}
