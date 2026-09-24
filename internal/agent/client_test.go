package agent

import (
	"context"
	"path/filepath"
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
