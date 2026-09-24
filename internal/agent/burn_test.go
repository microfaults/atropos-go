package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func cpuBurn(durationMs int64) wire.FaultRequest {
	return wire.FaultRequest{Category: "resource", FaultType: "cpu", DurationMs: durationMs,
		Params: json.RawMessage(`{"target_load":0.2,"window":"20ms"}`)}
}

// TestStartBurn_PlacesChildBeforeItRunsAndDeliversResult: the child's pid
// lands in the target cgroup while the child is parked, and the existing
// cpu stress runs to completion with the capacity the parent supplied.
func TestStartBurn_PlacesChildBeforeItRunsAndDeliversResult(t *testing.T) {
	dir := cgroupDir(t, map[string]string{"cgroup.procs": ""})
	b, err := StartBurn(context.Background(), os.Args[0], dir, cpuBurn(200), Capacity{CPUs: 1, MemoryBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	procs, _ := os.ReadFile(filepath.Join(dir, "cgroup.procs"))
	if strings.TrimSpace(string(procs)) != strconv.Itoa(b.pid) {
		t.Fatalf("cgroup.procs = %q, want the child's pid %d", procs, b.pid)
	}
	select {
	case res := <-b.Done():
		if res.Error != "" || res.ActualDurationMs < 150 {
			t.Fatalf("result = %+v", res)
		}
		detail, _ := res.Detail.(map[string]any)
		if detail["AvailableCPUs"] != 1.0 {
			t.Fatalf("child must plan from the supplied capacity, detail=%v", res.Detail)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no result from the stress child")
	}
}

func TestStartBurn_StopEndsTheChildEarly(t *testing.T) {
	dir := cgroupDir(t, map[string]string{"cgroup.procs": ""})
	b, err := StartBurn(context.Background(), os.Args[0], dir, cpuBurn(10000), Capacity{CPUs: 1})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	b.Stop()
	select {
	case res := <-b.Done():
		if time.Since(start) > 5*time.Second || res.ActualDurationMs > 5000 {
			t.Fatalf("stop was not prompt: %+v after %v", res, time.Since(start))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stopped child did not report")
	}
}

func TestStartBurn_FailsWhenPlacementFails(t *testing.T) {
	if _, err := StartBurn(context.Background(), os.Args[0], t.TempDir(), cpuBurn(200), Capacity{CPUs: 1}); err == nil {
		t.Fatal("a child that cannot be placed must not run")
	}
}
