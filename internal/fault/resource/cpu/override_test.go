package cpu

import (
	"context"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource"
)

// TestStress_AvailableCPUsOverride: the planned capacity comes from the
// override, not from this host's cgroup or NumCPU (the node agent's child
// runs in another container's cgroup but cannot see it).
func TestStress_AvailableCPUsOverride(t *testing.T) {
	s := &Stress{Config: resource.Config{FaultConfig: fault.FaultConfig{Duration: 120 * time.Millisecond}, TargetLoad: 0.5, AvailableCPUs: 2}}
	h, err := s.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res := <-h.Done()
	d, ok := res.Detail.(Detail)
	if !ok || d.AvailableCPUs != 2 || d.Workers != 1 {
		t.Fatalf("detail = %#v, want AvailableCPUs=2 Workers=1 (0.5 × 2)", res.Detail)
	}
}
