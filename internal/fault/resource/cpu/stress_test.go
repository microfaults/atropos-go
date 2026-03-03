package cpu

import (
	"context"
	"testing"
	"time"

	"atropos-go/internal/fault"
	"atropos-go/internal/fault/resource"
)

func newStress(load float64, duration, rampUp, rampDown time.Duration) *Stress {
	return &Stress{
		Config: resource.Config{
			FaultConfig: fault.FaultConfig{
				Duration: duration,
				RampUp:   rampUp,
				RampDown: rampDown,
			},
			TargetLoad: load,
		},
	}
}

func TestStress_Basic(t *testing.T) {
	s := newStress(0.3, 500*time.Millisecond, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	lower := 400 * time.Millisecond
	upper := 700 * time.Millisecond
	if result.ActualDuration < lower || result.ActualDuration > upper {
		t.Fatalf("expected duration ~500ms, got %s", result.ActualDuration)
	}

	d := result.Detail.(Detail)
	t.Logf("available=%.2f cpus, workers=%d, per-worker=%.2f, ran %s",
		d.AvailableCPUs, d.Workers, d.PerWorkerLoad, result.ActualDuration)
}

func TestStress_NonBlocking(t *testing.T) {
	s := newStress(0.2, 1*time.Second, 0, 0)

	before := time.Now()
	handle, err := s.Start(context.Background())
	callDuration := time.Since(before)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if callDuration > 50*time.Millisecond {
		t.Fatalf("Start blocked for %s; expected instant return", callDuration)
	}

	t.Logf("Start returned in %s (non-blocking)", callDuration)

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}
	t.Logf("completed after %s", result.ActualDuration)
}

func TestStress_Stop(t *testing.T) {
	s := newStress(0.5, 5*time.Second, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	handle.Stop()

	result := <-handle.Done()
	if result.ActualDuration > 1*time.Second {
		t.Fatalf("expected early stop, but ran for %s", result.ActualDuration)
	}
	t.Logf("stopped after %s", result.ActualDuration)
}

func TestStress_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newStress(0.5, 5*time.Second, 0, 0)

	handle, err := s.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	result := <-handle.Done()
	if result.ActualDuration > 1*time.Second {
		t.Fatalf("expected early cancellation, but ran for %s", result.ActualDuration)
	}
	t.Logf("cancelled after %s (err=%v)", result.ActualDuration, result.Err)
}

func TestStress_AutoDetectCPUs(t *testing.T) {
	available := AvailableCPUs()
	if available <= 0 {
		t.Fatalf("AvailableCPUs() returned %.2f; expected > 0", available)
	}
	t.Logf("detected %.2f available CPUs", available)

	s := newStress(0.1, 200*time.Millisecond, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	d := result.Detail.(Detail)
	if d.AvailableCPUs != available {
		t.Fatalf("AvailableCPUs=%.2f, want %.2f", d.AvailableCPUs, available)
	}
	if d.Workers < 1 {
		t.Fatalf("expected at least 1 worker, got %d", d.Workers)
	}
	t.Logf("workers=%d, per-worker=%.2f", d.Workers, d.PerWorkerLoad)
}

func TestStress_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		s    *Stress
	}{
		{"zero load", newStress(0, 1*time.Second, 0, 0)},
		{"load > 1", newStress(1.5, 1*time.Second, 0, 0)},
		{"zero duration", newStress(0.5, 0, 0, 0)},
		{"ramp >= duration", newStress(0.5, 1*time.Second, 1*time.Second, 0)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.s.Start(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			t.Logf("correctly rejected: %v", err)
		})
	}
}

func TestStress_RampUp(t *testing.T) {
	s := newStress(0.3, 800*time.Millisecond, 400*time.Millisecond, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	t.Logf("ramp-up: workers=%d, per-worker=%.2f, %s",
		d.Workers, d.PerWorkerLoad, result.ActualDuration)
}

// Verify Stress implements fault.Fault at compile time.
var _ fault.Fault = (*Stress)(nil)
