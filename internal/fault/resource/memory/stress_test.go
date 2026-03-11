package memory

import (
	"context"
	"testing"
	"time"

	"atropos-go/internal/fault"
)

func newStress(load float64, duration, rampUp, rampDown time.Duration, chunkSize int, thrashing bool, thrashWorkers int) *Stress {
	return &Stress{
		Config: Config{
			FaultConfig: fault.FaultConfig{
				Duration: duration,
				RampUp:   rampUp,
				RampDown: rampDown,
			},
			TargetLoad:    load,
			ChunkSize:     chunkSize,
			Thrashing:     thrashing,
			ThrashWorkers: thrashWorkers,
		},
	}
}

func TestStress_Basic(t *testing.T) {
	// Allocate 10% of available memory for 500ms.
	s := newStress(0.10, 500*time.Millisecond, 0, 0, 64*1024, false, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	lower := 400 * time.Millisecond
	upper := 800 * time.Millisecond
	if result.ActualDuration < lower || result.ActualDuration > upper {
		t.Fatalf("expected duration ~500ms, got %s", result.ActualDuration)
	}

	d := result.Detail.(Detail)
	t.Logf("available=%d bytes, target=%d bytes, peak=%d bytes, chunks=%d, ran %s",
		d.AvailableMemory, d.TargetBytes, d.PeakAllocated, d.ChunksAllocated, result.ActualDuration)
}

func TestStress_NonBlocking(t *testing.T) {
	s := newStress(0.05, 1*time.Second, 0, 0, 64*1024, false, 0)

	before := time.Now()
	handle, err := s.Start(context.Background())
	callDuration := time.Since(before)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if callDuration > 100*time.Millisecond {
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
	s := newStress(0.10, 5*time.Second, 0, 0, 64*1024, false, 0)

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
	s := newStress(0.10, 5*time.Second, 0, 0, 64*1024, false, 0)

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

func TestStress_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		s    *Stress
	}{
		{"zero load", newStress(0, 1*time.Second, 0, 0, 0, false, 0)},
		{"load > 1", newStress(1.5, 1*time.Second, 0, 0, 0, false, 0)},
		{"zero duration", newStress(0.5, 0, 0, 0, 0, false, 0)},
		{"ramp >= duration", newStress(0.5, 1*time.Second, 1*time.Second, 0, 0, false, 0)},
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
	s := newStress(0.10, 800*time.Millisecond, 400*time.Millisecond, 0, 64*1024, false, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	t.Logf("ramp-up: chunks=%d, peak=%d bytes, target=%d bytes, %s",
		d.ChunksAllocated, d.PeakAllocated, d.TargetBytes, result.ActualDuration)
}

func TestStress_Thrashing(t *testing.T) {
	s := newStress(0.05, 500*time.Millisecond, 0, 0, 64*1024, true, 2)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	if !d.ThrashingEnabled {
		t.Fatal("expected ThrashingEnabled=true in detail")
	}
	t.Logf("thrashing: chunks=%d, peak=%d bytes, %s",
		d.ChunksAllocated, d.PeakAllocated, result.ActualDuration)
}

func TestStress_AutoDetectMemory(t *testing.T) {
	available := AvailableMemory()
	if available == 0 {
		t.Skip("could not detect available memory (non-Linux?)")
	}
	t.Logf("detected %d bytes (%.1f MiB) available memory", available, float64(available)/(1024*1024))

	s := newStress(0.02, 300*time.Millisecond, 0, 0, 64*1024, false, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	d := result.Detail.(Detail)
	if d.AvailableMemory != available {
		t.Fatalf("AvailableMemory=%d, want %d", d.AvailableMemory, available)
	}
	if d.ChunksAllocated < 1 {
		t.Fatalf("expected at least 1 chunk, got %d", d.ChunksAllocated)
	}
	t.Logf("chunks=%d, peak=%d bytes, target=%d bytes", d.ChunksAllocated, d.PeakAllocated, d.TargetBytes)
}

// Verify Stress implements fault.Fault at compile time.
var _ fault.Fault = (*Stress)(nil)
