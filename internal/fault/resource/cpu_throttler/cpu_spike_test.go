package fault

import (
	"context"
	"testing"
	"time"
)

func TestStartCPUThrottle_Basic(t *testing.T) {
	cfg := CPUThrottleConfig{
		TargetLoad: 0.3,
		Duration:   500 * time.Millisecond,
	}

	handle, err := StartCPUThrottle(context.Background(), cfg)
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

	t.Logf("available=%.2f cpus, workers=%d, per-worker=%.2f, ran %s",
		result.AvailableCPUs, result.Workers, result.PerWorkerLoad, result.ActualDuration)
}

func TestStartCPUThrottle_NonBlocking(t *testing.T) {
	cfg := CPUThrottleConfig{
		TargetLoad: 0.2,
		Duration:   1 * time.Second,
	}

	before := time.Now()
	handle, err := StartCPUThrottle(context.Background(), cfg)
	callDuration := time.Since(before)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// StartCPUThrottle must return almost instantly (well under 50ms).
	if callDuration > 50*time.Millisecond {
		t.Fatalf("StartCPUThrottle blocked for %s; expected instant return", callDuration)
	}

	t.Logf("StartCPUThrottle returned in %s (non-blocking ✓)", callDuration)

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	t.Logf("throttle completed after %s", result.ActualDuration)
}

func TestStartCPUThrottle_Stop(t *testing.T) {
	cfg := CPUThrottleConfig{
		TargetLoad: 0.5,
		Duration:   5 * time.Second,
	}

	handle, err := StartCPUThrottle(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Let it run briefly, then stop.
	time.Sleep(300 * time.Millisecond)
	handle.Stop()

	result := <-handle.Done()
	if result.ActualDuration > 1*time.Second {
		t.Fatalf("expected early stop, but ran for %s", result.ActualDuration)
	}

	t.Logf("stopped after %s", result.ActualDuration)
}

func TestStartCPUThrottle_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := CPUThrottleConfig{
		TargetLoad: 0.5,
		Duration:   5 * time.Second,
	}

	handle, err := StartCPUThrottle(ctx, cfg)
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

func TestStartCPUThrottle_AutoDetectCPUs(t *testing.T) {
	available := AvailableCPUs()
	if available <= 0 {
		t.Fatalf("AvailableCPUs() returned %.2f; expected > 0", available)
	}
	t.Logf("detected %.2f available CPUs", available)

	cfg := CPUThrottleConfig{
		TargetLoad: 0.1,
		Duration:   200 * time.Millisecond,
	}

	handle, err := StartCPUThrottle(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.AvailableCPUs != available {
		t.Fatalf("result.AvailableCPUs=%.2f, want %.2f", result.AvailableCPUs, available)
	}
	if result.Workers < 1 {
		t.Fatalf("expected at least 1 worker, got %d", result.Workers)
	}

	t.Logf("workers=%d, per-worker=%.2f", result.Workers, result.PerWorkerLoad)
}

func TestStartCPUThrottle_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  CPUThrottleConfig
	}{
		{"zero load", CPUThrottleConfig{TargetLoad: 0, Duration: 1 * time.Second}},
		{"load > 1", CPUThrottleConfig{TargetLoad: 1.5, Duration: 1 * time.Second}},
		{"zero duration", CPUThrottleConfig{TargetLoad: 0.5, Duration: 0}},
		{"ramp >= duration", CPUThrottleConfig{TargetLoad: 0.5, Duration: 1 * time.Second, RampUp: 1 * time.Second}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := StartCPUThrottle(context.Background(), tc.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			t.Logf("correctly rejected: %v", err)
		})
	}
}

func TestStartCPUThrottle_RampUp(t *testing.T) {
	cfg := CPUThrottleConfig{
		TargetLoad: 0.3,
		Duration:   800 * time.Millisecond,
		RampUp:     400 * time.Millisecond,
	}

	handle, err := StartCPUThrottle(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	t.Logf("ramp-up throttle: workers=%d, per-worker=%.2f, %s",
		result.Workers, result.PerWorkerLoad, result.ActualDuration)
}
