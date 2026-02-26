package fault

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestInjectCPUSpike_Basic(t *testing.T) {
	// Use 1 core for 500 ms — should complete in roughly that time.
	cfg := CPUSpikeConfig{Cores: 1, Duration: 500 * time.Millisecond}
	result := InjectCPUSpike(context.Background(), cfg)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.ActualCores != 1 {
		t.Fatalf("expected 1 core, got %d", result.ActualCores)
	}

	// Allow ±200 ms tolerance.
	lower := 300 * time.Millisecond
	upper := 700 * time.Millisecond
	if result.ActualDuration < lower || result.ActualDuration > upper {
		t.Fatalf("expected duration ~500ms, got %s", result.ActualDuration)
	}

	t.Logf("spike ran on %d core(s) for %s", result.ActualCores, result.ActualDuration)
}

func TestInjectCPUSpike_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Schedule cancellation after 200 ms.
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	cfg := CPUSpikeConfig{Cores: 1, Duration: 5 * time.Second}
	result := InjectCPUSpike(ctx, cfg)

	// Should have stopped well before 5 s.
	if result.ActualDuration > 1*time.Second {
		t.Fatalf("expected early cancellation, but ran for %s", result.ActualDuration)
	}

	t.Logf("cancelled after %s (err=%v)", result.ActualDuration, result.Err)
}

func TestInjectCPUSpike_ClampsCores(t *testing.T) {
	cfg := CPUSpikeConfig{Cores: 999, Duration: 100 * time.Millisecond}
	result := InjectCPUSpike(context.Background(), cfg)

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.ActualCores != runtime.NumCPU() {
		t.Fatalf("expected cores clamped to %d, got %d", runtime.NumCPU(), result.ActualCores)
	}

	t.Logf("clamped to %d cores, ran for %s", result.ActualCores, result.ActualDuration)
}

func TestInjectCPUSpike_ZeroDuration(t *testing.T) {
	cfg := CPUSpikeConfig{Cores: 1, Duration: 0}
	result := InjectCPUSpike(context.Background(), cfg)

	if result.Err == nil {
		t.Fatal("expected error for zero duration, got nil")
	}
	if result.ActualDuration != 0 {
		t.Fatalf("expected 0 duration, got %s", result.ActualDuration)
	}

	t.Logf("correctly rejected zero duration: %v", result.Err)
}

func TestInjectCPUSpike_InvalidCores(t *testing.T) {
	cfg := CPUSpikeConfig{Cores: 0, Duration: 1 * time.Second}
	result := InjectCPUSpike(context.Background(), cfg)

	if result.Err == nil {
		t.Fatal("expected error for zero cores, got nil")
	}

	t.Logf("correctly rejected zero cores: %v", result.Err)
}
