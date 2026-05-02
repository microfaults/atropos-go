package io

import (
	"context"
	"os"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

func newStress(readRate int64, fileSize, fileCount, workers int, duration, rampUp, rampDown time.Duration) *Stress {
	return &Stress{
		Config: Config{
			FaultConfig: fault.FaultConfig{
				Duration: duration,
				RampUp:   rampUp,
				RampDown: rampDown,
			},
			ReadRate:  readRate,
			FileSize:  fileSize,
			FileCount: fileCount,
			Workers:   workers,
		},
	}
}

func TestStress_Basic(t *testing.T) {
	// 64 × 1KB files, read at 32 KB/s for 1s → expect ~32 KB read.
	s := newStress(32*1024, 1024, 64, 4, 1*time.Second, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	lower := 500 * time.Millisecond
	upper := 1500 * time.Millisecond
	if result.ActualDuration < lower || result.ActualDuration > upper {
		t.Fatalf("expected duration ~1s, got %s", result.ActualDuration)
	}

	d := result.Detail.(Detail)
	t.Logf("written=%d bytes (%d files), read=%d bytes (%d ops), ran %s",
		d.TotalBytesWritten, d.FileCount, d.TotalBytesRead, d.TotalReadOps, result.ActualDuration)

	// Check read total is in the ballpark: 32KB/s × 1s = 32KB ± 50%.
	expectedRead := int64(32 * 1024)
	if d.TotalBytesRead < expectedRead/2 || d.TotalBytesRead > expectedRead*2 {
		t.Fatalf("expected ~%d bytes read, got %d", expectedRead, d.TotalBytesRead)
	}
}

func TestStress_NonBlocking(t *testing.T) {
	s := newStress(16*1024, 512, 32, 2, 1*time.Second, 0, 0)

	before := time.Now()
	handle, err := s.Start(context.Background())
	callDuration := time.Since(before)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Phase 1 (write 32 × 512B = 16KB) should be near-instant.
	if callDuration > 500*time.Millisecond {
		t.Fatalf("Start blocked for %s; expected fast return after writing small files", callDuration)
	}

	t.Logf("Start returned in %s (write phase complete)", callDuration)

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}
	t.Logf("completed after %s", result.ActualDuration)
}

func TestStress_Stop(t *testing.T) {
	s := newStress(16*1024, 1024, 64, 4, 5*time.Second, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	handle.Stop()

	result := <-handle.Done()
	if result.ActualDuration > 2*time.Second {
		t.Fatalf("expected early stop, but ran for %s", result.ActualDuration)
	}
	t.Logf("stopped after %s", result.ActualDuration)
}

func TestStress_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newStress(16*1024, 1024, 64, 4, 5*time.Second, 0, 0)

	handle, err := s.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	result := <-handle.Done()
	if result.ActualDuration > 2*time.Second {
		t.Fatalf("expected early cancellation, but ran for %s", result.ActualDuration)
	}
	t.Logf("cancelled after %s (err=%v)", result.ActualDuration, result.Err)
}

func TestStress_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		s    *Stress
	}{
		{"zero rate", newStress(0, 1024, 64, 4, 1*time.Second, 0, 0)},
		{"negative rate", newStress(-100, 1024, 64, 4, 1*time.Second, 0, 0)},
		{"zero file size", newStress(1024, 0, 64, 4, 1*time.Second, 0, 0)},
		{"zero file count", newStress(1024, 1024, 0, 4, 1*time.Second, 0, 0)},
		{"zero workers", newStress(1024, 1024, 64, 0, 1*time.Second, 0, 0)},
		{"zero duration", newStress(1024, 1024, 64, 4, 0, 0, 0)},
		{"ramp >= duration", newStress(1024, 1024, 64, 4, 1*time.Second, 1*time.Second, 0)},
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

func TestStress_ReadRateAccuracy(t *testing.T) {
	// 256 × 1KB files, read at 64 KB/s for 2s → expect ~128 KB read.
	s := newStress(64*1024, 1024, 256, 4, 2*time.Second, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	expectedRead := int64(64 * 1024 * 2) // 128 KB

	t.Logf("read %d bytes in %s (expected ~%d)", d.TotalBytesRead, result.ActualDuration, expectedRead)

	// Allow ±40% tolerance for CI variability.
	lower := int64(float64(expectedRead) * 0.6)
	upper := int64(float64(expectedRead) * 1.4)
	if d.TotalBytesRead < lower || d.TotalBytesRead > upper {
		t.Fatalf("expected %d–%d bytes read, got %d", lower, upper, d.TotalBytesRead)
	}
}

func TestStress_Cleanup(t *testing.T) {
	s := newStress(64*1024, 512, 16, 2, 500*time.Millisecond, 0, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	d := result.Detail.(Detail)

	// Give cleanup a moment to execute.
	time.Sleep(100 * time.Millisecond)

	if _, err := os.Stat(d.TempDir); !os.IsNotExist(err) {
		t.Fatalf("temp dir %s should have been cleaned up but still exists", d.TempDir)
	}
	t.Logf("temp dir %s correctly cleaned up", d.TempDir)
}

func TestStress_RampUp(t *testing.T) {
	s := newStress(32*1024, 1024, 64, 4, 1*time.Second, 400*time.Millisecond, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	t.Logf("ramp-up: read=%d bytes (%d ops), %s",
		d.TotalBytesRead, d.TotalReadOps, result.ActualDuration)
}

// Verify Stress implements fault.Fault at compile time.
var _ fault.Fault = (*Stress)(nil)
