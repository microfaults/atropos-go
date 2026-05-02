package disk

import (
	"context"
	"os"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

func newStress(writeRate, maxDisk int64, duration, rampUp, rampDown time.Duration) *Stress {
	return &Stress{
		Config: Config{
			FaultConfig: fault.FaultConfig{
				Duration: duration,
				RampUp:   rampUp,
				RampDown: rampDown,
			},
			WriteRate:    writeRate,
			MaxDiskUsage: maxDisk,
		},
	}
}

func TestStress_Basic(t *testing.T) {
	s := newStress(32*1024, 1*1024*1024, 1*time.Second, 0, 0)

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
	t.Logf("written=%d bytes (%d ops), max_disk=%d, ran %s",
		d.TotalBytesWritten, d.TotalWriteOps, d.MaxDiskUsage, result.ActualDuration)
}

func TestStress_NonBlocking(t *testing.T) {
	s := newStress(16*1024, 512*1024, 1*time.Second, 0, 0)

	before := time.Now()
	handle, err := s.Start(context.Background())
	callDuration := time.Since(before)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if callDuration > 500*time.Millisecond {
		t.Fatalf("Start blocked for %s; expected fast return", callDuration)
	}
	t.Logf("Start returned in %s", callDuration)

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}
	t.Logf("completed after %s", result.ActualDuration)
}

func TestStress_Stop(t *testing.T) {
	s := newStress(16*1024, 4*1024*1024, 5*time.Second, 0, 0)

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
	s := newStress(16*1024, 4*1024*1024, 5*time.Second, 0, 0)

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
		{"zero rate", newStress(0, 1*1024*1024, 1*time.Second, 0, 0)},
		{"negative rate", newStress(-100, 1*1024*1024, 1*time.Second, 0, 0)},
		{"zero max disk", newStress(1024, 0, 1*time.Second, 0, 0)},
		{"zero duration", newStress(1024, 1*1024*1024, 0, 0, 0)},
		{"ramp >= duration", newStress(1024, 1*1024*1024, 1*time.Second, 1*time.Second, 0)},
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

func TestStress_SingleFile(t *testing.T) {
	// Verify only one temp file is created, grows to MaxDiskUsage, then overwrites.
	tmpDir := t.TempDir()
	chunkSize := int64(4 * 1024)           // 4 KB chunks
	maxDisk := int64(4 * chunkSize)        // cap at 4 chunks = 16 KB

	s := &Stress{
		Config: Config{
			FaultConfig:  fault.FaultConfig{Duration: 1 * time.Second},
			WriteRate:    chunkSize * 20, // fast enough to hit cap and sustain
			MaxDiskUsage: maxDisk,
			ChunkSize:    chunkSize,
			Path:         tmpDir,
		},
	}

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	d := result.Detail.(Detail)

	// Give defers time to run.
	time.Sleep(100 * time.Millisecond)

	// Temp file should be cleaned up.
	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 0 {
		t.Fatalf("expected temp file to be cleaned up, found %d files", len(entries))
	}

	// Should have written more than maxDisk (sustain phase overwrites in place).
	if d.TotalBytesWritten < maxDisk {
		t.Fatalf("expected at least %d bytes written, got %d", maxDisk, d.TotalBytesWritten)
	}

	t.Logf("written=%d bytes (%d ops), max_disk_cap=%d", d.TotalBytesWritten, d.TotalWriteOps, maxDisk)
}

func TestStress_WriteRateAccuracy(t *testing.T) {
	// ChunkSize must be << WriteRate×duration so the token bucket can grant
	// many tokens and we get an accurate rate measurement.
	chunkSize := int64(4 * 1024) // 4 KB
	writeRate := int64(64 * 1024) // 64 KB/s → ~128 KB over 2s
	s := &Stress{
		Config: Config{
			FaultConfig:  fault.FaultConfig{Duration: 2 * time.Second},
			WriteRate:    writeRate,
			MaxDiskUsage: 4 * 1024 * 1024,
			ChunkSize:    chunkSize,
		},
	}

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected result error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	expected := writeRate * 2 // rate × duration

	lower := int64(float64(expected) * 0.6)
	upper := int64(float64(expected) * 1.4)
	if d.TotalBytesWritten < lower || d.TotalBytesWritten > upper {
		t.Fatalf("expected %d–%d bytes written, got %d", lower, upper, d.TotalBytesWritten)
	}
	t.Logf("wrote %d bytes in %s (expected ~%d)", d.TotalBytesWritten, result.ActualDuration, expected)
}

func TestStress_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	s := &Stress{
		Config: Config{
			FaultConfig:  fault.FaultConfig{Duration: 500 * time.Millisecond},
			WriteRate:    64 * 1024,
			MaxDiskUsage: 1 * 1024 * 1024,
			Path:         tmpDir,
		},
	}

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	<-handle.Done()
	time.Sleep(50 * time.Millisecond)

	entries, _ := os.ReadDir(tmpDir)
	if len(entries) != 0 {
		t.Fatalf("expected temp file to be removed, found %d files", len(entries))
	}
	t.Logf("temp file correctly cleaned up in %s", tmpDir)
}

func TestStress_RampUp(t *testing.T) {
	s := newStress(32*1024, 2*1024*1024, 1*time.Second, 400*time.Millisecond, 0)

	handle, err := s.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := <-handle.Done()
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}

	d := result.Detail.(Detail)
	t.Logf("ramp-up: written=%d bytes (%d ops), %s", d.TotalBytesWritten, d.TotalWriteOps, result.ActualDuration)
}

// Compile-time check.
var _ fault.Fault = (*Stress)(nil)
