package memory

import (
	"context"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
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

// TestStress_ThrashingWithRampDown covers the chunks-slice mutation path that
// runs concurrently with thrash worker reads. Without the fix that stops
// workers before ramp-down, this triggers the race detector reliably.
func TestStress_ThrashingWithRampDown(t *testing.T) {
	s := newStress(0.05, 500*time.Millisecond, 0, 200*time.Millisecond, 64*1024, true, 4)

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
		t.Fatal("expected ThrashingEnabled=true")
	}
	t.Logf("thrash+rampdown: chunks=%d, peak=%d bytes, %s",
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

// Each ending path calls the release exactly once, on the fault's goroutine;
// the result arrives only after the release finished; and ending the fault
// (Stop, a cancelled parent) does not wait for the release.
func TestStress_ReleasesMemoryOnEveryEnding(t *testing.T) {
	stop := func(h *fault.Handle, _ context.CancelFunc) { h.Stop() }
	cancelParent := func(_ *fault.Handle, c context.CancelFunc) { c() }
	cases := []struct {
		name                       string
		duration, rampUp, rampDown time.Duration
		end                        func(*fault.Handle, context.CancelFunc) // nil: let it run out
	}{
		{"runs to its end", 200 * time.Millisecond, 0, 0, nil},
		{"Stop while holding", 5 * time.Second, 0, 0, stop},
		{"parent context cancelled", 5 * time.Second, 0, 0, cancelParent},
		{"Stop while ramping up", 5 * time.Second, 4 * time.Second, 0, stop},
		{"ramp-down runs to its end", 400 * time.Millisecond, 0, 200 * time.Millisecond, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			finish := make(chan struct{})
			var finishOnce sync.Once
			finishRelease := func() { finishOnce.Do(func() { close(finish) }) }
			orig := freeOSMemory
			freeOSMemory = func() { calls.Add(1); <-finish }
			t.Cleanup(func() { freeOSMemory = orig })
			// Registered last, so it runs first: a failed case leaves no
			// goroutine blocked in the release.
			t.Cleanup(finishRelease)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := newStress(0.05, tc.duration, tc.rampUp, tc.rampDown, 64*1024, false, 0)
			s.AvailableBytes, s.CurrentBytes = 256<<20, 0
			h, err := s.Start(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if tc.end != nil {
				time.Sleep(100 * time.Millisecond)
				ended := make(chan struct{})
				go func() { tc.end(h, cancel); close(ended) }()
				select {
				case <-ended:
				case <-time.After(time.Second):
					t.Fatal("ending the fault waited for the release")
				}
			}
			waitUntil(t, 5*time.Second, func() bool { return calls.Load() > 0 }) // the release started
			select {
			case <-h.Done():
				t.Fatal("the result arrived before the release finished")
			case <-time.After(50 * time.Millisecond):
			}
			finishRelease()
			select {
			case <-h.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("the fault did not end")
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("release called %d times, want 1", got)
			}
		})
	}
}

// With the real runtime, the fault's memory is released to the OS by the
// time its result arrives.
func TestStress_ReturnsMemoryToTheOS(t *testing.T) {
	const size = 64 << 20
	s := newStress(0.125, 10*time.Second, 0, 0, 1<<20, false, 0)
	s.AvailableBytes, s.CurrentBytes = 512<<20, 0
	h, err := s.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool { return readMetric("/memory/classes/heap/objects:bytes") >= size })
	before := readMetric("/memory/classes/heap/released:bytes")
	h.Stop()
	<-h.Done()
	// Released memory can also shrink (the allocator reuses released pages), so compare, never subtract unsigned.
	if after := readMetric("/memory/classes/heap/released:bytes"); after < before+size*3/4 {
		t.Fatalf("released %d MiB to the OS after the fault ended, want at least %d MiB",
			(int64(after)-int64(before))>>20, (size*3/4)>>20)
	}
}

// waitUntil polls cond every 5 ms and fails the test if it does not hold
// within d.
func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readMetric reads one runtime/metrics sample of kind KindUint64.
func readMetric(name string) uint64 {
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// Verify Stress implements fault.Fault at compile time.
var _ fault.Fault = (*Stress)(nil)
