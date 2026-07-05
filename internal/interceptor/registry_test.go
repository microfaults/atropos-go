package interceptor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

func newStubFault(duration time.Duration) fault.Fault {
	return &stubFault{duration: duration}
}

type stubFault struct {
	duration time.Duration
}

func (f *stubFault) Validate() error { return nil }
func (f *stubFault) Start(ctx context.Context) (*fault.Handle, error) {
	ctx, cancel := context.WithTimeout(ctx, f.duration)
	h := fault.NewHandle(cancel)
	go func() {
		defer cancel()
		<-ctx.Done()
		h.Send(fault.Result{ActualDuration: f.duration})
	}()
	return h, nil
}

func TestRegistry_DeduplicateByRule(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()

	var starts atomic.Int32
	startFn := func(ctx context.Context) (*fault.Handle, error) {
		starts.Add(1)
		return newStubFault(500 * time.Millisecond).Start(ctx)
	}

	h1, deduped1, err := r.StartOrJoin("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if deduped1 {
		t.Error("first start should not be deduped")
	}
	if h1 == nil {
		t.Fatal("expected handle")
	}

	_, deduped2, err := r.StartOrJoin("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	if !deduped2 {
		t.Error("second start should be deduped")
	}
	if starts.Load() != 1 {
		t.Errorf("expected 1 start, got %d", starts.Load())
	}

	<-h1.Done()
	// Small sleep to let cleanup goroutine run
	time.Sleep(50 * time.Millisecond)

	// After completion, same key should start fresh.
	_, deduped3, err := r.StartOrJoin("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil {
		t.Fatalf("third start: %v", err)
	}
	if deduped3 {
		t.Error("third start after completion should not be deduped")
	}
	if starts.Load() != 2 {
		t.Errorf("expected 2 starts, got %d", starts.Load())
	}
}

func TestRegistry_AlwaysStart(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()

	var starts atomic.Int32
	startFn := func(ctx context.Context) (*fault.Handle, error) {
		starts.Add(1)
		return newStubFault(200 * time.Millisecond).Start(ctx)
	}

	_, _, _ = r.StartOrJoin("rule-a", evaluator.AlwaysStart, startFn)
	_, _, _ = r.StartOrJoin("rule-a", evaluator.AlwaysStart, startFn)

	if starts.Load() != 2 {
		t.Errorf("expected 2 starts with AlwaysStart, got %d", starts.Load())
	}
}

func TestRegistry_Close_CancelsRunning(t *testing.T) {
	r := NewFaultRegistry()

	h, _, err := r.StartOrJoin("long-running", evaluator.DeduplicateByRule, func(ctx context.Context) (*fault.Handle, error) {
		return newStubFault(10 * time.Second).Start(ctx)
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	r.Close()

	select {
	case <-h.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel running fault within 2s")
	}
}

func TestRegistry_DifferentKeys_Independent(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()

	var starts atomic.Int32
	startFn := func(ctx context.Context) (*fault.Handle, error) {
		starts.Add(1)
		return newStubFault(200 * time.Millisecond).Start(ctx)
	}

	_, _, _ = r.StartOrJoin("rule-a", evaluator.DeduplicateByRule, startFn)
	_, _, _ = r.StartOrJoin("rule-b", evaluator.DeduplicateByRule, startFn)

	if starts.Load() != 2 {
		t.Errorf("expected 2 starts for different keys, got %d", starts.Load())
	}
}

func TestRegistry_StopByKey_CancelsAllInstances(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()

	startFn := func(ctx context.Context) (*fault.Handle, error) {
		return newStubFault(10 * time.Second).Start(ctx)
	}
	h1, _, _ := r.StartOrJoin("rule-a", evaluator.AlwaysStart, startFn)
	h2, _, _ := r.StartOrJoin("rule-a", evaluator.AlwaysStart, startFn)
	hOther, _, _ := r.StartOrJoin("rule-b", evaluator.DeduplicateByRule, startFn)

	r.Stop("rule-a")

	for i, h := range []*fault.Handle{h1, h2} {
		select {
		case <-h.Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("Stop(rule-a) did not cancel instance %d within 2s", i)
		}
	}
	select {
	case <-hOther.Done():
		t.Fatal("Stop(rule-a) must not cancel rule-b's fault")
	case <-time.After(100 * time.Millisecond):
	}

	// The key frees once its faults drain: a fresh start must not dedup
	// against stopped instances.
	time.Sleep(50 * time.Millisecond)
	_, deduped, err := r.StartOrJoin("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil {
		t.Fatalf("restart after stop: %v", err)
	}
	if deduped {
		t.Fatal("restart after Stop was deduped against a cancelled fault")
	}
}

// TestRegistry_InstantFaultDoesNotLeak pins the SetOnResult contract fix:
// a fault that completes before the registry registers its bookkeeping
// callback must still be removed from tracking (previously the callback
// was silently dropped, leaking the entry and hanging Close's WaitGroup).
func TestRegistry_InstantFaultDoesNotLeak(t *testing.T) {
	r := NewFaultRegistry()

	_, _, err := r.StartOrJoin("instant", evaluator.DeduplicateByRule, func(ctx context.Context) (*fault.Handle, error) {
		_, cancel := context.WithCancel(ctx)
		h := fault.NewHandle(cancel)
		h.Send(fault.Result{}) // completes before StartOrJoin can SetOnResult
		return h, nil
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung: instant fault leaked its registry entry")
	}
}

func TestRegistry_StopUnknownKeyAndNil(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	r.Stop("never-started") // no-op
	var nilReg *FaultRegistry
	nilReg.Stop("x") // no-op, no panic
}
