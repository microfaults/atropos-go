package interceptor

import (
	"context"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
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

// TestRegistry_StartOrJoin_AfterCloseRefuses pins the closed-guard: once the
// registry is closed, StartOrJoin must refuse rather than start a fault on a
// cancelled context and call wg.Add after Close's Wait already returned.
func TestRegistry_StartOrJoin_AfterCloseRefuses(t *testing.T) {
	r := NewFaultRegistry()
	r.Close()

	var started atomic.Int32
	h, deduped, err := r.StartOrJoin("k", evaluator.DeduplicateByRule, func(ctx context.Context) (*fault.Handle, error) {
		started.Add(1)
		return newStubFault(time.Second).Start(ctx)
	})
	if err == nil {
		t.Fatal("expected error from StartOrJoin on a closed registry")
	}
	if h != nil || deduped {
		t.Fatalf("closed registry must not start or dedup: h=%v deduped=%v", h, deduped)
	}
	if started.Load() != 0 {
		t.Fatalf("startFn ran %d times on a closed registry, want 0", started.Load())
	}
}

func TestRegistry_StopUnknownKeyAndNil(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	r.Stop("never-started") // no-op
	var nilReg *FaultRegistry
	nilReg.Stop("x") // no-op, no panic
}

// TestRegistry_PanickingStartReleasesTheLock: startFn runs under the registry
// lock. When a fault's Start panics, the panic must still reach the caller
// and the lock must come free -- a mutex left held would hang Stop, Close and
// every later start (the admin DELETE and the SDK shutdown among them).
func TestRegistry_PanickingStartReleasesTheLock(t *testing.T) {
	r := NewFaultRegistry()

	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want the Start panic to propagate to the caller", p)
			}
		}()
		_, _, _ = r.StartOrJoin("k", evaluator.DeduplicateByRule, func(context.Context) (*fault.Handle, error) {
			panic("boom")
		})
		t.Fatal("StartOrJoin returned normally; the panic must propagate")
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h, deduped, err := r.StartOrJoin("k", evaluator.DeduplicateByRule, func(ctx context.Context) (*fault.Handle, error) {
			return newStubFault(time.Hour).Start(ctx)
		})
		if err != nil || deduped || h == nil {
			t.Errorf("start after the panic: handle=%v deduped=%v err=%v, want a fresh start", h, deduped, err)
		}
		r.Stop("k")
		r.Close()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry is wedged: a panicking Start left its lock held")
	}
}

func TestRegistry_StartRun_JoinReturnsRunAndCounts(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	rec := &recordingSpan{}
	startFn := func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error) {
		if len(runID) != 16 {
			t.Fatalf("runID = %q, want 16 hex chars", runID)
		}
		h, err := newStubFault(300 * time.Millisecond).Start(ctx)
		return h, rec, err
	}
	run, joined, err := r.StartRun("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil || joined || run == nil || run.Span != trace.Span(rec) {
		t.Fatalf("first: run=%v joined=%v err=%v", run, joined, err)
	}
	run2, joined2, err := r.StartRun("rule-a", evaluator.DeduplicateByRule, startFn)
	if err != nil || !joined2 || run2 != run {
		t.Fatalf("second: run2=%p run=%p joined=%v err=%v", run2, run, joined2, err)
	}
	if n := run2.Join(); n != 1 {
		t.Fatalf("Join = %d, want 1", n)
	}
	if !hasAttr(rec.attrs, trace.AttrFaultJoined, "1") {
		t.Fatalf("fault span attrs = %v, want atropos.fault.joined=1", rec.attrs)
	}
}

// Review Focus 5: the StartOrJoin wrapper leaves Span nil; Join must not panic.
func TestRegistry_StartRun_JoinWithNilSpan(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	startFn := func(ctx context.Context) (*fault.Handle, error) {
		return newStubFault(200 * time.Millisecond).Start(ctx)
	}
	if _, _, err := r.StartOrJoin("rule-b", evaluator.DeduplicateByRule, startFn); err != nil {
		t.Fatal(err)
	}
	run, joined, err := r.StartRun("rule-b", evaluator.DeduplicateByRule, func(ctx context.Context, _ string) (*fault.Handle, trace.Span, error) {
		h, err := newStubFault(time.Millisecond).Start(ctx)
		return h, nil, err
	})
	if err != nil || !joined || run.Span != nil {
		t.Fatalf("run=%+v joined=%v err=%v", run, joined, err)
	}
	if n := run.Join(); n != 1 {
		t.Fatalf("Join = %d", n)
	}
}

// The run StartRun returns is the one the registry tracks: its id is the one
// startFn was handed, its key is the registry key, and Stop reaches its
// handle. A finished run frees its key.
func TestRegistry_StartRun_RunCarriesIDKeyAndHandle(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	var minted string
	run, joined, err := r.StartRun("rule-c", evaluator.DeduplicateByRule, func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error) {
		minted = runID
		h, err := newStubFault(time.Hour).Start(ctx)
		return h, nil, err
	})
	if err != nil || joined {
		t.Fatalf("start: joined=%v err=%v", joined, err)
	}
	if run.ID != minted || run.Key != "rule-c" || run.Handle == nil {
		t.Fatalf("run = {ID:%q Key:%q Handle:%v}, want ID %q (the one startFn got), Key rule-c, a handle", run.ID, run.Key, run.Handle, minted)
	}
	r.Stop("rule-c")
	select {
	case <-run.Handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not reach the run's handle")
	}
	time.Sleep(50 * time.Millisecond) // the completion callback frees the key
	next, joined, err := r.StartRun("rule-c", evaluator.DeduplicateByRule, func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error) {
		h, err := newStubFault(time.Millisecond).Start(ctx)
		return h, nil, err
	})
	if err != nil || joined || next.ID == run.ID {
		t.Fatalf("after the run ended: joined=%v err=%v id=%q (old %q), want a fresh run", joined, err, next.ID, run.ID)
	}
}

// A start that fails tracks nothing: the error comes back as is, and the key
// stays free for the next start.
func TestRegistry_StartRun_StartErrorTracksNothing(t *testing.T) {
	r := NewFaultRegistry()
	boom := errors.New("boom")
	run, joined, err := r.StartRun("rule-d", evaluator.DeduplicateByRule, func(context.Context, string) (*fault.Handle, trace.Span, error) {
		return nil, nil, boom
	})
	if !errors.Is(err, boom) || joined || run != nil {
		t.Fatalf("run=%v joined=%v err=%v, want nil, false, boom", run, joined, err)
	}
	_, joined, err = r.StartRun("rule-d", evaluator.DeduplicateByRule, func(ctx context.Context, _ string) (*fault.Handle, trace.Span, error) {
		h, err := newStubFault(time.Millisecond).Start(ctx)
		return h, nil, err
	})
	if err != nil || joined {
		t.Fatalf("start after a failed one: joined=%v err=%v, want a fresh start", joined, err)
	}
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung: the failed start was tracked")
	}
}

// Run ids are 16 hex characters (8 random bytes) and do not repeat.
func TestNewRunID_SixteenHexCharsUnique(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := NewRunID()
		if b, err := hex.DecodeString(id); err != nil || len(b) != 8 {
			t.Fatalf("run id %q: want 16 hex characters (err=%v)", id, err)
		}
		if seen[id] {
			t.Fatalf("run id %q repeated", id)
		}
		seen[id] = true
	}
}

// lastStampSpan is a goroutine-safe span that keeps the last
// atropos.fault.joined value stamped on it. SetAttributes yields first, so
// concurrent joiners interleave the way a busy process makes them.
type lastStampSpan struct {
	recordingSpan // the rest of trace.Span
	mu            sync.Mutex
	last          int64
}

func (s *lastStampSpan) SetAttributes(attrs ...attribute.KeyValue) {
	runtime.Gosched()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range attrs {
		if a.Key == trace.AttrFaultJoined {
			s.last = a.Value.AsInt64()
		}
	}
}

// Joiners race: each one counts itself and stamps the count on the run's
// span, and they need not stamp in the order they counted. However they
// interleave, the span must end up holding the final count, never an
// earlier one. Many short trials, because the race is at the end of each.
func TestFaultRun_ConcurrentJoinsLeaveTheFinalCountOnTheSpan(t *testing.T) {
	const trials, goroutines, joins = 200, 8, 25
	for trial := range trials {
		span := &lastStampSpan{}
		run := &FaultRun{ID: NewRunID(), Key: "k", Span: span}
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range joins {
					run.Join()
				}
			}()
		}
		wg.Wait()
		if span.last != goroutines*joins {
			t.Fatalf("trial %d: the span holds atropos.fault.joined=%d, want the final count %d", trial, span.last, goroutines*joins)
		}
	}
}

// Should AlwaysStart have left several runs under one key, a
// DeduplicateByRule start joins the newest of them.
func TestRegistry_StartRun_JoinsTheNewestRun(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	start := func(ctx context.Context, _ string) (*fault.Handle, trace.Span, error) {
		h, err := newStubFault(time.Hour).Start(ctx)
		return h, nil, err
	}
	older, _, err := r.StartRun("k", evaluator.AlwaysStart, start)
	if err != nil {
		t.Fatal(err)
	}
	newer, _, err := r.StartRun("k", evaluator.AlwaysStart, start)
	if err != nil {
		t.Fatal(err)
	}
	run, joined, err := r.StartRun("k", evaluator.DeduplicateByRule, start)
	if err != nil || !joined || run != newer {
		t.Fatalf("joined=%v run=%p (older %p, newer %p) err=%v, want a join on the newer run", joined, run, older, newer, err)
	}
	r.Stop("k")
}

// StartOrJoin keeps its contract over StartRun: a join returns a nil handle
// with deduped=true and never runs startFn.
func TestRegistry_StartOrJoin_JoinReturnsNilHandle(t *testing.T) {
	r := NewFaultRegistry()
	defer r.Close()
	var starts atomic.Int32
	startFn := func(ctx context.Context) (*fault.Handle, error) {
		starts.Add(1)
		return newStubFault(time.Hour).Start(ctx)
	}
	first, deduped, err := r.StartOrJoin("k", evaluator.DeduplicateByRule, startFn)
	if err != nil || deduped || first == nil {
		t.Fatalf("first: handle=%v deduped=%v err=%v", first, deduped, err)
	}
	h, deduped, err := r.StartOrJoin("k", evaluator.DeduplicateByRule, startFn)
	if err != nil || !deduped || h != nil || starts.Load() != 1 {
		t.Fatalf("join: handle=%v deduped=%v err=%v starts=%d, want nil, true, nil, 1", h, deduped, err, starts.Load())
	}
	r.Stop("k")
}
