package interceptor

import (
	"context"
	"errors"
	"sync"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// errRegistryClosed is returned by StartOrJoin once the registry is closed.
// A closed registry's context is cancelled and its WaitGroup has already
// drained, so starting a fault would run it on a dead context and call
// wg.Add after Wait returned. Refusing keeps that invariant intact.
var errRegistryClosed = errors.New("interceptor: fault registry is closed")

// FaultRegistry tracks every running background fault under its rule/slot
// key. It exists for two reasons, and both are lifecycle, not bookkeeping:
//
//   - Detachment: faults started through the registry run under the
//     registry's context, not the triggering request's. Without it, a
//     background fault inherits the request context and dies when the
//     request completes -- a 30s CPU stress triggered by a 50ms request
//     runs for 50ms.
//   - Stop: rule removal is the platform's only fault-stop signal (phase
//     teardown clears rules), so the thing that holds the handles must be
//     able to cancel them by key. A background fault that outlives its
//     phase contaminates the next phase's measurement (X1).
//
// StartPolicy semantics: DeduplicateByRule joins an already-running fault
// under the same key; AlwaysStart always starts another instance. BOTH are
// tracked -- an untracked fault is an unstoppable fault.
type FaultRegistry struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	active map[string][]*fault.Handle
	wg     sync.WaitGroup
	closed bool
}

// NewFaultRegistry creates a registry whose lifetime is bounded by Close.
func NewFaultRegistry() *FaultRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &FaultRegistry{
		ctx:    ctx,
		cancel: cancel,
		active: make(map[string][]*fault.Handle),
	}
}

// StartOrJoin either starts a new fault or joins an existing one depending
// on the StartPolicy. The returned bool reports whether the call was
// deduplicated (joined an existing fault rather than starting one).
//
// startFn receives the registry's context (see the type comment) and runs
// under the registry lock -- fault Start implementations only validate and
// spawn, so this serializes control-plane fault starts, not request
// traffic. A panic in startFn propagates to the caller, but the lock is
// released first: a mutex left held would hang Stop, Close and every later
// start.
func (r *FaultRegistry) StartOrJoin(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context) (*fault.Handle, error),
) (*fault.Handle, bool, error) {
	h, deduped, err := r.admit(key, policy, startFn)
	if err != nil || deduped {
		return nil, deduped, err
	}

	// Handle.SetOnResult fires immediately if the fault already finished,
	// so a near-instant fault cannot leak its tracking entry.
	h.SetOnResult(func(_ fault.Result) {
		r.mu.Lock()
		hs := r.active[key]
		for i, cur := range hs {
			if cur == h {
				r.active[key] = append(hs[:i], hs[i+1:]...)
				break
			}
		}
		if len(r.active[key]) == 0 {
			delete(r.active, key)
		}
		r.mu.Unlock()
		r.wg.Done()
	})

	return h, false, nil
}

// admit is StartOrJoin's locked section: refuse once closed, join an
// already-running fault under DeduplicateByRule, or run startFn and track
// the handle it returns. The deferred unlock is what keeps a panicking
// startFn from leaving the lock held.
func (r *FaultRegistry) admit(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context) (*fault.Handle, error),
) (*fault.Handle, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, errRegistryClosed
	}
	if policy == evaluator.DeduplicateByRule && len(r.active[key]) > 0 {
		return nil, true, nil
	}

	h, err := startFn(r.ctx)
	if err != nil {
		return nil, false, err
	}
	r.active[key] = append(r.active[key], h)
	r.wg.Add(1)
	return h, false, nil
}

// Stop cancels every running fault started under key. Non-blocking: each
// fault delivers its Result through its own Send path once it has cleaned
// up, which is also what removes it from the registry. Unknown keys and a
// nil registry are no-ops.
func (r *FaultRegistry) Stop(key string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	hs := append([]*fault.Handle(nil), r.active[key]...)
	r.mu.Unlock()
	for _, h := range hs {
		h.Stop()
	}
}

// Close cancels the parent context (killing all running faults) and waits
// for every tracked fault to drain.
func (r *FaultRegistry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()

	r.cancel()
	r.wg.Wait()
}
