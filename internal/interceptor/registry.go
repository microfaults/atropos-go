package interceptor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// errRegistryClosed is returned by StartRun (and StartOrJoin) once the
// registry is closed. A closed registry's context is cancelled and its
// WaitGroup has already drained, so starting a fault would run it on a dead
// context and call wg.Add after Wait returned. Refusing keeps that invariant
// intact.
var errRegistryClosed = errors.New("interceptor: fault registry is closed")

// FaultRun is one tracked execution of a background fault: the run id minted
// when it started (atropos.fault.id), the registry key it runs under, its
// handle, and the span recording it -- nil when the starter has none, as
// through StartOrJoin. A request that joins the run instead of starting
// another is counted on that span as atropos.fault.joined.
type FaultRun struct {
	ID     string
	Key    string
	Handle *fault.Handle
	Span   trace.Span

	joined atomic.Int64
	// stampMu orders Join's stamps. Joiners count themselves in one order and
	// could stamp in another, leaving an earlier count on the span; under the
	// mutex each stamp writes the count as of its turn, so the last one
	// written is the final count.
	stampMu sync.Mutex
}

// Join counts one more request joining the run and, when the run has a span,
// stamps the count so far on it as atropos.fault.joined. It returns this
// join's count (1 for the first joiner).
func (r *FaultRun) Join() int64 {
	n := r.joined.Add(1)
	if r.Span != nil {
		r.stampMu.Lock()
		r.Span.SetAttributes(attribute.Int64(trace.AttrFaultJoined, r.joined.Load()))
		r.stampMu.Unlock()
	}
	return n
}

// NewRunID mints a fault run id: 8 random bytes from crypto/rand,
// hex-encoded (16 characters). It panics only if the OS entropy source fails
// (since Go 1.24 crypto/rand.Read crashes the process itself instead of
// returning an error, so the panic is a guard that never fires).
func NewRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("interceptor: reading run id entropy: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

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
	active map[string][]*FaultRun
	wg     sync.WaitGroup
	closed bool
}

// NewFaultRegistry creates a registry whose lifetime is bounded by Close.
func NewFaultRegistry() *FaultRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &FaultRegistry{
		ctx:    ctx,
		cancel: cancel,
		active: make(map[string][]*FaultRun),
	}
}

// StartRun either starts a new run of a fault under key or joins one
// already running there, depending on the StartPolicy. joined reports a
// join: run is then the running one (the newest, should AlwaysStart have
// left several under the key), with the id, handle and span its starter
// gave it, and startFn did not run. On a fresh start run is the new run.
//
// startFn receives the registry's context (see the type comment) and the id
// minted for the run, and returns the fault's handle and the span recording
// it (nil if there is none). It runs under the registry lock -- fault Start
// implementations only validate and spawn, so this serializes control-plane
// fault starts, not request traffic. A startFn error is returned as is and
// nothing is tracked. A panic in startFn propagates to the caller, but the
// lock is released first: a mutex left held would hang Stop, Close and every
// later start.
func (r *FaultRegistry) StartRun(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error),
) (run *FaultRun, joined bool, err error) {
	run, joined, err = r.admit(key, policy, startFn)
	if err != nil || joined {
		return run, joined, err
	}

	// Handle.SetOnResult fires immediately if the fault already finished,
	// so a near-instant fault cannot leak its tracking entry.
	run.Handle.SetOnResult(func(_ fault.Result) {
		r.mu.Lock()
		runs := r.active[key]
		for i, cur := range runs {
			if cur == run {
				r.active[key] = append(runs[:i], runs[i+1:]...)
				break
			}
		}
		if len(r.active[key]) == 0 {
			delete(r.active, key)
		}
		r.mu.Unlock()
		r.wg.Done()
	})

	return run, false, nil
}

// StartOrJoin is StartRun for a caller that has no span and no use for the
// run id. It returns the new fault's handle; the bool reports whether the
// call was deduplicated (joined an existing fault rather than starting one),
// in which case the handle is nil.
func (r *FaultRegistry) StartOrJoin(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context) (*fault.Handle, error),
) (*fault.Handle, bool, error) {
	run, joined, err := r.StartRun(key, policy, func(ctx context.Context, _ string) (*fault.Handle, trace.Span, error) {
		h, err := startFn(ctx)
		return h, nil, err
	})
	if err != nil || joined {
		return nil, joined, err
	}
	return run.Handle, false, nil
}

// admit is StartRun's locked section: refuse once closed, join the newest
// run under key under DeduplicateByRule, or mint a run id, run startFn and
// track the run it starts. The deferred unlock is what keeps a panicking
// startFn from leaving the lock held.
func (r *FaultRegistry) admit(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context, runID string) (*fault.Handle, trace.Span, error),
) (*FaultRun, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, errRegistryClosed
	}
	if runs := r.active[key]; policy == evaluator.DeduplicateByRule && len(runs) > 0 {
		return runs[len(runs)-1], true, nil
	}

	id := NewRunID()
	h, span, err := startFn(r.ctx, id)
	if err != nil {
		return nil, false, err
	}
	run := &FaultRun{ID: id, Key: key, Handle: h, Span: span}
	r.active[key] = append(r.active[key], run)
	r.wg.Add(1)
	return run, false, nil
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
	runs := append([]*FaultRun(nil), r.active[key]...)
	r.mu.Unlock()
	for _, run := range runs {
		run.Handle.Stop()
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
