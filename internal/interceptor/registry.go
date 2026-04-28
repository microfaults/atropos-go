package interceptor

import (
	"context"
	"sync"

	"atropos-go/internal/evaluator"
	"atropos-go/internal/fault"
)

type registryEntry struct {
	handle *fault.Handle
}

// FaultRegistry deduplicates service-scoped faults (network proxies, CPU
// stress) that get triggered per-request but should only run once.
type FaultRegistry struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	active map[string]*registryEntry
	wg     sync.WaitGroup
	closed bool
}

// NewFaultRegistry creates a registry whose lifetime is bounded by Close.
func NewFaultRegistry() *FaultRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &FaultRegistry{
		ctx:    ctx,
		cancel: cancel,
		active: make(map[string]*registryEntry),
	}
}

// StartOrJoin either starts a new fault or joins an existing one depending
// on the StartPolicy.
//
//   - DeduplicateByRule: if a fault with the same key is already running,
//     return (nil, true, nil). After the fault completes the key is freed.
//   - AlwaysStart: always start a new instance, no dedup.
//
// The returned bool indicates whether the call was deduplicated (joined an
// existing fault rather than starting a fresh one).
func (r *FaultRegistry) StartOrJoin(
	key string,
	policy evaluator.StartPolicy,
	startFn func(ctx context.Context) (*fault.Handle, error),
) (*fault.Handle, bool, error) {
	if policy == evaluator.AlwaysStart {
		h, err := startFn(r.ctx)
		if err != nil {
			return nil, false, err
		}
		r.wg.Add(1)
		h.SetOnResult(func(_ fault.Result) {
			r.wg.Done()
		})
		return h, false, nil
	}

	r.mu.Lock()
	if _, ok := r.active[key]; ok {
		r.mu.Unlock()
		return nil, true, nil
	}

	h, err := startFn(r.ctx)
	if err != nil {
		r.mu.Unlock()
		return nil, false, err
	}

	r.active[key] = &registryEntry{handle: h}
	r.wg.Add(1)
	r.mu.Unlock()

	h.SetOnResult(func(_ fault.Result) {
		r.mu.Lock()
		delete(r.active, key)
		r.mu.Unlock()
		r.wg.Done()
	})

	return h, false, nil
}

// Close cancels the parent context (killing all running faults) and waits
// for every tracked fault to drain.
func (r *FaultRegistry) Close() {
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
