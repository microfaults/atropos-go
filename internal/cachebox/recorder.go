package cachebox

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// CacheRecord is a pending cache insertion. The interceptor builds one of
// these on the passthrough path and hands it to the recorder via the
// Recorder.Record method.
//
// Key, when set, is the authoritative cache key: the one the interceptor
// derived from the matched rule's CacheBoxContext (strategy + key headers)
// on the hot path -- the same derivation a replay-family decision will use
// at lookup time (INV-2: record-time and replay-time keys must be
// provably identical, so the replay-side derivation is captured rather
// than re-derived). KeyStrategy/StrategyVersion name that derivation for
// wire provenance. An empty Key falls back to the recorder's
// construction-time keyFn (legacy callers without a rule context).
//
// Request is kept so the fallback path can derive the key off the hot
// path. RequestBody is only set when the key strategy needs it
// (exact_with_body, canonical_v2). Neither the Request nor the Body is
// cloned by the recorder; callers must ensure the values are safe to read
// from the drain goroutine.
//
// ExperimentID/PhaseID are the matched rule's CacheBoxContext provenance
// (design doc Q5/INV-5). The interceptor only builds a CacheRecord when
// both are non-empty (see cacheBoxPassthrough) -- there is no ambient
// fallback for a missing pair.
type CacheRecord struct {
	Key             string
	KeyStrategy     string
	StrategyVersion int
	Request         *http.Request
	RequestBody     []byte
	StatusCode      int
	ResponseHeader  http.Header
	ResponseBody    []byte
	ObservedLatency time.Duration
	Timestamp       time.Time
	ExperimentID    string
	PhaseID         string

	// flushBarrier, when set, marks this as a control record rather than a
	// real one: drain() closes it and moves on instead of processing a
	// record. Only Flush uses this; it is not part of the public API.
	flushBarrier chan struct{}
}

// PushFunc is an optional hook for forwarding each newly-recorded entry
// to a central store (e.g. manteion). It is called from the drain goroutine
// -- never on the request hot path -- so it may perform synchronous I/O
// without affecting request latency.
//
// PushFunc implementations must be safe for concurrent use (although the
// default recorder only has one drain goroutine).
type PushFunc func(key string, entry *Entry)

// Recorder buffers CacheRecords and drains them asynchronously into a Store,
// optionally forwarding via PushFunc.
//
// The buffer is bounded; when full, Record drops the incoming record rather
// than blocking the request path. Drops are counted in Stats for
// observability. This is a deliberate backpressure strategy: we would
// rather lose cache entries than add synchronous latency to live traffic.
type Recorder struct {
	store    Store
	keyFn    KeyFunc
	push     PushFunc
	fidelity *FidelityRegistry

	ch chan CacheRecord
	wg sync.WaitGroup

	// mu makes the "check stopped, then send on ch" sequence in Record and
	// Flush atomic with respect to Stop's "set stopped, then close ch". Send
	// sites hold RLock (so they stay concurrent with one another on the hot
	// path) while Stop takes the exclusive Lock before closing ch, so ch is
	// never closed while a send is in flight -- the "send on closed channel"
	// panic that a bare atomic check-then-send allowed. drain never acquires
	// mu, so holding RLock across Flush's blocking send cannot deadlock Stop:
	// drain keeps draining ch and freeing buffer space regardless of the lock.
	mu      sync.RWMutex
	stopped bool

	recorded atomic.Int64
	dropped  atomic.Int64
}

// RecorderConfig configures a Recorder.
type RecorderConfig struct {
	Store   Store   // required
	KeyFunc KeyFunc // required
	Push    PushFunc
	BufSize int // default 1024

	// Fidelity, if set, receives per-(experiment_id, phase_id)
	// record_enqueued/record_dropped counts (ATRO-7, design doc Q6),
	// attributed from each record's own ExperimentID/PhaseID.
	Fidelity *FidelityRegistry
}

// NewRecorder constructs a Recorder and starts its drain goroutine.
// Callers MUST call Stop when the recorder is no longer needed.
func NewRecorder(cfg RecorderConfig) *Recorder {
	if cfg.BufSize <= 0 {
		cfg.BufSize = 1024
	}
	r := &Recorder{
		store:    cfg.Store,
		keyFn:    cfg.KeyFunc,
		push:     cfg.Push,
		fidelity: cfg.Fidelity,
		ch:       make(chan CacheRecord, cfg.BufSize),
	}
	r.wg.Add(1)
	go r.drain()
	return r
}

// Record enqueues a record for async insertion. This is the hot-path entry
// point; callers should not block on it. Returns true if the record was
// queued, false if it was dropped due to backpressure (or if the recorder
// has been stopped).
func (r *Recorder) Record(rec CacheRecord) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stopped {
		return false
	}
	pair := PhasePair{ExperimentID: rec.ExperimentID, PhaseID: rec.PhaseID}
	select {
	case r.ch <- rec:
		r.fidelity.RecordEnqueued(pair)
		return true
	default:
		r.dropped.Add(1)
		r.fidelity.RecordDropped(pair, 1)
		return false
	}
}

// drain is the background goroutine that processes queued records. It
// terminates when r.ch is closed.
func (r *Recorder) drain() {
	defer r.wg.Done()
	for rec := range r.ch {
		if rec.flushBarrier != nil {
			close(rec.flushBarrier)
			continue
		}
		// The record's own Key (the rule-context derivation captured on the
		// hot path) is authoritative; keyFn is only the fallback for records
		// without one. Re-deriving here with a different strategy than the
		// replay side would silently 100%-miss under freeze (INV-2).
		key := rec.Key
		if key == "" {
			key = r.keyFn(rec.Request, rec.RequestBody)
		}
		var header http.Header
		if rec.ResponseHeader != nil {
			header = rec.ResponseHeader.Clone()
		}
		entry := &Entry{
			Key:             key,
			StatusCode:      rec.StatusCode,
			Header:          header,
			Body:            rec.ResponseBody,
			ObservedLatency: rec.ObservedLatency,
			RecordedAt:      rec.Timestamp,
			ExperimentID:    rec.ExperimentID,
			PhaseID:         rec.PhaseID,
			KeyStrategy:     rec.KeyStrategy,
			StrategyVersion: rec.StrategyVersion,
		}
		r.store.Put(key, entry)
		r.recorded.Add(1)
		if r.push != nil {
			r.push(key, entry)
		}
	}
}

// Flush blocks until every CacheRecord enqueued before this call has been
// processed by the drain goroutine (and, if a push hook is set, handed to
// it). Unlike Stop, it does not terminate the recorder -- Record calls
// after Flush returns work normally. Used at recording-phase end (design
// doc Q2) so a drain report can be built only after every record up to
// that point is accounted for. A no-op after Stop.
//
// Flush sends a barrier directly on the channel (a blocking send, unlike
// Record's drop-on-full) so it can never be silently discarded by the same
// backpressure policy that protects the hot path -- a dropped barrier
// would hang this call forever.
func (r *Recorder) Flush() {
	// Hold RLock across the check and the barrier send so Stop cannot close
	// ch in between (RLock blocks Stop's exclusive Lock). Release it before
	// waiting on done: once the barrier is queued, drain will process it and
	// close done even if Stop closes ch immediately after, so there is no
	// need to keep other senders (or Stop) blocked while we wait.
	r.mu.RLock()
	if r.stopped {
		r.mu.RUnlock()
		return
	}
	done := make(chan struct{})
	r.ch <- CacheRecord{flushBarrier: done}
	r.mu.RUnlock()
	<-done
}

// Stop signals the recorder to finish draining pending records and
// terminates the drain goroutine. It blocks until the drain goroutine
// exits. Subsequent Record calls are no-ops. Stop is safe to call
// multiple times.
func (r *Recorder) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.ch)
	r.mu.Unlock()
	r.wg.Wait()
}

// RecorderStats reports recorder counters.
type RecorderStats struct {
	Recorded int64
	Dropped  int64
	Pending  int
}

// Stats returns a snapshot of recorder counters.
func (r *Recorder) Stats() RecorderStats {
	return RecorderStats{
		Recorded: r.recorded.Load(),
		Dropped:  r.dropped.Load(),
		Pending:  len(r.ch),
	}
}
