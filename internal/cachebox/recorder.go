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
// Request is kept so the recorder can derive the key off the hot path.
// RequestBody is only set when the key strategy needs it (exact_with_body).
// Neither the Request nor the Body is cloned by the recorder; callers must
// ensure the values are safe to read from the drain goroutine.
//
// ExperimentID/PhaseID are the matched rule's CacheBoxContext provenance
// (design doc Q5/INV-5). The interceptor only builds a CacheRecord when
// both are non-empty (see cacheBoxPassthrough) -- there is no ambient
// fallback for a missing pair.
type CacheRecord struct {
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

	ch      chan CacheRecord
	wg      sync.WaitGroup
	stopped atomic.Bool

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
	if r.stopped.Load() {
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
		key := r.keyFn(rec.Request, rec.RequestBody)
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
	if r.stopped.Load() {
		return
	}
	done := make(chan struct{})
	r.ch <- CacheRecord{flushBarrier: done}
	<-done
}

// Stop signals the recorder to finish draining pending records and
// terminates the drain goroutine. It blocks until the drain goroutine
// exits. Subsequent Record calls are no-ops. Stop is safe to call
// multiple times.
func (r *Recorder) Stop() {
	if !r.stopped.CompareAndSwap(false, true) {
		return
	}
	close(r.ch)
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
