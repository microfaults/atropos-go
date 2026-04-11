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
type CacheRecord struct {
	Request         *http.Request
	RequestBody     []byte
	StatusCode      int
	ResponseHeader  http.Header
	ResponseBody    []byte
	ObservedLatency time.Duration
	Timestamp       time.Time
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
	store Store
	keyFn KeyFunc
	push  PushFunc

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
}

// NewRecorder constructs a Recorder and starts its drain goroutine.
// Callers MUST call Stop when the recorder is no longer needed.
func NewRecorder(cfg RecorderConfig) *Recorder {
	if cfg.BufSize <= 0 {
		cfg.BufSize = 1024
	}
	r := &Recorder{
		store: cfg.Store,
		keyFn: cfg.KeyFunc,
		push:  cfg.Push,
		ch:    make(chan CacheRecord, cfg.BufSize),
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
	select {
	case r.ch <- rec:
		return true
	default:
		r.dropped.Add(1)
		return false
	}
}

// drain is the background goroutine that processes queued records. It
// terminates when r.ch is closed.
func (r *Recorder) drain() {
	defer r.wg.Done()
	for rec := range r.ch {
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
		}
		r.store.Put(key, entry)
		r.recorded.Add(1)
		if r.push != nil {
			r.push(key, entry)
		}
	}
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
