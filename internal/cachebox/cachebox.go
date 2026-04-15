package cachebox

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"
)

// DefaultMaxBodyBytes caps the size of response bodies that cache-box is
// willing to buffer. Responses larger than this are passed through to the
// caller without being cached -- the entry is never created and a span
// event is emitted for observability.
//
// 1 MiB is a conservative default suitable for REST JSON traffic. Services
// returning larger payloads (file downloads, HTML pages, binary assets)
// should either raise the limit via Config.MaxBodyBytes or avoid
// configuring cache-box for those endpoints.
const DefaultMaxBodyBytes = 1 << 20 // 1 MiB

// CacheBox is the runtime coordinator for cache-box operations. It owns
// the store, recorder, delay source, and key function used by the
// interceptor to serve cache-box decisions.
//
// A nil *CacheBox is safe for Lookup and Record (both become no-ops)
// but not for SampleDelay or NeedsRequestBody -- the interceptor must
// check for nil before dispatching to handleCacheBox.
type CacheBox struct {
	store        Store
	recorder     *Recorder
	keyFn        KeyFunc
	strategy     KeyStrategy
	maxBodyBytes int
	otelCapture  int

	mu    sync.RWMutex
	delay DelaySource
}

// Config builds a CacheBox.
type Config struct {
	// Store is the persistence backend. If nil, a 10k-entry MemStore is used.
	Store Store

	// KeyStrategy selects one of the built-in key functions. Ignored if
	// KeyFunc is set.
	KeyStrategy KeyStrategy

	// KeyFunc overrides KeyStrategy if set. Use this for custom keying
	// schemes not covered by the built-in strategies.
	KeyFunc KeyFunc

	// Delay is the replay_with_delay sampler. Defaults to ObservedDelaySource.
	Delay DelaySource

	// Recorder, if non-nil, overrides the built-in recorder construction.
	// Supply your own if you need a custom buffer size or push hook that
	// differs from Push below.
	Recorder *Recorder

	// MaxBodyBytes caps response body buffering. 0 = DefaultMaxBodyBytes.
	MaxBodyBytes int

	// OTelCapture, if > 0, attaches the response body to the cache-box
	// span as an attribute when the body is <= this many bytes. Useful
	// for research/debug runs; leaves traces unchanged in production.
	OTelCapture int

	// Push is passed through to the built-in recorder if Recorder is nil.
	Push PushFunc

	// RecorderBuf is passed through to the built-in recorder if Recorder
	// is nil. 0 uses the recorder default (1024).
	RecorderBuf int
}

// New constructs a CacheBox from Config. All unset fields get sensible
// defaults. The returned *CacheBox owns its recorder's drain goroutine;
// callers should invoke Stop when the CacheBox is no longer needed.
func New(cfg Config) *CacheBox {
	if cfg.Store == nil {
		cfg.Store = NewMemStore(MemStoreConfig{MaxEntries: 10000})
	}
	if cfg.KeyStrategy == "" {
		cfg.KeyStrategy = KeyStrategyExact
	}
	if cfg.Delay == nil {
		cfg.Delay = ObservedDelaySource{}
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}

	keyFn := cfg.KeyFunc
	if keyFn == nil {
		keyFn = KeyFuncFor(cfg.KeyStrategy)
	}

	rec := cfg.Recorder
	if rec == nil {
		rec = NewRecorder(RecorderConfig{
			Store:   cfg.Store,
			KeyFunc: keyFn,
			Push:    cfg.Push,
			BufSize: cfg.RecorderBuf,
		})
	}

	return &CacheBox{
		store:        cfg.Store,
		recorder:     rec,
		keyFn:        keyFn,
		strategy:     cfg.KeyStrategy,
		maxBodyBytes: cfg.MaxBodyBytes,
		otelCapture:  cfg.OTelCapture,
		delay:        cfg.Delay,
	}
}

// DeriveKey runs the configured key function over the request (and body,
// if the strategy needs it). Nil-safe.
func (cb *CacheBox) DeriveKey(r *http.Request, body []byte) string {
	if cb == nil {
		return ""
	}
	return cb.keyFn(r, body)
}

// Lookup returns a cached entry for the key, if one exists. Nil-safe.
func (cb *CacheBox) Lookup(key string) (*Entry, bool) {
	if cb == nil {
		return nil, false
	}
	return cb.store.Get(key)
}

// Record enqueues a new entry for async insertion. Nil-safe.
func (cb *CacheBox) Record(rec CacheRecord) {
	if cb == nil || cb.recorder == nil {
		return
	}
	cb.recorder.Record(rec)
}

// SampleDelay asks the delay source how long replay_with_delay should sleep
// for this entry. Not nil-safe on the receiver -- callers must check.
func (cb *CacheBox) SampleDelay(entry *Entry) time.Duration {
	cb.mu.RLock()
	d := cb.delay
	cb.mu.RUnlock()
	return d.Sample(entry)
}

// SetDelaySource atomically swaps the delay source. Used by manteion
// integration (Stage 3) to push fitted distribution parameters at rule
// update time.
func (cb *CacheBox) SetDelaySource(d DelaySource) {
	if d == nil {
		return
	}
	cb.mu.Lock()
	cb.delay = d
	cb.mu.Unlock()
}

// NeedsRequestBody reports whether the current key strategy requires the
// request body to be buffered on the hot path.
func (cb *CacheBox) NeedsRequestBody() bool {
	if cb == nil {
		return false
	}
	return cb.strategy.NeedsBody()
}

// MaxBodyBytes returns the response body buffering cap.
func (cb *CacheBox) MaxBodyBytes() int {
	return cb.maxBodyBytes
}

// OTelCaptureLimit returns the OTel response-body capture threshold, or 0
// if capture is disabled.
func (cb *CacheBox) OTelCaptureLimit() int {
	return cb.otelCapture
}

// Store returns the underlying store. Useful for inspection in tests.
func (cb *CacheBox) Store() Store {
	return cb.store
}

// Stop terminates the recorder's drain goroutine. Safe to call multiple times.
func (cb *CacheBox) Stop() {
	if cb == nil || cb.recorder == nil {
		return
	}
	cb.recorder.Stop()
}

// Stats returns combined store and recorder stats.
type Stats struct {
	Store    StoreStats
	Recorder RecorderStats
}

// Stats returns a snapshot of cache-box observability counters.
func (cb *CacheBox) Stats() Stats {
	return Stats{
		Store:    cb.store.Stats(),
		Recorder: cb.recorder.Stats(),
	}
}

// BufferRequestBody reads up to (cap + 1) bytes of r.Body, restores the
// body on r, and returns the captured slice. If the body exceeds cap, the
// returned slice is nil and the request body is restored so base.RoundTrip
// can still stream it downstream -- this lets the passthrough path work
// even for large bodies, at the cost of not being able to cache by body
// hash for that particular request.
//
// A nil r.Body is a no-op (returns nil, nil).
func BufferRequestBody(r *http.Request, cap int) ([]byte, error) {
	if r == nil || r.Body == nil {
		return nil, nil
	}
	limited := io.LimitReader(r.Body, int64(cap)+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(body) > cap {
		// Exceeded cap -- prepend what we already read to the remainder of the
		// original body so downstream sees the full stream. The original
		// Closer is preserved so close semantics are correct.
		origCloser := r.Body
		r.Body = struct {
			io.Reader
			io.Closer
		}{
			Reader: io.MultiReader(bytes.NewReader(body), r.Body),
			Closer: origCloser,
		}
		return nil, nil
	}
	// Fully captured -- close the original body and substitute a replayable
	// NopCloser over the captured bytes.
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
