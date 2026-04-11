// Package cachebox implements the cache-box engine: a request/response
// recorder and replayer used by the atropos-go interceptor to "freeze"
// outbound dependencies at the SDK boundary.
//
// Cache-box is the interventional dual of fault injection. Where fault
// injection degrades behavior (latency, error, hang), cache-box replaces
// behavior with a previously-recorded response. This lets experiments
// attribute end-to-end latency contributions to individual services by
// selectively replaying their call sites instead of executing them.
//
// The package is stdlib-only and safe for concurrent use. It is intended
// to be embedded in service pods via the atropos-go SDK; no external
// dependencies are required to build or test it.
package cachebox

import (
	"net/http"
	"sync/atomic"
	"time"
)

// Entry is one cached HTTP response tagged with the latency observed when
// it was recorded. Entries are immutable after Put and may be shared by
// concurrent Get callers; callers MUST NOT mutate Header or Body directly.
// Use Header.Clone() if you need a mutable copy.
type Entry struct {
	Key             string
	StatusCode      int
	Header          http.Header
	Body            []byte
	ObservedLatency time.Duration
	RecordedAt      time.Time
	HitCount        atomic.Int64
}

// Size returns an approximate byte count for the entry, used by the store
// for accounting. It includes the body, key, and header key/value lengths.
// Cost is O(n) over header entries; callers should not call it in hot paths.
func (e *Entry) Size() int {
	if e == nil {
		return 0
	}
	n := len(e.Body) + len(e.Key)
	for k, vs := range e.Header {
		n += len(k)
		for _, v := range vs {
			n += len(v)
		}
	}
	return n
}

// Store is the cache-box persistence contract. Implementations must be
// safe for concurrent use. Get increments hit/miss counters; Put may
// evict other entries if the implementation enforces a size cap.
type Store interface {
	// Get returns a matching entry and true if one exists. The returned
	// entry is owned by the store; callers must not mutate it.
	Get(key string) (*Entry, bool)

	// Put inserts or replaces an entry. Callers transfer ownership of the
	// entry (and its Header and Body) to the store.
	Put(key string, entry *Entry)

	// Delete removes an entry. Missing keys are a no-op.
	Delete(key string)

	// Len returns the number of entries currently resident in the store.
	Len() int

	// Clear empties the store and resets byte accounting, but does not
	// reset hit/miss/eviction counters (those reflect lifetime activity).
	Clear()

	// Stats returns a snapshot of cache health.
	Stats() StoreStats
}

// StoreStats reports cache health counters.
type StoreStats struct {
	Entries   int
	Hits      int64
	Misses    int64
	BytesUsed int64
	Evictions int64
}
