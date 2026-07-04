package cachebox

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
)

// RecordBuffer holds newly-recorded entries pending push to manteion. It is
// the record half of the record/replay split (design doc Q4, INV-4/INV-7):
// unlike a cache it never feeds replay (see ReplaySet) and never evicts
// silently -- once at capacity, a new key is dropped and counted rather
// than displacing an older, not-yet-pushed entry.
//
// RecordBuffer implements the Store interface so it can be handed to
// NewRecorder unchanged; Get/Delete/Clear/Stats exist for that conformance
// and for tests/diagnostics, not because the replay path consults them (it
// doesn't -- only ReplaySet is consulted for replay).
type RecordBuffer struct {
	maxEntries int

	mu      sync.Mutex
	entries map[string]*Entry

	overflowDropped     atomic.Int64
	collisionsDivergent atomic.Int64
	collisionsIdentical atomic.Int64
}

// RecordBufferConfig configures a RecordBuffer.
type RecordBufferConfig struct {
	// MaxEntries caps the number of distinct keys held at once. 0 means
	// unbounded. Exceeding the cap drops the incoming (new-key) record and
	// increments OverflowDropped; it never evicts an existing key to make
	// room.
	MaxEntries int
}

// NewRecordBuffer builds an empty RecordBuffer.
func NewRecordBuffer(cfg RecordBufferConfig) *RecordBuffer {
	return &RecordBuffer{
		maxEntries: cfg.MaxEntries,
		entries:    make(map[string]*Entry),
	}
}

// Get returns the buffered entry for key, if any.
func (b *RecordBuffer) Get(key string) (*Entry, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[key]
	return e, ok
}

// Put inserts or replaces the entry for key (latest-wins, design doc Q3).
// If key already holds an entry, Put compares (StatusCode, sha256(Body))
// against it: a difference increments CollisionsDivergent, an exact match
// increments CollisionsIdentical -- either way the newer entry replaces the
// older one. If key is new and the buffer is at MaxEntries capacity, the
// record is dropped (OverflowDropped++) and the existing contents are left
// untouched.
func (b *RecordBuffer) Put(key string, entry *Entry) {
	if entry == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.entries[key]; ok {
		if existing.StatusCode == entry.StatusCode && sha256.Sum256(existing.Body) == sha256.Sum256(entry.Body) {
			b.collisionsIdentical.Add(1)
		} else {
			b.collisionsDivergent.Add(1)
		}
		b.entries[key] = entry
		return
	}

	if b.maxEntries > 0 && len(b.entries) >= b.maxEntries {
		b.overflowDropped.Add(1)
		return
	}
	b.entries[key] = entry
}

// Delete removes an entry. Missing keys are a no-op.
func (b *RecordBuffer) Delete(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.entries, key)
}

// Len returns the current number of distinct buffered keys.
func (b *RecordBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// Clear empties the buffer. Lifetime counters are preserved.
func (b *RecordBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = make(map[string]*Entry)
}

// Stats implements the Store interface's generic view. Evictions reports
// overflow drops (there is no LRU eviction to distinguish it from).
func (b *RecordBuffer) Stats() StoreStats {
	b.mu.Lock()
	n := len(b.entries)
	b.mu.Unlock()
	return StoreStats{
		Entries:   n,
		Evictions: b.overflowDropped.Load(),
	}
}

// RecordBufferStats reports the richer counters generic StoreStats can't
// carry: overflow drops and per-key collision counts (design doc Q3/INV-7).
type RecordBufferStats struct {
	Entries             int
	OverflowDropped     int64
	CollisionsDivergent int64
	CollisionsIdentical int64
}

// BufferStats returns a snapshot of RecordBuffer-specific counters.
func (b *RecordBuffer) BufferStats() RecordBufferStats {
	b.mu.Lock()
	n := len(b.entries)
	b.mu.Unlock()
	return RecordBufferStats{
		Entries:             n,
		OverflowDropped:     b.overflowDropped.Load(),
		CollisionsDivergent: b.collisionsDivergent.Load(),
		CollisionsIdentical: b.collisionsIdentical.Load(),
	}
}
