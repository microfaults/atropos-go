package cachebox

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// MemStoreConfig configures the in-memory store.
//
// MaxEntries = 0 disables the entry cap (unbounded growth -- dangerous
// in production but convenient for tests).
//
// TTL = 0 disables time-based expiry; entries live until evicted by LRU
// pressure or explicit Delete/Clear. A positive TTL is checked lazily on
// Get; the store does NOT run a background sweep to reclaim stale entries,
// so a cold entry may sit in memory indefinitely until accessed. This is
// acceptable for cache-box experiments where warmup populates the cache
// and replay exercises every hot key.
type MemStoreConfig struct {
	MaxEntries int
	TTL        time.Duration
}

// MemStore is an LRU-bounded in-memory Store implementation. It uses
// container/list for O(1) eviction and a map for O(1) lookup.
type MemStore struct {
	cfg MemStoreConfig

	mu      sync.Mutex
	entries map[string]*list.Element
	lru     *list.List // front = most recent, back = oldest

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	bytes     atomic.Int64
}

// listNode is the payload stored in each LRU element.
type listNode struct {
	key   string
	entry *Entry
}

// NewMemStore builds an in-memory Store.
func NewMemStore(cfg MemStoreConfig) *MemStore {
	return &MemStore{
		cfg:     cfg,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
}

// Get returns the entry for key, updating LRU order. Returns (nil, false)
// on miss or expired entry. Expired entries are removed lazily.
func (s *MemStore) Get(key string) (*Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.entries[key]
	if !ok {
		s.misses.Add(1)
		return nil, false
	}
	entry := el.Value.(*listNode).entry
	if s.cfg.TTL > 0 && time.Since(entry.RecordedAt) > s.cfg.TTL {
		s.removeLocked(el)
		s.misses.Add(1)
		return nil, false
	}
	s.lru.MoveToFront(el)
	s.hits.Add(1)
	entry.HitCount.Add(1)
	return entry, true
}

// Put inserts or replaces an entry. If MaxEntries is exceeded after
// insertion, the oldest entries are evicted until the cap is satisfied.
func (s *MemStore) Put(key string, entry *Entry) {
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.entries[key]; ok {
		// Replace in place; update byte accounting for the diff.
		old := el.Value.(*listNode).entry
		s.bytes.Add(-int64(old.Size()))
		el.Value.(*listNode).entry = entry
		s.lru.MoveToFront(el)
		s.bytes.Add(int64(entry.Size()))
		return
	}
	el := s.lru.PushFront(&listNode{key: key, entry: entry})
	s.entries[key] = el
	s.bytes.Add(int64(entry.Size()))

	for s.cfg.MaxEntries > 0 && s.lru.Len() > s.cfg.MaxEntries {
		back := s.lru.Back()
		if back == nil {
			break
		}
		s.removeLocked(back)
		s.evictions.Add(1)
	}
}

// removeLocked removes el from both map and list. Caller must hold s.mu.
func (s *MemStore) removeLocked(el *list.Element) {
	node := el.Value.(*listNode)
	delete(s.entries, node.key)
	s.lru.Remove(el)
	s.bytes.Add(-int64(node.entry.Size()))
}

// Delete removes an entry. Missing keys are a no-op.
func (s *MemStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.entries[key]; ok {
		s.removeLocked(el)
	}
}

// Len returns the current number of entries.
func (s *MemStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

// Clear empties the store. Lifetime counters are preserved.
func (s *MemStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*list.Element)
	s.lru = list.New()
	s.bytes.Store(0)
}

// Stats returns a snapshot of cache counters.
func (s *MemStore) Stats() StoreStats {
	s.mu.Lock()
	n := s.lru.Len()
	s.mu.Unlock()
	return StoreStats{
		Entries:   n,
		Hits:      s.hits.Load(),
		Misses:    s.misses.Load(),
		BytesUsed: s.bytes.Load(),
		Evictions: s.evictions.Load(),
	}
}
