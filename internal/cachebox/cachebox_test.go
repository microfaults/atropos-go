package cachebox

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Store tests ---

func TestMemStore_PutGet(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	entry := &Entry{
		Key:             "k",
		StatusCode:      200,
		Header:          http.Header{"Content-Type": []string{"application/json"}},
		Body:            []byte("hello"),
		ObservedLatency: 10 * time.Millisecond,
		RecordedAt:      time.Now(),
	}
	s.Put("k", entry)

	got, ok := s.Get("k")
	if !ok {
		t.Fatal("expected hit after Put")
	}
	if got != entry {
		t.Fatal("Get should return the exact stored entry pointer")
	}
	stats := s.Stats()
	if stats.Entries != 1 {
		t.Fatalf("entries = %d, want 1", stats.Entries)
	}
	if stats.Hits != 1 {
		t.Fatalf("hits = %d, want 1", stats.Hits)
	}
}

func TestMemStore_Miss(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	if _, ok := s.Get("absent"); ok {
		t.Fatal("expected miss on empty store")
	}
	if s.Stats().Misses != 1 {
		t.Fatalf("expected 1 miss, got %d", s.Stats().Misses)
	}
}

func TestMemStore_Replace(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	s.Put("k", &Entry{Key: "k", Body: []byte("v1")})
	s.Put("k", &Entry{Key: "k", Body: []byte("v2-longer")})
	got, ok := s.Get("k")
	if !ok {
		t.Fatal("expected entry after replace")
	}
	if string(got.Body) != "v2-longer" {
		t.Fatalf("got %q, want v2-longer", got.Body)
	}
	if s.Len() != 1 {
		t.Fatalf("len = %d, want 1", s.Len())
	}
	// Byte accounting should reflect the larger entry.
	if s.Stats().BytesUsed < int64(len("v2-longer")+1) {
		t.Fatalf("bytes accounting looks wrong after replace: %d", s.Stats().BytesUsed)
	}
}

func TestMemStore_Delete(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	s.Put("k", &Entry{Key: "k", Body: []byte("v")})
	s.Delete("k")
	if _, ok := s.Get("k"); ok {
		t.Fatal("expected miss after delete")
	}
	if s.Len() != 0 {
		t.Fatal("expected empty store")
	}
}

func TestMemStore_LRU_Eviction(t *testing.T) {
	s := NewMemStore(MemStoreConfig{MaxEntries: 2})
	s.Put("a", &Entry{Key: "a", Body: []byte("1")})
	s.Put("b", &Entry{Key: "b", Body: []byte("2")})
	// Touch "a" so it becomes the most recent.
	_, _ = s.Get("a")
	s.Put("c", &Entry{Key: "c", Body: []byte("3")})

	// "b" should have been evicted (it was the oldest after touching "a").
	if _, ok := s.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("expected a to survive")
	}
	if _, ok := s.Get("c"); !ok {
		t.Fatal("expected c to survive")
	}
	if s.Stats().Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", s.Stats().Evictions)
	}
}

func TestMemStore_TTL_LazyExpiry(t *testing.T) {
	s := NewMemStore(MemStoreConfig{TTL: 20 * time.Millisecond})
	s.Put("k", &Entry{Key: "k", Body: []byte("v"), RecordedAt: time.Now()})

	// Fresh get succeeds.
	if _, ok := s.Get("k"); !ok {
		t.Fatal("expected fresh entry to hit")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Fatal("expected stale entry to miss")
	}
	// The expired entry should have been removed as a side effect of Get.
	if s.Len() != 0 {
		t.Fatalf("expected empty after lazy eviction, got %d", s.Len())
	}
}

func TestMemStore_Clear(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	s.Put("a", &Entry{Key: "a", Body: []byte("1")})
	s.Put("b", &Entry{Key: "b", Body: []byte("2")})
	s.Clear()
	if s.Len() != 0 {
		t.Fatal("expected empty after Clear")
	}
	if s.Stats().BytesUsed != 0 {
		t.Fatal("expected zero bytes after Clear")
	}
}

// --- Key strategy tests ---

func mustRequest(t *testing.T, method, rawURL string) *http.Request {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Request{
		Method: method,
		URL:    u,
		Host:   u.Host,
	}
}

func TestKeyExact_BasicMethodPath(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyExact)
	a := fn(mustRequest(t, "GET", "http://svc/api/v1/products"), nil)
	b := fn(mustRequest(t, "POST", "http://svc/api/v1/products"), nil)
	if a == b {
		t.Fatal("GET and POST should produce distinct keys")
	}
	if !strings.HasPrefix(a, "exact:GET|/api/v1/products") {
		t.Fatalf("unexpected key %q", a)
	}
}

func TestKeyExact_QueryStringDistinct(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyExact)
	a := fn(mustRequest(t, "GET", "http://svc/products?id=1"), nil)
	b := fn(mustRequest(t, "GET", "http://svc/products?id=2"), nil)
	if a == b {
		t.Fatalf("query params should change the key: %q vs %q", a, b)
	}
}

func TestKeyExact_QueryOrderNormalized(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyExact)
	a := fn(mustRequest(t, "GET", "http://svc/products?a=1&b=2"), nil)
	b := fn(mustRequest(t, "GET", "http://svc/products?b=2&a=1"), nil)
	if a != b {
		t.Fatalf("query param order should not affect the key: %q vs %q", a, b)
	}
}

func TestKeyExactWithHost_HostMatters(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyExactWithHost)
	a := fn(mustRequest(t, "GET", "http://svc-a/path"), nil)
	b := fn(mustRequest(t, "GET", "http://svc-b/path"), nil)
	if a == b {
		t.Fatalf("host should distinguish keys: %q vs %q", a, b)
	}
}

func TestKeyExactWithBody_Determinism(t *testing.T) {
	fn := KeyFuncFor(KeyStrategyExactWithBody)
	body := []byte(`{"q":"socks"}`)
	a := fn(mustRequest(t, "POST", "http://svc/search"), body)
	b := fn(mustRequest(t, "POST", "http://svc/search"), body)
	if a != b {
		t.Fatal("same body should yield same key")
	}
	c := fn(mustRequest(t, "POST", "http://svc/search"), []byte(`{"q":"shoes"}`))
	if a == c {
		t.Fatal("different bodies should yield different keys")
	}
}

func TestKeyStrategy_NeedsBody(t *testing.T) {
	if KeyStrategyExact.NeedsBody() {
		t.Fatal("exact should not need body")
	}
	if KeyStrategyExactWithHost.NeedsBody() {
		t.Fatal("exact_with_host should not need body")
	}
	if !KeyStrategyExactWithBody.NeedsBody() {
		t.Fatal("exact_with_body must need body")
	}
}

func TestKeyFuncFor_UnknownFallsBack(t *testing.T) {
	fn := KeyFuncFor(KeyStrategy("made_up"))
	r := mustRequest(t, "GET", "http://svc/x")
	key := fn(r, nil)
	// Unknown strategy should produce an "exact:" prefixed key.
	if !strings.HasPrefix(key, "exact:") {
		t.Fatalf("unknown strategy did not fall back to exact: %q", key)
	}
}

// --- Delay source tests ---

func TestObservedDelaySource(t *testing.T) {
	var s ObservedDelaySource
	got := s.Sample(&Entry{ObservedLatency: 123 * time.Millisecond})
	if got != 123*time.Millisecond {
		t.Fatalf("got %s, want 123ms", got)
	}
	if s.Sample(nil) != 0 {
		t.Fatal("nil entry should sample 0")
	}
}

func TestDistributionDelaySource_FallbackWhenUnfitted(t *testing.T) {
	d := NewDistributionDelaySource(0, 0, 42)
	got := d.Sample(&Entry{ObservedLatency: 50 * time.Millisecond})
	if got != 50*time.Millisecond {
		t.Fatalf("expected fallback to observed latency, got %s", got)
	}
}

func TestDistributionDelaySource_SampleInReasonableBounds(t *testing.T) {
	// Fit: mean ~1000us, moderate variance. Box-Muller output should land
	// on positive microseconds values for reasonable seeds.
	mu := math.Log(1000)
	sigma := 0.3
	d := NewDistributionDelaySource(mu, sigma, 1)
	var total time.Duration
	const n = 200
	for i := 0; i < n; i++ {
		sample := d.Sample(nil)
		if sample < 0 {
			t.Fatalf("negative delay sample: %s", sample)
		}
		total += sample
	}
	avg := total / n
	// Lognormal mean is exp(mu + sigma^2/2). With mu=ln(1000), sigma=0.3 the
	// expected mean is about 1046us. Allow a very wide tolerance -- this is a
	// sanity check, not a statistical test.
	if avg < 100*time.Microsecond || avg > 10*time.Millisecond {
		t.Fatalf("unexpected average delay %s (want ~1ms order)", avg)
	}
}

func TestDistributionDelaySource_SetParamsSwitchesMode(t *testing.T) {
	d := NewDistributionDelaySource(0, 0, 7)
	// Unfitted -> fallback.
	if got := d.Sample(&Entry{ObservedLatency: 5 * time.Millisecond}); got != 5*time.Millisecond {
		t.Fatalf("expected fallback, got %s", got)
	}
	d.SetParams(math.Log(2000), 0.1)
	// Now we should be sampling from the distribution; fallback would return 0
	// for a nil entry, distribution should return something positive.
	got := d.Sample(nil)
	if got == 0 {
		t.Fatal("expected positive sample from fitted distribution")
	}
}

// --- Recorder tests ---

func TestRecorder_BasicFlow(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	r := NewRecorder(RecorderConfig{
		Store:   s,
		KeyFunc: KeyFuncFor(KeyStrategyExact),
		BufSize: 4,
	})
	defer r.Stop()

	for i := 0; i < 3; i++ {
		ok := r.Record(CacheRecord{
			Request:         mustRequest(t, "GET", "http://svc/a"),
			StatusCode:      200,
			ResponseHeader:  http.Header{"Content-Type": []string{"text/plain"}},
			ResponseBody:    []byte("ok"),
			ObservedLatency: 1 * time.Millisecond,
			Timestamp:       time.Now(),
		})
		if !ok {
			t.Fatal("unexpected drop on empty buffer")
		}
	}

	// Stop drains pending records and waits for the goroutine to exit.
	r.Stop()
	if got := s.Len(); got != 1 {
		t.Fatalf("expected 1 entry (same key), got %d", got)
	}
	stats := r.Stats()
	if stats.Recorded != 3 {
		t.Fatalf("recorded = %d, want 3", stats.Recorded)
	}
	if stats.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", stats.Dropped)
	}
}

func TestRecorder_BackpressureDrops(t *testing.T) {
	// Use a store wrapper that blocks Put so the drain goroutine cannot
	// make progress, forcing the channel to fill up.
	blocking := &blockingStore{}
	r := NewRecorder(RecorderConfig{
		Store:   blocking,
		KeyFunc: KeyFuncFor(KeyStrategyExact),
		BufSize: 2,
	})
	// Don't defer Stop -- we'll unblock then stop explicitly at the end.

	// Fill the channel plus at least one drop.
	const total = 10
	var accepted int
	for i := 0; i < total; i++ {
		if r.Record(CacheRecord{
			Request:        mustRequest(t, "GET", "http://svc/x"),
			ResponseHeader: http.Header{},
		}) {
			accepted++
		}
	}
	if accepted >= total {
		t.Fatal("expected at least one drop under backpressure")
	}
	if r.Stats().Dropped == 0 {
		t.Fatal("dropped counter should be >0")
	}

	// Unblock the store so Stop can drain.
	blocking.unblock()
	r.Stop()
}

func TestRecorder_RecordAfterStopIsNoop(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	r := NewRecorder(RecorderConfig{
		Store:   s,
		KeyFunc: KeyFuncFor(KeyStrategyExact),
	})
	r.Stop()
	if r.Record(CacheRecord{
		Request:        mustRequest(t, "GET", "http://svc/x"),
		ResponseHeader: http.Header{},
	}) {
		t.Fatal("Record should return false after Stop")
	}
}

func TestRecorder_PushHook(t *testing.T) {
	s := NewMemStore(MemStoreConfig{})
	var pushed []string
	var mu sync.Mutex
	push := func(key string, _ *Entry) {
		mu.Lock()
		pushed = append(pushed, key)
		mu.Unlock()
	}
	r := NewRecorder(RecorderConfig{
		Store:   s,
		KeyFunc: KeyFuncFor(KeyStrategyExact),
		Push:    push,
	})
	r.Record(CacheRecord{
		Request:        mustRequest(t, "GET", "http://svc/x"),
		StatusCode:     200,
		ResponseHeader: http.Header{},
		ResponseBody:   []byte("body"),
		Timestamp:      time.Now(),
	})
	r.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(pushed) != 1 {
		t.Fatalf("push called %d times, want 1", len(pushed))
	}
	if !strings.HasPrefix(pushed[0], "exact:GET|/x") {
		t.Fatalf("unexpected pushed key %q", pushed[0])
	}
}

// blockingStore is a Store that blocks on Put until unblock() is called.
// Used to simulate backpressure for the recorder.
type blockingStore struct {
	release chan struct{}
	once    sync.Once
}

func (b *blockingStore) ensure() {
	b.once.Do(func() {
		b.release = make(chan struct{})
	})
}

func (b *blockingStore) Get(string) (*Entry, bool) { return nil, false }

func (b *blockingStore) Put(string, *Entry) {
	b.ensure()
	<-b.release
}

func (b *blockingStore) Delete(string)  {}
func (b *blockingStore) Len() int       { return 0 }
func (b *blockingStore) Clear()         {}
func (b *blockingStore) Stats() StoreStats {
	return StoreStats{}
}

func (b *blockingStore) unblock() {
	b.ensure()
	close(b.release)
}

// --- CacheBox coordinator + BufferRequestBody tests ---

func TestNewDefaults(t *testing.T) {
	cb := New(Config{})
	defer cb.Stop()
	if cb.MaxBodyBytes() != DefaultMaxBodyBytes {
		t.Fatalf("default body cap = %d, want %d", cb.MaxBodyBytes(), DefaultMaxBodyBytes)
	}
	if cb.NeedsRequestBody() {
		t.Fatal("default strategy should not need body")
	}
	if cb.OTelCaptureLimit() != 0 {
		t.Fatal("otel capture should be disabled by default")
	}
}

func TestBufferRequestBody_Small(t *testing.T) {
	r := &http.Request{
		Body: io.NopCloser(strings.NewReader("hello world")),
	}
	body, err := BufferRequestBody(r, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello world" {
		t.Fatalf("got %q", body)
	}
	// Body should be replayable.
	replay, _ := io.ReadAll(r.Body)
	if string(replay) != "hello world" {
		t.Fatalf("replay %q", replay)
	}
}

func TestBufferRequestBody_Oversized(t *testing.T) {
	large := strings.Repeat("x", 100)
	r := &http.Request{
		Body: io.NopCloser(strings.NewReader(large)),
	}
	body, err := BufferRequestBody(r, 10)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		t.Fatal("expected nil body when cap exceeded")
	}
	// Downstream should still be able to read the full stream.
	replay, _ := io.ReadAll(r.Body)
	if string(replay) != large {
		t.Fatalf("replay missing bytes: len = %d, want %d", len(replay), len(large))
	}
}

func TestBufferRequestBody_Nil(t *testing.T) {
	r := &http.Request{}
	body, err := BufferRequestBody(r, 10)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		t.Fatal("nil body should return nil slice")
	}
}

func TestCacheBox_LookupAndRecord(t *testing.T) {
	cb := New(Config{
		Store:       NewMemStore(MemStoreConfig{}),
		KeyStrategy: KeyStrategyExact,
	})
	defer cb.Stop()

	req := mustRequest(t, "GET", "http://svc/items")
	key := cb.DeriveKey(req, nil)

	if _, ok := cb.Lookup(key); ok {
		t.Fatal("unexpected hit on empty store")
	}

	cb.Record(CacheRecord{
		Request:         req,
		StatusCode:      200,
		ResponseHeader:  http.Header{"X-Test": []string{"1"}},
		ResponseBody:    []byte("payload"),
		ObservedLatency: 50 * time.Microsecond,
		Timestamp:       time.Now(),
	})
	cb.Stop() // drains pending

	entry, ok := cb.Lookup(key)
	if !ok {
		t.Fatal("expected hit after record")
	}
	if string(entry.Body) != "payload" {
		t.Fatalf("unexpected body %q", entry.Body)
	}
	if entry.ObservedLatency != 50*time.Microsecond {
		t.Fatalf("unexpected latency %s", entry.ObservedLatency)
	}
}

func TestCacheBox_SampleDelayDefaultsToObserved(t *testing.T) {
	cb := New(Config{})
	defer cb.Stop()
	got := cb.SampleDelay(&Entry{ObservedLatency: 7 * time.Millisecond})
	if got != 7*time.Millisecond {
		t.Fatalf("default delay source mismatch: %s", got)
	}
}

func TestCacheBox_SetDelaySource(t *testing.T) {
	cb := New(Config{})
	defer cb.Stop()
	cb.SetDelaySource(NewDistributionDelaySource(math.Log(1000), 0.2, 3))
	// Unfitted sigma != 0 so we expect non-observed samples; verify it's positive.
	got := cb.SampleDelay(&Entry{ObservedLatency: 0})
	if got <= 0 {
		t.Fatalf("expected positive fitted sample, got %s", got)
	}
}

func TestCacheBox_CustomKeyFuncOverridesStrategy(t *testing.T) {
	customKey := "fixed-key"
	cb := New(Config{
		KeyFunc: func(_ *http.Request, _ []byte) string { return customKey },
	})
	defer cb.Stop()
	key := cb.DeriveKey(mustRequest(t, "GET", "http://svc/anything"), nil)
	if key != customKey {
		t.Fatalf("got %q, want %q", key, customKey)
	}
}

func TestEntry_SizeCountsBodyAndHeader(t *testing.T) {
	e := &Entry{
		Key:  "k",
		Body: bytes.Repeat([]byte("a"), 100),
		Header: http.Header{
			"X-Foo": []string{"bar"},
		},
	}
	// Size = 100 (body) + 1 (key) + 5 (X-Foo) + 3 (bar) = 109
	if got := e.Size(); got != 109 {
		t.Fatalf("Size = %d, want 109", got)
	}
	if (*Entry)(nil).Size() != 0 {
		t.Fatal("nil entry Size should be 0")
	}
}
