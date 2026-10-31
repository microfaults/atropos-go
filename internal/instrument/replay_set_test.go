package instrument

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

	dto "github.com/prometheus/client_model/go"
)

// fakeArtifact serves one replay-set artifact the way manteion's
// GET /api/v1/cache/artifacts/{id} does: http.ServeContent implements Range,
// 206 and Content-Range. The hooks inject 5xx at an offset, a server that
// ignores Range, a server that hides the total size, and a gate that holds
// every request.
type fakeArtifact struct {
	srv         *httptest.Server
	body        []byte
	mu          sync.Mutex
	ranges      []string      // Range headers seen, in order of arrival
	failAt      map[int64]int // offset → remaining 500s to serve
	ignoreRange bool          // answer 200 with the whole body
	hideTotal   bool          // Content-Range "bytes a-b/*": the size is never given
	gate        chan struct{} // when non-nil, every request blocks until it is closed
}

func newFakeArtifact(t *testing.T, entries []*cachebox.Entry) *fakeArtifact {
	t.Helper()
	var buf bytes.Buffer
	for _, e := range entries {
		line, err := json.Marshal(cachebox.EntryToWire(e))
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	f := &fakeArtifact{body: buf.Bytes(), failAt: map[int64]int{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Recorded on arrival, before the gate, so a test can tell a request is in flight.
		f.mu.Lock()
		f.ranges = append(f.ranges, r.Header.Get("Range"))
		gate, ignoreRange, hideTotal := f.gate, f.ignoreRange, f.hideTotal
		var off int64
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &off)
		n := f.failAt[off]
		if n > 0 {
			f.failAt[off] = n - 1
		}
		f.mu.Unlock()
		if gate != nil {
			<-gate
		}
		if n > 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if ignoreRange {
			w.Write(f.body)
			return
		}
		if hideTotal {
			w = starTotalWriter{w}
		}
		http.ServeContent(w, r, "artifact", time.Time{}, bytes.NewReader(f.body))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// hold makes every request to f block until release is called. release also
// runs at cleanup, ahead of srv.Close, which would otherwise wait on a held
// handler forever.
func (f *fakeArtifact) hold(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	f.mu.Lock()
	f.gate = gate
	f.mu.Unlock()
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release
}

func (f *fakeArtifact) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ranges...)
}

// starTotalWriter rewrites "Content-Range: bytes a-b/N" to "bytes a-b/*".
type starTotalWriter struct{ http.ResponseWriter }

func (s starTotalWriter) WriteHeader(code int) {
	if cr := s.Header().Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			s.Header().Set("Content-Range", cr[:i+1]+"*")
		}
	}
	s.ResponseWriter.WriteHeader(code)
}

// replayEntries builds n entries keyed v2:k<i>, each a 200 whose 40-byte body
// repeats one letter, so a marshalled NDJSON line runs well past 100 bytes.
func replayEntries(n int) []*cachebox.Entry {
	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	entries := make([]*cachebox.Entry, n)
	for i := range entries {
		entries[i] = &cachebox.Entry{
			Key:        fmt.Sprintf("v2:k%d", i),
			StatusCode: 200,
			Body:       bytes.Repeat([]byte{byte('x' + i)}, 40),
			RecordedAt: at,
		}
	}
	return entries
}

func refFor(f *fakeArtifact, entries []*cachebox.Entry) wire.ReplaySetRef {
	return wire.ReplaySetRef{ID: "sha256:" + fmt.Sprintf("%x", sha256.Sum256(f.body)), KeyStrategy: "canonical_v2",
		Count: len(entries), SHA256: cachebox.SetChecksum(entries), SizeBytes: int64(len(f.body)), URL: "/api/v1/cache/artifacts/x"}
}

func newTestPuller(t *testing.T, f *fakeArtifact, cb *cachebox.CacheBox, onChange func()) *replaySetPuller {
	t.Helper()
	p := newReplaySetPuller(replaySetPullerConfig{BaseURL: f.srv.URL, CacheBox: cb, Logger: slog.New(slog.DiscardHandler), OnChange: onChange})
	t.Cleanup(p.Close)
	return p
}

func newTestCacheBox(t *testing.T) *cachebox.CacheBox {
	t.Helper()
	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
	t.Cleanup(cb.Stop)
	return cb
}

// withReplayChunkBytes shrinks the Range chunk for one test. Set it before the
// puller starts; the restore runs after the puller's Close (cleanups are LIFO).
func withReplayChunkBytes(t *testing.T, n int64) {
	t.Helper()
	replaySetChunkBytes = n
	t.Cleanup(func() { replaySetChunkBytes = 4 << 20 })
}

func waitState(t *testing.T, p *replaySetPuller, want string) (*wire.ReplaySetStatus, string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st, msg := p.Status(); st != nil && st.State == want {
			return st, msg
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, msg := p.Status()
	t.Fatalf("puller state = %+v (%s), want %s", st, msg, want)
	return nil, ""
}

// waitChanges waits for OnChange to have run want times. The state turns
// visible before OnChange runs (OnChange reports it), so a test that has just
// seen the state must wait for the call.
func waitChanges(t *testing.T, changes *atomic.Int32, want int32) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool { return changes.Load() >= want })
	if got := changes.Load(); got != want {
		t.Fatalf("OnChange calls = %d, want %d", got, want)
	}
}

func replaySetGauge(t *testing.T, state string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := metrics.ReplaySetState.WithLabelValues(state).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func assertReplaySetGauge(t *testing.T, want string) {
	t.Helper()
	for _, s := range []string{wire.ReplaySetFetching, wire.ReplaySetCommitted, wire.ReplaySetFailed} {
		wantV := 0.0
		if s == want {
			wantV = 1
		}
		if got := replaySetGauge(t, s); got != wantV {
			t.Errorf("atropos_replay_set_state{state=%q} = %v, want %v", s, got, wantV)
		}
	}
}

func ptr[T any](v T) *T { return &v }

var scope1 = cachebox.PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"}

func TestReplaySetPuller_FetchesInChunksAndCommits(t *testing.T) {
	withReplayChunkBytes(t, 100) // not a multiple of the ~160-byte lines: lines straddle chunks
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	var changes atomic.Int32
	p := newTestPuller(t, f, cb, func() { changes.Add(1) })

	release := f.hold(t) // keep the fetch in flight until the fetching state is checked
	p.Reconcile(ptr(refFor(f, entries)), scope1)
	if st, msg := p.Status(); st == nil || st.State != wire.ReplaySetFetching || st.ID != refFor(f, entries).ID || msg != "" {
		t.Fatalf("state right after Reconcile = %+v (%q), want fetching", st, msg)
	}
	assertReplaySetGauge(t, wire.ReplaySetFetching)
	release()

	st, msg := waitState(t, p, wire.ReplaySetCommitted)
	if msg != "" || st.ID != refFor(f, entries).ID {
		t.Fatalf("status = %+v msg=%q", st, msg)
	}
	if cb.ReplaySetLen() != 5 || cb.ReplaySetPhaseKey() != cachebox.PhaseKey("exp-1", "phase-1") {
		t.Fatalf("installed %d entries under %q", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
	}
	if _, ok := cb.Lookup("v2:k3"); !ok {
		t.Fatal("entry v2:k3 missing after commit")
	}
	ranges := f.requests()
	if len(ranges) < 5 || ranges[0] != "bytes=0-99" {
		t.Fatalf("ranges = %q, want ≥5 chunked requests starting at bytes=0-99", ranges)
	}
	waitChanges(t, &changes, 1)
	assertReplaySetGauge(t, wire.ReplaySetCommitted)
	if snap := cb.Fidelity().Snapshot(scope1); !snap.PreloadCommitted || snap.PreloadEntries != 5 {
		t.Fatalf("fidelity preload state = %+v", snap)
	}

	// Same id again: no new fetch, state stays committed.
	p.Reconcile(ptr(refFor(f, entries)), scope1)
	if st, _ := p.Status(); st.State != wire.ReplaySetCommitted {
		t.Fatalf("re-reconcile state = %+v", st)
	}
	if again := len(f.requests()); again != len(ranges) {
		t.Fatalf("same id must not refetch: %d → %d requests", len(ranges), again)
	}
	if changes.Load() != 1 {
		t.Fatalf("OnChange calls after a no-op Reconcile = %d, want 1", changes.Load())
	}
}

func TestReplaySetPuller_ResumesAfterTransportError(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	f.failAt[100] = 1 // the second chunk 500s once
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	if cb.ReplaySetLen() != 5 {
		t.Fatalf("installed %d entries, want 5", cb.ReplaySetLen())
	}
	n := 0
	for _, r := range f.requests() {
		if r == "bytes=100-199" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("bytes=100-199 requested %d times, want 2 (resume from the failed offset): %q", n, f.requests())
	}
}

func TestReplaySetPuller_AcceptsFullBodyWhenRangeIgnored(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	f.ignoreRange = true
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	if cb.ReplaySetLen() != len(entries) {
		t.Fatalf("installed %d entries, want %d", cb.ReplaySetLen(), len(entries))
	}
	if got := f.requests(); len(got) != 1 {
		t.Fatalf("requests = %q, want one: a 200 is the whole artifact", got)
	}
}

// An artifact whose size is an exact multiple of the chunk, from a server that
// never states the total, ends on the 416 for the range past its end.
func TestReplaySetPuller_EndsOn416WhenSizeIsAChunkMultiple(t *testing.T) {
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	f.hideTotal = true
	withReplayChunkBytes(t, int64(len(f.body)))
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	if cb.ReplaySetLen() != 5 {
		t.Fatalf("installed %d entries, want 5", cb.ReplaySetLen())
	}
	if got := f.requests(); len(got) != 2 {
		t.Fatalf("requests = %q, want the full chunk then the 416 past the end", got)
	}
}

// http.ServeContent answers any range of an empty artifact with a 200 and no
// body: an empty set, which commits (count 0, the W5 digest of nothing) rather
// than failing.
func TestReplaySetPuller_EmptyArtifactCommitsAnEmptySet(t *testing.T) {
	f := newFakeArtifact(t, nil)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, nil)), scope1)
	st, msg := waitState(t, p, wire.ReplaySetCommitted)
	if msg != "" || st.ID != refFor(f, nil).ID {
		t.Fatalf("status = %+v msg=%q", st, msg)
	}
	if cb.ReplaySetPhaseKey() != cachebox.PhaseKey("exp-1", "phase-1") || cb.ReplaySetLen() != 0 {
		t.Fatalf("installed %d entries under %q, want an empty set under scope1", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
	}
}

func TestReplaySetPuller_DigestMismatchFails(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	cb.InstallReplaySet("other", map[string]*cachebox.Entry{"prior": {Key: "prior", StatusCode: 200}})
	var changes atomic.Int32
	p := newTestPuller(t, f, cb, func() { changes.Add(1) })

	ref := refFor(f, entries)
	ref.SHA256 = strings.Repeat("0", 64)
	p.Reconcile(&ref, scope1)
	st, msg := waitState(t, p, wire.ReplaySetFailed)
	if st.ID != ref.ID || !strings.Contains(msg, "commit mismatch") {
		t.Fatalf("status = %+v msg=%q, want failed with a commit mismatch", st, msg)
	}
	if _, ok := cb.Lookup("v2:k0"); ok {
		t.Fatal("an entry of the mismatched set was installed")
	}
	if cb.ReplaySetPhaseKey() != "other" || cb.ReplaySetLen() != 1 {
		t.Fatalf("prior set disturbed: %d entries under %q", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
	}
	if _, ok := cb.Lookup("prior"); !ok {
		t.Fatal("prior set's entry gone")
	}
	waitChanges(t, &changes, 1)
	assertReplaySetGauge(t, wire.ReplaySetFailed)
}

func TestReplaySetPuller_SizeMismatchFails(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	ref := refFor(f, entries)
	ref.SizeBytes = int64(len(f.body)) + 1
	p.Reconcile(&ref, scope1)
	_, msg := waitState(t, p, wire.ReplaySetFailed)
	if !strings.Contains(msg, "size mismatch") {
		t.Fatalf("msg = %q, want a size mismatch", msg)
	}
	if cb.ReplaySetLen() != 0 || cb.ReplaySetPhaseKey() != "" {
		t.Fatalf("installed %d entries under %q, want nothing", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
	}
}

func TestReplaySetPuller_UnscopedFails(t *testing.T) {
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	var changes atomic.Int32
	p := newTestPuller(t, f, cb, func() { changes.Add(1) })

	p.Reconcile(ptr(refFor(f, entries)), cachebox.PhasePair{})
	_, msg := waitState(t, p, wire.ReplaySetFailed)
	if !strings.Contains(msg, "no (experiment_id, phase_id)") {
		t.Fatalf("msg = %q, want the missing-scope reason", msg)
	}
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("requests = %q, want none for an unscoped set", got)
	}
	waitChanges(t, &changes, 1)
}

func TestReplaySetPuller_NewIDSupersedesInFlight(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries1, entries2 := replayEntries(5), replayEntries(3)
	f1 := newFakeArtifact(t, entries1)
	release := f1.hold(t)
	f2 := newFakeArtifact(t, entries2)
	cb := newTestCacheBox(t)
	var changes atomic.Int32
	p := newTestPuller(t, f1, cb, func() { changes.Add(1) })

	p.Reconcile(ptr(refFor(f1, entries1)), scope1)
	waitFor(t, 5*time.Second, func() bool { return len(f1.requests()) > 0 }) // ref1's first chunk is held in flight
	ref2 := refFor(f2, entries2)
	ref2.URL = f2.srv.URL + "/api/v1/cache/artifacts/y" // absolute: passes through resolve
	p.Reconcile(&ref2, scope1)
	release()

	st, msg := waitState(t, p, wire.ReplaySetCommitted)
	if st.ID != ref2.ID || msg != "" {
		t.Fatalf("status = %+v msg=%q, want ref2 committed", st, msg)
	}
	if cb.ReplaySetLen() != 3 {
		t.Fatalf("installed %d entries, want ref2's 3", cb.ReplaySetLen())
	}
	if _, ok := cb.Lookup("v2:k4"); ok {
		t.Fatal("f1-only key v2:k4 installed: the superseded fetch reached the new staging")
	}
	if got := f1.requests(); len(got) != 1 {
		t.Fatalf("f1 requests = %q, want only the held first chunk: the superseded fetch must stop", got)
	}
	waitChanges(t, &changes, 1) // ref2's commit only: the superseded fetch must not report
}

// Manteion going back to the committed set while another is in flight cancels
// that fetch: left running, it would replace the set manteion now wants.
func TestReplaySetPuller_ReturnToCommittedIDCancelsInFlight(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries1, entries2 := replayEntries(5), replayEntries(3)
	f1, f2 := newFakeArtifact(t, entries1), newFakeArtifact(t, entries2)
	cb := newTestCacheBox(t)
	var changes atomic.Int32
	p := newTestPuller(t, f1, cb, func() { changes.Add(1) })

	ref1 := refFor(f1, entries1)
	p.Reconcile(&ref1, scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	waitChanges(t, &changes, 1)

	release := f2.hold(t)
	ref2 := refFor(f2, entries2)
	ref2.URL = f2.srv.URL + "/api/v1/cache/artifacts/y"
	p.Reconcile(&ref2, scope1)
	waitFor(t, 5*time.Second, func() bool { return len(f2.requests()) > 0 })
	p.Reconcile(&ref1, scope1)
	release()

	if st, msg := p.Status(); st == nil || st.ID != ref1.ID || st.State != wire.ReplaySetCommitted || msg != "" {
		t.Fatalf("status = %+v (%q), want ref1 committed again", st, msg)
	}
	if cb.ReplaySetLen() != 5 {
		t.Fatalf("installed %d entries, want ref1's 5", cb.ReplaySetLen())
	}
	if got := f2.requests(); len(got) != 1 {
		t.Fatalf("f2 requests = %q, want only the held first chunk", got)
	}
	if changes.Load() != 1 {
		t.Fatalf("OnChange calls = %d, want 1: the cancelled fetch must not report", changes.Load())
	}
	assertReplaySetGauge(t, wire.ReplaySetCommitted)
}

func TestReplaySetPuller_NilClearsInstalledSet(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	p.Reconcile(nil, scope1)
	if st, msg := p.Status(); st != nil || msg != "" {
		t.Fatalf("Status() = %+v, %q, want nil, \"\"", st, msg)
	}
	if cb.ReplaySetLen() != 0 || cb.ReplaySetPhaseKey() != "" {
		t.Fatalf("installed %d entries under %q, want none", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
	}
	assertReplaySetGauge(t, "")
}

func TestReplaySetPuller_RefetchesWhenInstalledSetWasClearedUnderneath(t *testing.T) {
	withReplayChunkBytes(t, 100)
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	cb := newTestCacheBox(t)
	p := newTestPuller(t, f, cb, nil)

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitState(t, p, wire.ReplaySetCommitted)
	first := len(f.requests())

	cb.ClearReplaySet()  // thaw via DELETE /admin/cachebox
	release := f.hold(t) // the refetch stays in flight until its state is read
	p.Reconcile(ptr(refFor(f, entries)), scope1)
	if st, _ := p.Status(); st == nil || st.State != wire.ReplaySetFetching {
		t.Fatalf("state after Reconcile over a cleared set = %+v, want fetching", st)
	}
	release()
	waitState(t, p, wire.ReplaySetCommitted)
	if cb.ReplaySetLen() != 5 {
		t.Fatalf("installed %d entries after the refetch, want 5", cb.ReplaySetLen())
	}
	if again := len(f.requests()); again <= first {
		t.Fatalf("requests %d → %d, want a refetch", first, again)
	}
}

// Close cancels a fetch in flight without reporting it as failed, and the
// puller starts nothing afterwards.
func TestReplaySetPuller_CloseCancelsWithoutReporting(t *testing.T) {
	entries := replayEntries(5)
	f := newFakeArtifact(t, entries)
	release := f.hold(t)
	cb := newTestCacheBox(t)
	var changes atomic.Int32
	p := newTestPuller(t, f, cb, func() { changes.Add(1) })

	p.Reconcile(ptr(refFor(f, entries)), scope1)
	waitFor(t, 5*time.Second, func() bool { return len(f.requests()) > 0 })
	p.Close() // returns once the held request is cancelled: release has not run
	release()
	if changes.Load() != 0 {
		t.Fatalf("OnChange calls = %d, want 0: shutdown is not a failure", changes.Load())
	}
	if st, _ := p.Status(); st == nil || st.State == wire.ReplaySetFailed {
		t.Fatalf("status after Close = %+v, want the fetch unreported", st)
	}

	p.Reconcile(ptr(refFor(f, entries)), cachebox.PhasePair{ExperimentID: "exp-2", PhaseID: "phase-2"})
	time.Sleep(50 * time.Millisecond)
	if got := f.requests(); len(got) != 1 {
		t.Fatalf("requests after Close = %q, want none new", got)
	}
	p.Close() // idempotent
}

func TestReplaySetPuller_NilMeansNoReplaySetSupport(t *testing.T) {
	var p *replaySetPuller
	p.Reconcile(&wire.ReplaySetRef{ID: "sha256:x"}, scope1)
	if st, msg := p.Status(); st != nil || msg != "" {
		t.Fatalf("nil puller Status() = %+v, %q", st, msg)
	}
	p.Close()
	if q := newReplaySetPuller(replaySetPullerConfig{BaseURL: "http://manteion:8080"}); q != nil {
		t.Fatal("a puller without a cache box has nothing to install into and must be nil")
	}
}

func TestReplaySetPuller_ResolvesArtifactURL(t *testing.T) {
	for _, tc := range []struct{ base, ref, want string }{
		{"http://manteion:8080", "/api/v1/cache/artifacts/sha256:ab", "http://manteion:8080/api/v1/cache/artifacts/sha256:ab"},
		{"http://manteion:8080/", "/api/v1/cache/artifacts/x", "http://manteion:8080/api/v1/cache/artifacts/x"},
		// Relative to the base URL the way every manteion endpoint is: a path prefix stays.
		{"http://gw/manteion", "/api/v1/cache/artifacts/x", "http://gw/manteion/api/v1/cache/artifacts/x"},
		{"http://manteion:8080", "http://blobs:9000/a/x", "http://blobs:9000/a/x"},
	} {
		p := &replaySetPuller{baseURL: tc.base}
		if got, err := p.resolve(tc.ref); err != nil || got != tc.want {
			t.Errorf("resolve(%q) against %q = %q, %v; want %q", tc.ref, tc.base, got, err, tc.want)
		}
	}
	if _, err := (&replaySetPuller{baseURL: "http://manteion:8080"}).resolve(""); err == nil {
		t.Error("an empty artifact url must not resolve to the base URL")
	}
}

func TestDecodeNDJSON_CarriesPartialLines(t *testing.T) {
	line := func(key string) string {
		b, err := json.Marshal(cachebox.WireEntry{Key: key, StatusCode: 200, Body: []byte(key)})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	c := line("c")
	entries, carry, err := decodeNDJSON([]byte(line("a")+"\n\n"+line("b")+"\n"+c[:5]), false)
	if err != nil || len(entries) != 2 || entries[0].Key != "a" || entries[1].Key != "b" || string(carry) != c[:5] {
		t.Fatalf("first chunk: %d entries, carry %q, err %v", len(entries), carry, err)
	}
	entries, carry, err = decodeNDJSON(append(carry, c[5:]...), true) // unterminated last line
	if err != nil || len(entries) != 1 || entries[0].Key != "c" || string(entries[0].Body) != "c" || carry != nil {
		t.Fatalf("final chunk: %d entries, carry %q, err %v", len(entries), carry, err)
	}
	if entries, carry, err = decodeNDJSON([]byte(line("d")+"\n \r\n"), true); err != nil || len(entries) != 1 || carry != nil {
		t.Fatalf("blank trailing lines: %d entries, carry %q, err %v", len(entries), carry, err)
	}
	if _, _, err := decodeNDJSON([]byte("{not json}\n"), false); err == nil {
		t.Fatal("a malformed line must fail the decode")
	}
}
