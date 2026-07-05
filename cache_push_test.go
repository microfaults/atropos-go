package atropos_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atropos "git.ucsc.edu/microfaults/atropos-go"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

func TestCachePushClient_BatchByCount(t *testing.T) {
	var received atomic.Int32
	var mu sync.Mutex
	var lastBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		lastBody = b
		mu.Unlock()
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 3,
		MaxWait:  10 * time.Second,
	})
	defer client.Stop()

	pushFn := client.PushFunc()
	for i := 0; i < 3; i++ {
		pushFn("key", &cachebox.Entry{
			Key: "key", StatusCode: 200, Body: []byte("body"),
		})
	}

	time.Sleep(200 * time.Millisecond)

	if received.Load() != 1 {
		t.Fatalf("expected 1 POST, got %d", received.Load())
	}

	var envelope struct {
		Service  string            `json:"service"`
		Instance string            `json:"instance"`
		Entries  []json.RawMessage `json:"entries"`
	}
	mu.Lock()
	body := lastBody
	mu.Unlock()
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if envelope.Service != "cart" {
		t.Errorf("service = %q", envelope.Service)
	}
	if len(envelope.Entries) != 3 {
		t.Errorf("entries = %d, want 3", len(envelope.Entries))
	}
}

func TestCachePushClient_BatchByTime(t *testing.T) {
	var received atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 100,
		MaxWait:  200 * time.Millisecond,
	})
	defer client.Stop()

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})

	time.Sleep(500 * time.Millisecond)

	if received.Load() != 1 {
		t.Fatalf("expected 1 POST after time flush, got %d", received.Load())
	}
}

func TestCachePushClient_StopFlushes(t *testing.T) {
	var received atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 100,
		MaxWait:  10 * time.Second,
	})

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})
	client.Stop()

	if received.Load() != 1 {
		t.Fatalf("Stop should flush pending batch, got %d POSTs", received.Load())
	}
}

func TestCachePushClient_DropOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL:  server.URL,
		Service:  "cart",
		Instance: "pod-1",
		MaxBatch: 1,
		MaxWait:  10 * time.Second,
	})
	defer client.Stop()

	client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x")})

	// A persistent 500 is retried (up to maxPushAttempts) with exponential
	// backoff before counting as dropped -- poll instead of a fixed sleep
	// to tolerate that without hardcoding the backoff schedule here.
	waitFor(t, 5*time.Second, func() bool { return client.Stats().Dropped > 0 })
}

// waitFor polls cond every 10ms until it returns true or timeout elapses,
// failing the test in the latter case.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestPush_RetriesThenDelivers pins the ATRO-5 retry contract (design doc
// Q2): a persistent 500 is retried (bounded, exponential backoff) within a
// single batch's own attempt budget, and batch_seq stays contiguous across
// separate batches even though the first one burned retries.
func TestPush_RetriesThenDelivers(t *testing.T) {
	var mu sync.Mutex
	var seenSeqs []int
	var reqCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqCount.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var env struct {
			BatchSeq int `json:"batch_seq"`
		}
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &env)
		mu.Lock()
		seenSeqs = append(seenSeqs, env.BatchSeq)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 1, MaxWait: 10 * time.Second,
	})
	defer client.Stop()

	// Three sequential single-entry batches, same phase. Waiting for each
	// to land before starting the next keeps this deterministic: the first
	// batch alone absorbs both server failures within its own retry
	// budget, so batches 2 and 3 land on their first attempt once the
	// server has recovered.
	for i := 0; i < 3; i++ {
		client.PushFunc()("k", &cachebox.Entry{
			Key: "k", StatusCode: 200, Body: []byte("x"),
			ExperimentID: "exp-1", PhaseID: "phase-1",
		})
		want := int64(i + 1)
		waitFor(t, 5*time.Second, func() bool { return client.Stats().Pushed == want })
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seenSeqs) != 3 {
		t.Fatalf("expected all 3 batches to eventually arrive, got %v", seenSeqs)
	}
	for i, seq := range seenSeqs {
		if seq != i+1 {
			t.Fatalf("batch_seq not contiguous across batches: got %v, want [1 2 3]", seenSeqs)
		}
	}
	if got := client.Stats().Dropped; got != 0 {
		t.Fatalf("expected no drops once every batch eventually succeeds, got %d", got)
	}
}

// TestPush_TerminalRejectStopsRetry pins that a 409 phase_not_recording
// response stops retrying immediately (design doc Q2) -- the server must
// see exactly one request, not up to maxPushAttempts.
func TestPush_TerminalRejectStopsRetry(t *testing.T) {
	var reqCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"phase_not_recording"}`))
	}))
	defer server.Close()

	client := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 1, MaxWait: 10 * time.Second,
	})
	defer client.Stop()

	client.PushFunc()("k", &cachebox.Entry{
		Key: "k", StatusCode: 200, Body: []byte("x"),
		ExperimentID: "exp-1", PhaseID: "phase-1",
	})

	waitFor(t, 5*time.Second, func() bool { return client.Stats().PushRejectedTerminal > 0 })

	// Give an (incorrect) retry a moment it would need to fire, then
	// confirm it never did.
	time.Sleep(300 * time.Millisecond)
	if got := reqCount.Load(); got != 1 {
		t.Fatalf("409 must stop retrying immediately, server saw %d requests", got)
	}
	if got := client.Stats().Dropped; got != 1 {
		t.Fatalf("expected the terminally-rejected entry also counted as dropped, got %d", got)
	}
}

// TestPush_DrainReportCountsMatchEnqueued pins the ATRO-5 drain-report
// contract (design doc Q2/W3): after flushing, entries_recorded ==
// entries_pushed + entries_dropped, and entries_recorded equals however
// many records the test actually enqueued.
func TestPush_DrainReportCountsMatchEnqueued(t *testing.T) {
	var mu sync.Mutex
	var drainReports []atropos.DrainReport

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			var report atropos.DrainReport
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &report)
			mu.Lock()
			drainReports = append(drainReports, report)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(atropos.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Canonical external-host wiring: the push client and CacheBox need ONE
	// shared fidelity registry (the drain report snapshots it), but the
	// construction is circular -- so bind after building the CacheBox.
	pusher := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 100, MaxWait: 10 * time.Second,
	})
	defer pusher.Stop()

	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{
		KeyStrategy: atropos.KeyStrategyExact,
		Push:        pusher.PushFunc(),
	})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())

	const n = 25
	for i := 0; i < n; i++ {
		req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://svc/x%d", i), nil)
		cb.Record(cachebox.CacheRecord{
			Request:      req,
			StatusCode:   200,
			ResponseBody: []byte("v"),
			Timestamp:    time.Now(),
			ExperimentID: "exp-1",
			PhaseID:      "phase-1",
		})
	}
	cb.FlushRecording() // guarantee every record has reached the pusher

	resp := pusher.SendDrainReport("exp-1", "phase-1", cb)
	if !resp.Accepted {
		t.Fatal("expected drain report to be accepted")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(drainReports) != 1 {
		t.Fatalf("expected exactly one drain report received, got %d", len(drainReports))
	}
	report := drainReports[0]
	if report.EntriesRecorded != n {
		t.Fatalf("EntriesRecorded = %d, want %d (the test's enqueue count)", report.EntriesRecorded, n)
	}
	if report.EntriesRecorded != report.EntriesPushed+report.EntriesDropped {
		t.Fatalf("EntriesRecorded (%d) != EntriesPushed (%d) + EntriesDropped (%d)",
			report.EntriesRecorded, report.EntriesPushed, report.EntriesDropped)
	}
}

// TestPush_DrainReportIsPerPhase pins F2: the drain report's counts are
// scoped to the reported (experiment, phase) -- a second recording phase in
// the same process must not inherit the first phase's totals, or manteion's
// drain gate (received == recorded per instance) can never be satisfied for
// any phase after the first.
func TestPush_DrainReportIsPerPhase(t *testing.T) {
	var mu sync.Mutex
	var drainReports []atropos.DrainReport
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			var report atropos.DrainReport
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &report)
			mu.Lock()
			drainReports = append(drainReports, report)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(atropos.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	pusher := atropos.NewCachePushClient(atropos.CachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 100, MaxWait: 10 * time.Second,
	})
	defer pusher.Stop()
	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{
		KeyStrategy: atropos.KeyStrategyExact,
		Push:        pusher.PushFunc(),
	})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())

	record := func(phase string, n int) {
		for i := 0; i < n; i++ {
			req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://svc/%s/x%d", phase, i), nil)
			cb.Record(cachebox.CacheRecord{
				Request: req, StatusCode: 200, ResponseBody: []byte("v"),
				Timestamp: time.Now(), ExperimentID: "exp-1", PhaseID: phase,
			})
		}
	}

	record("phase-1", 7)
	if resp := pusher.SendDrainReport("exp-1", "phase-1", cb); !resp.Accepted {
		t.Fatal("phase-1 drain report not accepted")
	}
	record("phase-2", 3)
	if resp := pusher.SendDrainReport("exp-1", "phase-2", cb); !resp.Accepted {
		t.Fatal("phase-2 drain report not accepted")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(drainReports) != 2 {
		t.Fatalf("expected 2 drain reports, got %d", len(drainReports))
	}
	for i, want := range []int64{7, 3} {
		r := drainReports[i]
		if r.EntriesRecorded != want || r.EntriesPushed != want || r.EntriesDropped != 0 {
			t.Fatalf("report %d (%s): recorded=%d pushed=%d dropped=%d, want %d/%d/0 -- counts leaked across phases",
				i, r.PhaseID, r.EntriesRecorded, r.EntriesPushed, r.EntriesDropped, want, want)
		}
	}
}
