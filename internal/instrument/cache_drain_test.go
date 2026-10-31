package instrument

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func recordingRule(name, experimentID, phaseID string) wire.CompiledRule {
	return wire.CompiledRule{
		Name:           name,
		InjectionPoint: "egress",
		Mode:           "inline",
		CacheBox: &wire.CompiledCacheBox{
			Mode: "passthrough",
			Context: &wire.CacheBoxContext{
				ExperimentID: experimentID, PhaseID: phaseID,
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}
}

func TestActiveRecordingPhases_ScansPassthroughContexts(t *testing.T) {
	rules := []wire.CompiledRule{
		recordingRule("r1", "exp-1", "phase-1"),
		{Name: "replay-rule", CacheBox: &wire.CompiledCacheBox{Mode: "replay", Context: &wire.CacheBoxContext{ExperimentID: "exp-2", PhaseID: "phase-2"}}},
		{Name: "no-context", CacheBox: &wire.CompiledCacheBox{Mode: "passthrough"}},
		{Name: "fault-rule"},
	}
	active := activeRecordingPhases(rules)
	if len(active) != 1 {
		t.Fatalf("expected exactly 1 active recording phase (passthrough+context only), got %d: %+v", len(active), active)
	}
	if !active[recordingPhaseKey{ExperimentID: "exp-1", PhaseID: "phase-1"}] {
		t.Fatalf("expected exp-1/phase-1 to be active, got %+v", active)
	}
}

func TestEndedRecordingPhases_DiffsPrevVsCurr(t *testing.T) {
	prev := map[recordingPhaseKey]bool{
		{ExperimentID: "exp-1", PhaseID: "phase-1"}: true,
		{ExperimentID: "exp-2", PhaseID: "phase-2"}: true,
	}
	curr := map[recordingPhaseKey]bool{
		{ExperimentID: "exp-2", PhaseID: "phase-2"}: true,
	}
	ended := endedRecordingPhases(prev, curr)
	if len(ended) != 1 || ended[0] != (recordingPhaseKey{ExperimentID: "exp-1", PhaseID: "phase-1"}) {
		t.Fatalf("expected exp-1/phase-1 to be reported ended, got %+v", ended)
	}
}

// TestCacheDrainTracker_ObservesPhaseEnd is an end-to-end check of the
// ATRO-5(c) auto-detection wiring: apply(), given a rule set that no
// longer authorizes a previously-recording phase, triggers exactly one
// drain report for it.
func TestCacheDrainTracker_ObservesPhaseEnd(t *testing.T) {
	var mu sync.Mutex
	var drainCalls int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			mu.Lock()
			drainCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	pusher := newCachePushClient(cachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 100, MaxWait: 10 * time.Second,
	})
	defer pusher.Stop()
	cb := cachebox.New(cachebox.Config{Push: pusher.PushFunc()})
	defer cb.Stop()

	tracker := newCacheDrainTracker(cb, pusher, nil)
	eval := evaluator.NewStaticEvaluator()
	targets := applyTargets{Evaluator: eval, CacheDrain: tracker}

	// First poll: phase exp-1/phase-1 is recording.
	first := wire.RegisterResponse{RuleSync: wire.RuleSync{
		Rules: []wire.CompiledRule{recordingRule("r1", "exp-1", "phase-1")},
	}}
	if _, err := apply(first.RuleSync, targets, scopeAll); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if drainCalls != 0 {
		t.Fatalf("no phase has ended yet, expected 0 drain calls, got %d", drainCalls)
	}

	// Second poll: the rule set no longer contains exp-1/phase-1 --
	// recording for it has ended.
	second := wire.RegisterResponse{RuleSync: wire.RuleSync{
		Rules: []wire.CompiledRule{recordingRule("r2", "exp-9", "phase-9")},
	}}
	if _, err := apply(second.RuleSync, targets, scopeAll); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	mu.Lock()
	got := drainCalls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("expected exactly 1 drain report for the ended phase, got %d", got)
	}

	// Third poll: a NULL rules list means "no change" (wire.RuleSync's
	// documented semantics), not "every phase ended" -- must not misfire.
	empty := wire.RegisterResponse{RuleSync: wire.RuleSync{Version: 3, Etag: "3-c"}}
	if _, err := apply(empty.RuleSync, targets, scopeAll); err != nil {
		t.Fatalf("empty apply: %v", err)
	}
	mu.Lock()
	got = drainCalls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("an empty rules list must not trigger a drain report, got %d calls", got)
	}
}

// TestCacheDrainTracker_ConcurrentDrainsReportConsistentCounts: drains run
// with no apply lock held, so two applies that each end a recording phase can
// drain at once. A drain's flush takes over whatever batch is pending -- here
// phase 2's entries, slow to post -- and the other drain must not build its
// report while that batch is in flight, or phase 2's report would count none
// of its entries pushed. Drains run one at a time.
func TestCacheDrainTracker_ConcurrentDrainsReportConsistentCounts(t *testing.T) {
	ingesting, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var first atomic.Bool
	var mu sync.Mutex
	reports := map[string]wire.DrainReport{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			if first.CompareAndSwap(false, true) {
				close(ingesting)
				<-release // the batch is slow to land
			}
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			var rep wire.DrainReport
			_ = json.NewDecoder(r.Body).Decode(&rep)
			mu.Lock()
			reports[rep.PhaseID] = rep
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	defer releaseOnce.Do(func() { close(release) }) // ahead of server.Close, which waits for a held handler
	pusher := newCachePushClient(cachePushConfig{
		BaseURL: server.URL, Service: "cart", Instance: "pod-1",
		MaxBatch: 100, MaxWait: 10 * time.Second,
	})
	defer pusher.Stop()
	cb := cachebox.New(cachebox.Config{Push: pusher.PushFunc()})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())
	tracker := newCacheDrainTracker(cb, pusher, nil)

	tracker.Observe([]wire.CompiledRule{recordingRule("r1", "exp-1", "phase-1"), recordingRule("r2", "exp-1", "phase-2")})
	for i := range 3 { // phase 2's entries: the batch pending in the push client
		key := fmt.Sprintf("k%d", i)
		pusher.PushFunc()(key, &cachebox.Entry{Key: key, StatusCode: 200, Body: []byte("x"), ExperimentID: "exp-1", PhaseID: "phase-2"})
	}
	if ended := tracker.Observe([]wire.CompiledRule{}); len(ended) != 2 {
		t.Fatalf("ended phases = %v, want both", ended)
	}

	var wg sync.WaitGroup
	wg.Go(func() { tracker.Drain([]recordingPhaseKey{{ExperimentID: "exp-1", PhaseID: "phase-1"}}) })
	waitClosed(t, ingesting, "phase 2's batch to be posted by the first drain's flush")
	wg.Go(func() { tracker.Drain([]recordingPhaseKey{{ExperimentID: "exp-1", PhaseID: "phase-2"}}) })
	time.Sleep(100 * time.Millisecond) // room for an unserialized drain to report early
	releaseOnce.Do(func() { close(release) })
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if got := reports["phase-2"]; got.EntriesPushed != 3 || got.EntriesDropped != 0 {
		t.Fatalf("phase-2 drain report: pushed=%d dropped=%d, want 3 and 0: it was built while its batch was in flight", got.EntriesPushed, got.EntriesDropped)
	}
	if got, ok := reports["phase-1"]; !ok || got.EntriesPushed != 0 {
		t.Fatalf("phase-1 drain report = %+v (sent: %v), want one with nothing pushed", got, ok)
	}
}
