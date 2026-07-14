package atropos

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
)

func recordingRule(name, experimentID, phaseID string) CompiledRule {
	return CompiledRule{
		Name:           name,
		InjectionPoint: "egress",
		Mode:           "inline",
		CacheBox: &CompiledCacheBox{
			Mode: "passthrough",
			Context: &CacheBoxContext{
				ExperimentID: experimentID, PhaseID: phaseID,
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}
}

func TestActiveRecordingPhases_ScansPassthroughContexts(t *testing.T) {
	rules := []CompiledRule{
		recordingRule("r1", "exp-1", "phase-1"),
		{Name: "replay-rule", CacheBox: &CompiledCacheBox{Mode: "replay", Context: &CacheBoxContext{ExperimentID: "exp-2", PhaseID: "phase-2"}}},
		{Name: "no-context", CacheBox: &CompiledCacheBox{Mode: "passthrough"}},
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
			_ = json.NewEncoder(w).Encode(DrainReportResponse{Accepted: true})
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
	first := RegisterResponse{RuleSync: RuleSync{
		Rules: []CompiledRule{recordingRule("r1", "exp-1", "phase-1")},
	}}
	if err := apply(first, targets); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if drainCalls != 0 {
		t.Fatalf("no phase has ended yet, expected 0 drain calls, got %d", drainCalls)
	}

	// Second poll: the rule set no longer contains exp-1/phase-1 --
	// recording for it has ended.
	second := RegisterResponse{RuleSync: RuleSync{
		Rules: []CompiledRule{recordingRule("r2", "exp-9", "phase-9")},
	}}
	if err := apply(second, targets); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	mu.Lock()
	got := drainCalls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("expected exactly 1 drain report for the ended phase, got %d", got)
	}

	// Third poll: an EMPTY rules list means "no change" (RuleSync's
	// documented semantics), not "every phase ended" -- must not misfire.
	empty := RegisterResponse{RuleSync: RuleSync{}}
	if err := apply(empty, targets); err != nil {
		t.Fatalf("empty apply: %v", err)
	}
	mu.Lock()
	got = drainCalls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("an empty rules list must not trigger a drain report, got %d calls", got)
	}
}
