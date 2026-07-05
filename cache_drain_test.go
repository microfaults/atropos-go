package atropos_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	atropos "git.ucsc.edu/microfaults/atropos-go"
)

func recordingRule(name, experimentID, phaseID string) atropos.CompiledRule {
	return atropos.CompiledRule{
		Name:           name,
		InjectionPoint: "egress",
		Mode:           "inline",
		CacheBox: &atropos.CompiledCacheBox{
			Mode: "passthrough",
			Context: &atropos.CacheBoxContext{
				ExperimentID: experimentID, PhaseID: phaseID,
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		},
	}
}

func TestActiveRecordingPhases_ScansPassthroughContexts(t *testing.T) {
	rules := []atropos.CompiledRule{
		recordingRule("r1", "exp-1", "phase-1"),
		{Name: "replay-rule", CacheBox: &atropos.CompiledCacheBox{Mode: "replay", Context: &atropos.CacheBoxContext{ExperimentID: "exp-2", PhaseID: "phase-2"}}},
		{Name: "no-context", CacheBox: &atropos.CompiledCacheBox{Mode: "passthrough"}},
		{Name: "fault-rule"},
	}
	active := atropos.ActiveRecordingPhases(rules)
	if len(active) != 1 {
		t.Fatalf("expected exactly 1 active recording phase (passthrough+context only), got %d: %+v", len(active), active)
	}
	if !active[atropos.RecordingPhaseKey{ExperimentID: "exp-1", PhaseID: "phase-1"}] {
		t.Fatalf("expected exp-1/phase-1 to be active, got %+v", active)
	}
}

func TestEndedRecordingPhases_DiffsPrevVsCurr(t *testing.T) {
	prev := map[atropos.RecordingPhaseKey]bool{
		{ExperimentID: "exp-1", PhaseID: "phase-1"}: true,
		{ExperimentID: "exp-2", PhaseID: "phase-2"}: true,
	}
	curr := map[atropos.RecordingPhaseKey]bool{
		{ExperimentID: "exp-2", PhaseID: "phase-2"}: true,
	}
	ended := atropos.EndedRecordingPhases(prev, curr)
	if len(ended) != 1 || ended[0] != (atropos.RecordingPhaseKey{ExperimentID: "exp-1", PhaseID: "phase-1"}) {
		t.Fatalf("expected exp-1/phase-1 to be reported ended, got %+v", ended)
	}
}

// TestCacheDrainTracker_ObservesPhaseEnd is an end-to-end check of the
// ATRO-5(c) auto-detection wiring: Apply(), given a rule set that no
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
	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{Push: pusher.PushFunc()})
	defer cb.Stop()

	tracker := atropos.NewCacheDrainTracker(cb, pusher, nil)
	eval := atropos.NewStaticEvaluator()
	targets := atropos.ApplyTargets{Evaluator: eval, CacheDrain: tracker}

	// First poll: phase exp-1/phase-1 is recording.
	first := atropos.RegisterResponse{RuleSync: atropos.RuleSync{
		Rules: []atropos.CompiledRule{recordingRule("r1", "exp-1", "phase-1")},
	}}
	if err := atropos.Apply(first, targets); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if drainCalls != 0 {
		t.Fatalf("no phase has ended yet, expected 0 drain calls, got %d", drainCalls)
	}

	// Second poll: the rule set no longer contains exp-1/phase-1 --
	// recording for it has ended.
	second := atropos.RegisterResponse{RuleSync: atropos.RuleSync{
		Rules: []atropos.CompiledRule{recordingRule("r2", "exp-9", "phase-9")},
	}}
	if err := atropos.Apply(second, targets); err != nil {
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
	empty := atropos.RegisterResponse{RuleSync: atropos.RuleSync{}}
	if err := atropos.Apply(empty, targets); err != nil {
		t.Fatalf("empty apply: %v", err)
	}
	mu.Lock()
	got = drainCalls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("an empty rules list must not trigger a drain report, got %d calls", got)
	}
}
