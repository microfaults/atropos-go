package interceptor

import (
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// TestReplayLookupRefusesForeignReplaySet pins the ownership gate: a
// replay-family decision scoped to pair B must not be served entries from
// pair A's installed set, even on a key match -- that is silent
// cross-phase (or cross-experiment) data bleed.
func TestReplayLookupRefusesForeignReplaySet(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	cb.InstallReplaySet(cachebox.PhaseKey("exp-A", "phase-A"), map[string]*cachebox.Entry{
		"k": {Key: "k", StatusCode: 200, Body: []byte("A")},
	})

	foreign := &cachebox.CacheBoxContext{ExperimentID: "exp-B", PhaseID: "phase-B"}
	if _, ok := replayLookup(cb, foreign, "k"); ok {
		t.Fatalf("lookup served an entry from another pair's replay set")
	}

	owner := &cachebox.CacheBoxContext{ExperimentID: "exp-A", PhaseID: "phase-A"}
	if _, ok := replayLookup(cb, owner, "k"); !ok {
		t.Fatalf("owning pair's lookup should hit")
	}

	// Legacy decisions without a context keep the ungated behavior.
	if _, ok := replayLookup(cb, nil, "k"); !ok {
		t.Fatalf("nil-context lookup should keep legacy ungated behavior")
	}
}
