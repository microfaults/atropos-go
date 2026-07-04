package cachebox

import (
	"testing"
	"time"
)

// TestFidelity_CountersTrackHitMissRecordDrop runs a scripted sequence
// through the registry and checks the resulting snapshot exactly (design
// doc Q6 counter set).
func TestFidelity_CountersTrackHitMissRecordDrop(t *testing.T) {
	r := NewFidelityRegistry()
	pair := PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"}

	r.RecordReplayHit(pair, time.Now().Add(-50*time.Millisecond))
	r.RecordReplayHit(pair, time.Now().Add(-150*time.Millisecond))
	r.RecordReplayMiss(pair, MissReasonKeyAbsent)
	r.RecordReplayMiss(pair, MissReasonNotCommitted)
	r.RecordReplayMiss(pair, MissReasonBodyBufferFailed)
	r.RecordEnqueued(pair)
	r.RecordEnqueued(pair)
	r.RecordPushed(pair, 2)
	r.RecordDropped(pair, 1)
	r.RecordPushRejectedTerminal(pair, 1)
	r.RecordCollision(pair, true)  // divergent
	r.RecordCollision(pair, false) // identical

	got := r.Snapshot(pair)
	check := func(name string, got, want int64) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	check("ReplayHits", got.ReplayHits, 2)
	check("ReplayMisses", got.ReplayMisses, 3)
	check("MissKeyAbsent", got.MissKeyAbsent, 1)
	check("MissNotCommitted", got.MissNotCommitted, 1)
	check("MissBodyBufferFailed", got.MissBodyBufferFailed, 1)
	check("RecordEnqueued", got.RecordEnqueued, 2)
	check("RecordPushed", got.RecordPushed, 2)
	check("RecordDropped", got.RecordDropped, 1)
	check("PushRejectedTerminal", got.PushRejectedTerminal, 1)
	check("KeyCollisionsDivergent", got.KeyCollisionsDivergent, 1)
	check("KeyCollisionsIdentical", got.KeyCollisionsIdentical, 1)

	// The two hits were recorded 50ms and 150ms in the past -- max must be
	// at least 150ms, mean somewhere around the midpoint.
	if got.ReplayAgeMaxMs < 150 {
		t.Errorf("ReplayAgeMaxMs = %d, want >= 150", got.ReplayAgeMaxMs)
	}
	if got.ReplayAgeMeanMs < 90 || got.ReplayAgeMeanMs > 250 {
		t.Errorf("ReplayAgeMeanMs = %d, want roughly ~100 (between the 50ms and 150ms samples)", got.ReplayAgeMeanMs)
	}
}

// TestFidelity_ScopedByPhasePair pins that scoping every counter by
// PhasePair IS the per-phase reset (design doc Q6): two pairs never
// cross-contaminate, and a never-touched pair reads as all-zero rather
// than reflecting some shared/global state.
func TestFidelity_ScopedByPhasePair(t *testing.T) {
	r := NewFidelityRegistry()
	pairA := PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"}
	pairB := PhasePair{ExperimentID: "exp-2", PhaseID: "phase-2"}

	r.RecordReplayHit(pairA, time.Now())
	r.RecordReplayHit(pairA, time.Now())
	r.RecordReplayMiss(pairA, MissReasonKeyAbsent)

	r.RecordReplayMiss(pairB, MissReasonNotCommitted)
	r.RecordReplayMiss(pairB, MissReasonNotCommitted)

	snapA := r.Snapshot(pairA)
	snapB := r.Snapshot(pairB)

	if snapA.ReplayHits != 2 || snapA.ReplayMisses != 1 || snapA.MissKeyAbsent != 1 {
		t.Fatalf("pair A counters wrong: %+v", snapA)
	}
	if snapB.ReplayHits != 0 || snapB.ReplayMisses != 2 || snapB.MissNotCommitted != 2 {
		t.Fatalf("pair B counters wrong: %+v", snapB)
	}
	if snapA.MissNotCommitted != 0 {
		t.Fatalf("pair A must not see pair B's counts, got MissNotCommitted=%d", snapA.MissNotCommitted)
	}
	if snapB.MissKeyAbsent != 0 {
		t.Fatalf("pair B must not see pair A's counts, got MissKeyAbsent=%d", snapB.MissKeyAbsent)
	}

	// A never-touched pair reads as zero -- confirms scoping isn't a
	// shared/global counter in disguise.
	untouched := PhasePair{ExperimentID: "exp-3", PhaseID: "phase-3"}
	snapC := r.Snapshot(untouched)
	if snapC.ReplayHits != 0 || snapC.ReplayMisses != 0 {
		t.Fatalf("untouched pair should read all-zero, got %+v", snapC)
	}
}
