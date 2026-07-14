package atropos

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// TestFidelity_SnapshotGoldenJSON runs a scripted sequence through the
// real registry, serves it through the real HTTP handler, and checks both
// that every §W6 wire key is present and that the handler maps each
// counter to the right field (not just that the wire TYPE round-trips,
// which TestWireContract_JSONRoundTrip already covers).
func TestFidelity_SnapshotGoldenJSON(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()

	pair := cachebox.PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"}
	fid := cb.Fidelity()
	fid.RecordReplayHit(pair, time.Now().Add(-100*time.Millisecond))
	fid.RecordReplayHit(pair, time.Now().Add(-200*time.Millisecond))
	fid.RecordReplayMiss(pair, cachebox.MissReasonKeyAbsent)
	fid.RecordEnqueued(pair)
	fid.RecordPushed(pair, 1)
	fid.RecordDropped(pair, 1)
	fid.RecordPushRejectedTerminal(pair, 1)
	fid.RecordCollision(pair, true)
	fid.RecordCollision(pair, false)
	fid.SetPreloadState(pair, true, 12345, "deadbeef", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	handler := cacheBoxFidelityHandler(cb, "cart", "pod-1")
	req := httptest.NewRequest(http.MethodGet, "/cachebox/fidelity?experiment_id=exp-1&phase_id=phase-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	raw := rec.Body.String()
	for _, key := range []string{
		`"instance_id"`, `"service"`, `"experiment_id"`, `"phase_id"`,
		`"replay_hits"`, `"replay_misses"`, `"miss_reasons"`,
		`"key_absent"`, `"not_committed"`, `"body_buffer_failed"`,
		`"record_enqueued"`, `"record_pushed"`, `"record_dropped"`,
		`"push_rejected_terminal"`,
		`"key_collisions_divergent"`, `"key_collisions_identical"`,
		`"replay_age_ms"`, `"max"`, `"mean"`,
		`"preload"`, `"committed"`, `"entries"`, `"checksum"`, `"committed_at"`,
	} {
		if !strings.Contains(raw, key) {
			t.Errorf("snapshot missing wire key %s: %s", key, raw)
		}
	}

	var snap FidelitySnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.InstanceID != "pod-1" || snap.Service != "cart" {
		t.Errorf("instance_id/service = %q/%q, want pod-1/cart", snap.InstanceID, snap.Service)
	}
	if snap.ExperimentID != "exp-1" || snap.PhaseID != "phase-1" {
		t.Errorf("experiment_id/phase_id = %q/%q, want exp-1/phase-1", snap.ExperimentID, snap.PhaseID)
	}
	if snap.ReplayHits != 2 {
		t.Errorf("replay_hits = %d, want 2", snap.ReplayHits)
	}
	if snap.ReplayMisses != 1 || snap.MissReasons.KeyAbsent != 1 {
		t.Errorf("replay_misses/key_absent = %d/%d, want 1/1", snap.ReplayMisses, snap.MissReasons.KeyAbsent)
	}
	if snap.MissReasons.NotCommitted != 0 || snap.MissReasons.BodyBufferFailed != 0 {
		t.Errorf("unexpected non-zero miss reasons: %+v", snap.MissReasons)
	}
	if snap.RecordEnqueued != 1 || snap.RecordPushed != 1 || snap.RecordDropped != 1 || snap.PushRejectedTerminal != 1 {
		t.Errorf("record counters mismatch: %+v", snap)
	}
	if snap.KeyCollisionsDivergent != 1 || snap.KeyCollisionsIdentical != 1 {
		t.Errorf("collision counters mismatch: %+v", snap)
	}
	if snap.ReplayAgeMs.Max < 200 {
		t.Errorf("replay_age_ms.max = %d, want >= 200", snap.ReplayAgeMs.Max)
	}
	if !snap.Preload.Committed || snap.Preload.Entries != 12345 || snap.Preload.Checksum != "deadbeef" {
		t.Errorf("preload state mismatch: %+v", snap.Preload)
	}
	if !snap.Preload.CommittedAt.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("preload.committed_at = %v, want 2026-01-01", snap.Preload.CommittedAt)
	}
}

func TestFidelity_RequiresExperimentAndPhaseID(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxFidelityHandler(cb, "cart", "pod-1")

	req := httptest.NewRequest(http.MethodGet, "/cachebox/fidelity", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without query params, got %d: %s", rec.Code, rec.Body.String())
	}
}
