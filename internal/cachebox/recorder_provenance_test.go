package cachebox

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestRecorderPrefersRecordKey pins INV-2's SDK half: when a CacheRecord
// carries the key the interceptor derived from the matched rule's context,
// the recorder stores under exactly that key and stamps the strategy
// provenance -- it must not re-derive with its construction-time keyFn,
// which may be a different strategy than the replay side will use.
func TestRecorderPrefersRecordKey(t *testing.T) {
	store := NewRecordBuffer(RecordBufferConfig{MaxEntries: 10})
	rec := NewRecorder(RecorderConfig{
		Store:   store,
		KeyFunc: func(*http.Request, []byte) string { return "fallback-key" },
	})
	defer rec.Stop()

	req := httptest.NewRequest(http.MethodGet, "http://svc/a", nil)
	rec.Record(CacheRecord{
		Key:             "rule-derived-key",
		KeyStrategy:     string(KeyStrategyCanonicalV2),
		StrategyVersion: 2,
		Request:         req,
		StatusCode:      200,
		ResponseBody:    []byte("x"),
		Timestamp:       time.Now(),
		ExperimentID:    "exp-1",
		PhaseID:         "phase-1",
	})
	rec.Flush()

	e, ok := store.Get("rule-derived-key")
	if !ok {
		t.Fatalf("entry not stored under the record's own key")
	}
	if _, ok := store.Get("fallback-key"); ok {
		t.Fatalf("recorder re-derived the key with its construction-time keyFn")
	}
	if e.KeyStrategy != string(KeyStrategyCanonicalV2) || e.StrategyVersion != 2 {
		t.Fatalf("strategy provenance not stamped: got (%q, %d)", e.KeyStrategy, e.StrategyVersion)
	}

	// Provenance must survive the wire round-trip (INV-2's manteion half --
	// the preload strategy preflight -- reads it off the wire entry).
	w := EntryToWire(e)
	if w.KeyStrategy != e.KeyStrategy || w.StrategyVersion != e.StrategyVersion {
		t.Fatalf("EntryToWire dropped strategy provenance: %+v", w)
	}
	back := WireToEntry(&w)
	if back.KeyStrategy != e.KeyStrategy || back.StrategyVersion != e.StrategyVersion {
		t.Fatalf("WireToEntry dropped strategy provenance: %+v", back)
	}
}

// TestRecorderFallsBackToKeyFn pins the legacy path: a record without a
// pre-derived key (no rule context) still keys via the recorder's keyFn.
func TestRecorderFallsBackToKeyFn(t *testing.T) {
	store := NewRecordBuffer(RecordBufferConfig{MaxEntries: 10})
	rec := NewRecorder(RecorderConfig{
		Store:   store,
		KeyFunc: func(*http.Request, []byte) string { return "fallback-key" },
	})
	defer rec.Stop()

	req := httptest.NewRequest(http.MethodGet, "http://svc/a", nil)
	rec.Record(CacheRecord{
		Request:      req,
		StatusCode:   200,
		Timestamp:    time.Now(),
		ExperimentID: "exp-1",
		PhaseID:      "phase-1",
	})
	rec.Flush()

	if _, ok := store.Get("fallback-key"); !ok {
		t.Fatalf("keyFn fallback not used for a record without a pre-derived key")
	}
}
