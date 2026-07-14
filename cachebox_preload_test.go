package atropos

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

func doPreloadRequest(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// mustChecksum computes the wire spec §W5 checksum the same way the
// handler does, for constructing commit requests in tests.
func mustChecksum(t *testing.T, entries []CacheBoxWireEntry) string {
	t.Helper()
	converted := make([]*cachebox.Entry, len(entries))
	for i := range entries {
		converted[i] = cachebox.WireToEntry(&entries[i])
	}
	return cachebox.SetChecksum(converted)
}

// mustPreload runs begin+chunk(1)+commit to completion for entries under
// (experimentID, phaseID), failing the test on any non-200.
func mustPreload(t *testing.T, handler http.Handler, experimentID, phaseID string, entries []CacheBoxWireEntry) {
	t.Helper()
	rec := doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: experimentID, PhaseID: phaseID,
		TotalEntries: len(entries), TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("begin: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	rec = doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: experimentID, PhaseID: phaseID, ChunkSeq: 1, Entries: entries,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("chunk: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	rec = doPreloadRequest(t, handler, "/cachebox/preload/commit", PreloadCommitRequest{
		ExperimentID: experimentID, PhaseID: phaseID,
		TotalEntries: len(entries), Checksum: mustChecksum(t, entries),
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("commit: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestPreload_StageCommitSwap pins the ATRO-6 done-criteria: a checksum
// computed per the exact §W5 fixture matches, entries hit only after
// commit (never before), and the commit response carries a verifiable
// count+checksum.
func TestPreload_StageCommitSwap(t *testing.T) {
	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	entries := []CacheBoxWireEntry{
		{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")},
		{Key: "v2:bbb", StatusCode: 404, Body: []byte("")},
	}
	// Independently hand-computed (see internal/cachebox/checksum_test.go's
	// golden fixture, same entries) -- not just self-consistency with
	// mustChecksum's use of the same production code.
	const wantChecksum = "b39b7e27ad99477228741443d70b1a721d946dddaf65e3526d25a2b1c9bbe035"
	if got := mustChecksum(t, entries); got != wantChecksum {
		t.Fatalf("test setup checksum = %s, want %s (fixture drifted from checksum_test.go)", got, wantChecksum)
	}

	if _, ok := cb.Lookup("v2:aaa"); ok {
		t.Fatal("unexpected hit before any preload")
	}

	rec := doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", SourcePhaseID: "phase-0",
		TotalEntries: 2, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("begin: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1, Entries: entries,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("chunk: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var chunkResp PreloadChunkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &chunkResp); err != nil {
		t.Fatalf("decode chunk response: %v", err)
	}
	if chunkResp.StagedTotal != 2 {
		t.Fatalf("staged_total = %d, want 2", chunkResp.StagedTotal)
	}

	// Still a miss -- nothing is visible before commit.
	if _, ok := cb.Lookup("v2:aaa"); ok {
		t.Fatal("unexpected hit before commit (half-delivered preload must never be visible)")
	}

	rec = doPreloadRequest(t, handler, "/cachebox/preload/commit", PreloadCommitRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1",
		TotalEntries: 2, Checksum: wantChecksum,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("commit: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var commitResp PreloadCommitResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &commitResp); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if !commitResp.OK || commitResp.Loaded != 2 || commitResp.Checksum != wantChecksum {
		t.Fatalf("unexpected commit response: %+v", commitResp)
	}

	entry, ok := cb.Lookup("v2:aaa")
	if !ok || string(entry.Body) != "hello" {
		t.Fatalf("expected hit for v2:aaa after commit, got ok=%v entry=%+v", ok, entry)
	}
	entry2, ok := cb.Lookup("v2:bbb")
	if !ok || entry2.StatusCode != 404 {
		t.Fatalf("expected hit for v2:bbb after commit, got ok=%v entry=%+v", ok, entry2)
	}
}

// TestPreload_ChunkRedeliveryIdempotent pins that redelivering the same
// chunk_seq (an SDK/client retry) never double-counts staged entries.
func TestPreload_ChunkRedeliveryIdempotent(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})

	chunkReq := PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
		Entries: []CacheBoxWireEntry{{Key: "v2:a", StatusCode: 200, Body: []byte("x")}},
	}
	for i := 0; i < 3; i++ {
		rec := doPreloadRequest(t, handler, "/cachebox/preload/chunk", chunkReq)
		if rec.Code != http.StatusOK {
			t.Fatalf("chunk delivery %d: expected 200, got %d: %s", i, rec.Code, rec.Body.String())
		}
		var resp PreloadChunkResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode chunk response: %v", err)
		}
		if resp.StagedTotal != 1 {
			t.Fatalf("chunk delivery %d: staged_total = %d, want 1 (idempotent redelivery)", i, resp.StagedTotal)
		}
	}
}

// TestPreload_CommitMismatchNoSwap pins that a tampered entry (staged
// content diverges from what the commit's checksum claims) is rejected
// with 409, drops staging, and leaves any prior installed set untouched.
func TestPreload_CommitMismatchNoSwap(t *testing.T) {
	cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	// A valid, live prior set.
	priorEntries := []CacheBoxWireEntry{{Key: "v2:aaa", StatusCode: 200, Body: []byte("hello")}}
	mustPreload(t, handler, "exp-1", "phase-1", priorEntries)
	if _, ok := cb.Lookup("v2:aaa"); !ok {
		t.Fatal("expected prior set to be live after initial commit")
	}

	// A new preload for a different pair: what's actually staged (tampered)
	// differs from what the commit's checksum claims (computed from the
	// correct body) -- e.g. a chunk corrupted in transit.
	doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-2", PhaseID: "phase-2", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	tamperedEntries := []CacheBoxWireEntry{{Key: "v2:zzz", StatusCode: 200, Body: []byte("TAMPERED")}}
	doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-2", PhaseID: "phase-2", ChunkSeq: 1, Entries: tamperedEntries,
	})
	correctEntries := []CacheBoxWireEntry{{Key: "v2:zzz", StatusCode: 200, Body: []byte("correct-value")}}
	rec := doPreloadRequest(t, handler, "/cachebox/preload/commit", PreloadCommitRequest{
		ExperimentID: "exp-2", PhaseID: "phase-2",
		TotalEntries: 1, Checksum: mustChecksum(t, correctEntries),
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on checksum mismatch, got %d: %s", rec.Code, rec.Body.String())
	}
	var commitResp PreloadCommitResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &commitResp); err != nil {
		t.Fatalf("decode commit response: %v", err)
	}
	if commitResp.OK {
		t.Fatal("expected ok=false on checksum mismatch")
	}

	// Prior set (exp-1/phase-1) must remain untouched -- no swap happened.
	if entry, ok := cb.Lookup("v2:aaa"); !ok || string(entry.Body) != "hello" {
		t.Fatalf("prior replay set must remain untouched after a failed commit, got ok=%v", ok)
	}
	// The tampered/mismatched set must not be visible either.
	if _, ok := cb.Lookup("v2:zzz"); ok {
		t.Fatal("mismatched commit must not install anything")
	}
}

// TestPreload_OversizeRejected413 pins the max_bytes guard: a chunk that
// would push staged bytes over the cap is rejected with 413 and never
// staged, while a within-cap chunk in the same session still succeeds.
func TestPreload_OversizeRejected413(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
		MaxBytes: 10, // tiny cap
	})

	rec := doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
		Entries: []CacheBoxWireEntry{{Key: "v2:aaa", StatusCode: 200, Body: []byte(strings.Repeat("x", 100))}},
	})
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", rec.Code, rec.Body.String())
	}

	// The oversize chunk must not have been staged: a subsequent, small
	// chunk should report staged_total=1, not 2.
	rec = doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 2,
		Entries: []CacheBoxWireEntry{{Key: "v2:bbb", StatusCode: 200, Body: []byte("ok")}},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a within-cap chunk, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp PreloadChunkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode chunk response: %v", err)
	}
	if resp.StagedTotal != 1 {
		t.Fatalf("staged_total = %d, want 1 (the oversize chunk must not have been staged)", resp.StagedTotal)
	}
}

// TestPreload_BeginClearsPriorStaging pins that begin always clears
// whatever was staged (but never committed) before it for the same pair.
func TestPreload_BeginClearsPriorStaging(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 3, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
		Entries: []CacheBoxWireEntry{
			{Key: "v2:a", StatusCode: 200, Body: []byte("1")},
			{Key: "v2:b", StatusCode: 200, Body: []byte("2")},
			{Key: "v2:c", StatusCode: 200, Body: []byte("3")},
		},
	})

	// A second begin for the SAME pair must clear the 3 already-staged
	// entries -- a subsequent chunk with 1 new entry should report
	// staged_total=1, not 4.
	rec := doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("second begin: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	rec = doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
		Entries: []CacheBoxWireEntry{{Key: "v2:z", StatusCode: 200, Body: []byte("new")}},
	})
	var resp PreloadChunkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode chunk response: %v", err)
	}
	if resp.StagedTotal != 1 {
		t.Fatalf("staged_total = %d, want 1 -- begin must clear prior staging", resp.StagedTotal)
	}
}

func TestPreload_AbortDropsStaging(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "canonical_v2", StrategyVersion: 2,
	})
	doPreloadRequest(t, handler, "/cachebox/preload/chunk", PreloadChunkRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
		Entries: []CacheBoxWireEntry{{Key: "v2:a", StatusCode: 200, Body: []byte("x")}},
	})

	rec := doPreloadRequest(t, handler, "/cachebox/preload/abort", PreloadAbortRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("abort: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Commit after abort must fail (no active staging) and install nothing.
	rec = doPreloadRequest(t, handler, "/cachebox/preload/commit", PreloadCommitRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, Checksum: "anything",
	})
	if rec.Code == http.StatusOK {
		t.Fatalf("commit after abort should not succeed, got 200: %s", rec.Body.String())
	}
	if _, ok := cb.Lookup("v2:a"); ok {
		t.Fatal("aborted staging must never become visible to replay")
	}
}

func TestPreload_UnsupportedStrategyRejected409(t *testing.T) {
	cb := cachebox.New(cachebox.Config{})
	defer cb.Stop()
	handler := cacheBoxPreloadHandler(cb)

	rec := doPreloadRequest(t, handler, "/cachebox/preload/begin", PreloadBeginRequest{
		ExperimentID: "exp-1", PhaseID: "phase-1", TotalEntries: 1, TotalChunks: 1,
		KeyStrategy: "some_future_strategy", StrategyVersion: 1,
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for an unsupported key_strategy, got %d: %s", rec.Code, rec.Body.String())
	}
}
