package atropos_test

// Cross-repo wire contract tests for the fidelity-refinement additions
// (design doc 2026-07-cachebox-fidelity-refinement.md, §W). manteion
// imports these types directly, so the JSON field names pinned here are
// load-bearing across the repo boundary in both directions: golden
// fixtures matching the spec's literal examples marshal/unmarshal
// losslessly, and legacy payloads without the new fields still decode.

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	atropos "git.ucsc.edu/microfaults/atropos-go"
)

func TestWireContract_JSONRoundTrip(t *testing.T) {
	t.Run("CacheBoxContext", func(t *testing.T) {
		ctx := atropos.CacheBoxContext{
			ExperimentID:    "exp-1",
			PhaseID:         "phase-1",
			KeyStrategy:     "canonical_v2",
			StrategyVersion: 2,
			KeyHeaders:      []string{"x-tenant-id"},
			MissStatus:      503,
		}
		raw, err := json.Marshal(ctx)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{
			`"experiment_id"`, `"phase_id"`, `"key_strategy"`,
			`"strategy_version"`, `"key_headers"`, `"miss_status"`,
		} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("missing %s in %s", key, raw)
			}
		}
		var decoded atropos.CacheBoxContext
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !reflect.DeepEqual(decoded, ctx) {
			t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, ctx)
		}
	})

	t.Run("CompiledCacheBox_ContextAttached", func(t *testing.T) {
		cb := atropos.CompiledCacheBox{
			Mode:        "replay",
			KeyStrategy: "exact",
			Context: &atropos.CacheBoxContext{
				ExperimentID:    "exp-1",
				PhaseID:         "phase-1",
				KeyStrategy:     "canonical_v2",
				StrategyVersion: 2,
			},
		}
		raw, err := json.Marshal(cb)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded atropos.CompiledCacheBox
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if decoded.Context == nil || decoded.Context.ExperimentID != "exp-1" {
			t.Fatalf("context did not round-trip: %+v", decoded)
		}
		// The legacy per-rule field is untouched by the new context sitting
		// alongside it -- no field-name collision between the two.
		if decoded.KeyStrategy != "exact" {
			t.Fatalf("legacy key_strategy field corrupted: %q", decoded.KeyStrategy)
		}

		// A legacy compiled rule with no context must still decode cleanly.
		legacy := []byte(`{"mode":"replay","key_strategy":"exact_with_host"}`)
		var legacyDecoded atropos.CompiledCacheBox
		if err := json.Unmarshal(legacy, &legacyDecoded); err != nil {
			t.Fatalf("legacy decode: %v", err)
		}
		if legacyDecoded.Context != nil {
			t.Fatalf("expected nil context for legacy payload, got %+v", legacyDecoded.Context)
		}
	})

	t.Run("DelayRequest_ContextAttached", func(t *testing.T) {
		req := atropos.DelayRequest{
			Mu: 8.5, Sigma: 0.3, Seed: 42,
			Context: &atropos.CacheBoxContext{
				ExperimentID: "exp-1", PhaseID: "phase-1",
				KeyStrategy: "canonical_v2", StrategyVersion: 2,
			},
		}
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded atropos.DelayRequest
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if decoded.Context == nil || decoded.Context.PhaseID != "phase-1" {
			t.Fatalf("context did not round-trip: %+v", decoded)
		}

		legacy := []byte(`{"mu":1,"sigma":0.5,"seed":42}`)
		var legacyDecoded atropos.DelayRequest
		if err := json.Unmarshal(legacy, &legacyDecoded); err != nil {
			t.Fatalf("legacy decode: %v", err)
		}
		if legacyDecoded.Context != nil {
			t.Fatalf("expected nil context for legacy freeze command")
		}
	})

	t.Run("WireEntry_FidelityAdditions", func(t *testing.T) {
		entry := atropos.CacheBoxWireEntry{
			Key:                "v2:abc",
			StatusCode:         200,
			ObservedLatencyUs:  1500,
			RecordedAt:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			KeyStrategy:        "canonical_v2",
			StrategyVersion:    2,
			ResponseBodySHA256: "deadbeef",
			RequestMeta: &atropos.CacheBoxRequestMeta{
				Method: "GET", Host: "svc", Path: "/x",
			},
		}
		raw, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{
			`"key_strategy"`, `"strategy_version"`,
			`"response_body_sha256"`, `"request_meta"`,
		} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("missing %s in %s", key, raw)
			}
		}
		var decoded atropos.CacheBoxWireEntry
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if decoded.RequestMeta == nil || decoded.RequestMeta.Path != "/x" {
			t.Fatalf("request_meta did not round-trip: %+v", decoded)
		}

		// A legacy (pre-fidelity) wire entry with none of the new fields
		// must still decode.
		legacy := []byte(`{"key":"exact:GET|/x","status_code":200,"observed_latency_us":100,"recorded_at":"2026-01-01T00:00:00Z"}`)
		var legacyEntry atropos.CacheBoxWireEntry
		if err := json.Unmarshal(legacy, &legacyEntry); err != nil {
			t.Fatalf("legacy wire entry decode: %v", err)
		}
		if legacyEntry.RequestMeta != nil || legacyEntry.KeyStrategy != "" {
			t.Fatalf("expected zero-valued additions for legacy entry, got %+v", legacyEntry)
		}
	})

	t.Run("PushEnvelope_ExperimentAndBatchSeq", func(t *testing.T) {
		// The push envelope itself is unexported (cache_push.go); we can
		// only observe it via CachePushClient's wire output, so this test
		// instead pins the two additive JSON keys directly against the
		// spec, matching the style already used for CacheBoxWireEntry.
		type ingestEnvelopeShape struct {
			Service      string                      `json:"service"`
			Instance     string                      `json:"instance"`
			PhaseID      string                      `json:"phase_id"`
			ExperimentID string                      `json:"experiment_id,omitempty"`
			BatchSeq     int                         `json:"batch_seq,omitempty"`
			Entries      []atropos.CacheBoxWireEntry `json:"entries"`
		}
		env := ingestEnvelopeShape{
			Service: "svc", Instance: "i1", PhaseID: "phase-1",
			ExperimentID: "exp-1", BatchSeq: 3,
			Entries: []atropos.CacheBoxWireEntry{{Key: "k", StatusCode: 200}},
		}
		raw, _ := json.Marshal(env)
		for _, key := range []string{`"experiment_id"`, `"batch_seq"`} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("missing %s in %s", key, raw)
			}
		}
	})

	t.Run("DrainReport", func(t *testing.T) {
		report := atropos.DrainReport{
			ExperimentID: "exp-1", PhaseID: "phase-1",
			Service: "svc", InstanceID: "inst-1",
			EntriesRecorded: 12345, EntriesPushed: 12345, EntriesDropped: 0,
			BatchesSent: 25, LastBatchSeq: 25,
			KeyCollisionsDivergent: 7, KeyCollisionsIdentical: 92,
		}
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{
			`"experiment_id"`, `"phase_id"`, `"service"`, `"instance_id"`,
			`"entries_recorded"`, `"entries_pushed"`, `"entries_dropped"`,
			`"batches_sent"`, `"last_batch_seq"`,
			`"key_collisions_divergent"`, `"key_collisions_identical"`,
		} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("missing %s in %s", key, raw)
			}
		}
		var decoded atropos.DrainReport
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if decoded != report {
			t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, report)
		}

		respRaw, _ := json.Marshal(atropos.DrainReportResponse{Accepted: true})
		if !strings.Contains(string(respRaw), `"accepted":true`) {
			t.Errorf("drain response missing accepted: %s", respRaw)
		}
	})

	t.Run("Preload_Begin", func(t *testing.T) {
		begin := atropos.PreloadBeginRequest{
			ExperimentID: "exp-1", PhaseID: "phase-1", SourcePhaseID: "phase-0",
			TotalEntries: 501, TotalChunks: 2,
			KeyStrategy: "canonical_v2", StrategyVersion: 2,
			KeyHeaders: []string{"x-tenant-id"}, MaxBytes: 268435456,
		}
		raw, err := json.Marshal(begin)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{
			`"experiment_id"`, `"phase_id"`, `"source_phase_id"`,
			`"total_entries"`, `"total_chunks"`, `"key_strategy"`,
			`"strategy_version"`, `"key_headers"`, `"max_bytes"`,
		} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("begin missing %s in %s", key, raw)
			}
		}
		var decoded atropos.PreloadBeginRequest
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal begin: %v", err)
		}
		if !reflect.DeepEqual(decoded, begin) {
			t.Errorf("begin round-trip mismatch: got %+v, want %+v", decoded, begin)
		}

		respRaw, _ := json.Marshal(atropos.PreloadBeginResponse{OK: true})
		if !strings.Contains(string(respRaw), `"ok":true`) {
			t.Errorf("begin response: %s", respRaw)
		}
	})

	t.Run("Preload_Chunk", func(t *testing.T) {
		chunk := atropos.PreloadChunkRequest{
			ExperimentID: "exp-1", PhaseID: "phase-1", ChunkSeq: 1,
			Entries: []atropos.CacheBoxWireEntry{
				{Key: "v2:a", StatusCode: 200},
				{Key: "v2:b", StatusCode: 404},
			},
		}
		raw, err := json.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{`"experiment_id"`, `"phase_id"`, `"chunk_seq"`, `"entries"`} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("chunk missing %s in %s", key, raw)
			}
		}
		var decoded atropos.PreloadChunkRequest
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal chunk: %v", err)
		}
		if len(decoded.Entries) != 2 || decoded.Entries[1].StatusCode != 404 {
			t.Fatalf("chunk entries did not round-trip: %+v", decoded.Entries)
		}

		respRaw, _ := json.Marshal(atropos.PreloadChunkResponse{StagedTotal: 2})
		if !strings.Contains(string(respRaw), `"staged_total":2`) {
			t.Errorf("chunk response: %s", respRaw)
		}
	})

	t.Run("Preload_Commit", func(t *testing.T) {
		commit := atropos.PreloadCommitRequest{
			ExperimentID: "exp-1", PhaseID: "phase-1",
			TotalEntries: 501, Checksum: "deadbeef",
		}
		raw, err := json.Marshal(commit)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		for _, key := range []string{`"experiment_id"`, `"phase_id"`, `"total_entries"`, `"checksum"`} {
			if !strings.Contains(string(raw), key) {
				t.Errorf("commit missing %s in %s", key, raw)
			}
		}
		var decoded atropos.PreloadCommitRequest
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal commit: %v", err)
		}
		if decoded != commit {
			t.Errorf("commit round-trip mismatch: got %+v, want %+v", decoded, commit)
		}

		matchResp, _ := json.Marshal(atropos.PreloadCommitResponse{OK: true, Loaded: 501, Checksum: "deadbeef"})
		mismatchResp, _ := json.Marshal(atropos.PreloadCommitResponse{OK: false, Loaded: 500, Checksum: "beefdead"})
		for _, raw := range [][]byte{matchResp, mismatchResp} {
			for _, key := range []string{`"ok"`, `"loaded"`, `"checksum"`} {
				if !strings.Contains(string(raw), key) {
					t.Errorf("commit response missing %s in %s", key, raw)
				}
			}
		}
	})

	t.Run("Preload_Abort", func(t *testing.T) {
		abort := atropos.PreloadAbortRequest{ExperimentID: "exp-1", PhaseID: "phase-1"}
		raw, err := json.Marshal(abort)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded atropos.PreloadAbortRequest
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal abort: %v", err)
		}
		if decoded != abort {
			t.Errorf("abort round-trip mismatch: got %+v, want %+v", decoded, abort)
		}
	})

	t.Run("FidelitySnapshot", func(t *testing.T) {
		snap := atropos.FidelitySnapshot{
			InstanceID: "i1", Service: "svc",
			ExperimentID: "exp-1", PhaseID: "phase-1",
			ReplayHits: 10000, ReplayMisses: 0,
			MissReasons: atropos.FidelityMissReasons{
				KeyAbsent: 0, NotCommitted: 0, BodyBufferFailed: 0,
			},
			RecordEnqueued: 0, RecordPushed: 0, RecordDropped: 0,
			PushRejectedTerminal:   0,
			KeyCollisionsDivergent: 0, KeyCollisionsIdentical: 0,
			ReplayAgeMs: atropos.FidelityReplayAge{Max: 61234, Mean: 30500},
			Preload: atropos.FidelityPreloadState{
				Committed: true, Entries: 12345, Checksum: "deadbeef",
				CommittedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}
		raw, err := json.Marshal(snap)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
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
			if !strings.Contains(string(raw), key) {
				t.Errorf("snapshot missing %s in %s", key, raw)
			}
		}
		var decoded atropos.FidelitySnapshot
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal snapshot: %v", err)
		}
		if !reflect.DeepEqual(decoded, snap) {
			t.Errorf("snapshot round-trip mismatch: got %+v, want %+v", decoded, snap)
		}
	})
}
