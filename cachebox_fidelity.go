package atropos

import (
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CacheBoxContext (wire spec §W1) is the authoritative-end-to-end bundle
// that scopes a cache-box operation to one experiment phase and pins the
// key strategy used to derive/verify cache keys. It is attached to:
//   - the cache-box portion of a compiled rule (CompiledCacheBox.Context)
//   - the freeze command body (DelayRequest.Context)
//   - a preload begin request (PreloadBeginRequest, flattened)
//
// ExperimentID, PhaseID, KeyStrategy, and StrategyVersion are always
// present when Context itself is present -- an empty value in a populated
// Context is a control-plane bug, not an omitted field. KeyHeaders and
// MissStatus are true optionals (zero value = "use defaults").
type CacheBoxContext struct {
	ExperimentID    string   `json:"experiment_id"`
	PhaseID         string   `json:"phase_id"`
	KeyStrategy     string   `json:"key_strategy"`
	StrategyVersion int      `json:"strategy_version"`
	KeyHeaders      []string `json:"key_headers,omitempty"`
	MissStatus      int      `json:"miss_status,omitempty"`
}

// DrainReport (wire spec §W3) is the SDK's POST body to
// /api/v1/sdk/cachebox/drain at recording-phase end, confirming every
// buffered record was flushed (or accounting for what wasn't). Idempotent
// on (ExperimentID, PhaseID, InstanceID).
type DrainReport struct {
	ExperimentID           string `json:"experiment_id"`
	PhaseID                string `json:"phase_id"`
	Service                string `json:"service"`
	InstanceID             string `json:"instance_id"`
	EntriesRecorded        int64  `json:"entries_recorded"`
	EntriesPushed          int64  `json:"entries_pushed"`
	EntriesDropped         int64  `json:"entries_dropped"`
	BatchesSent            int64  `json:"batches_sent"`
	LastBatchSeq           int64  `json:"last_batch_seq"`
	KeyCollisionsDivergent int64  `json:"key_collisions_divergent"`
	KeyCollisionsIdentical int64  `json:"key_collisions_identical"`
}

// DrainReportResponse is manteion's response to a DrainReport.
type DrainReportResponse struct {
	Accepted bool `json:"accepted"`
}

// PreloadBeginRequest (wire spec §W4) starts a staged preload for
// (ExperimentID, PhaseID), clearing any prior staging for that pair. Its
// key-strategy fields mirror CacheBoxContext but are declared independently
// since MissStatus has no meaning here and SourcePhaseID/TotalEntries/
// TotalChunks/MaxBytes do not belong on CacheBoxContext.
type PreloadBeginRequest struct {
	ExperimentID    string   `json:"experiment_id"`
	PhaseID         string   `json:"phase_id"`
	SourcePhaseID   string   `json:"source_phase_id"`
	TotalEntries    int      `json:"total_entries"`
	TotalChunks     int      `json:"total_chunks"`
	KeyStrategy     string   `json:"key_strategy"`
	StrategyVersion int      `json:"strategy_version"`
	KeyHeaders      []string `json:"key_headers,omitempty"`
	MaxBytes        int64    `json:"max_bytes,omitempty"`
}

// PreloadBeginResponse is returned on a successful begin (200). Begin can
// alternatively fail with 413 (too_large) or 409 (unsupported strategy),
// both reported via the shared ErrorResponse envelope.
type PreloadBeginResponse struct {
	OK bool `json:"ok"`
}

// PreloadChunkRequest carries one chunk of staged entries. ChunkSeq makes
// redelivery idempotent.
type PreloadChunkRequest struct {
	ExperimentID string               `json:"experiment_id"`
	PhaseID      string               `json:"phase_id"`
	ChunkSeq     int                  `json:"chunk_seq"`
	Entries      []cachebox.WireEntry `json:"entries"`
}

// PreloadChunkResponse reports the running staged total after accepting
// (or re-accepting, if ChunkSeq was already seen) a chunk.
type PreloadChunkResponse struct {
	StagedTotal int `json:"staged_total"`
}

// PreloadCommitRequest asks the SDK to verify staged entries against
// (TotalEntries, Checksum) -- computed per wire spec §W5 -- and atomically
// swap them in as the live replay set on match.
type PreloadCommitRequest struct {
	ExperimentID string `json:"experiment_id"`
	PhaseID      string `json:"phase_id"`
	TotalEntries int    `json:"total_entries"`
	Checksum     string `json:"checksum"`
}

// PreloadCommitResponse reports the outcome of a commit: OK+200 on a
// checksum match (the swap already happened), or OK=false+409 on mismatch
// (staging was dropped, the prior replay set is untouched). Loaded/Checksum
// describe what the SDK actually staged, for diagnosing a mismatch.
type PreloadCommitResponse struct {
	OK       bool   `json:"ok"`
	Loaded   int    `json:"loaded"`
	Checksum string `json:"checksum"`
}

// PreloadAbortRequest drops any staged (not yet committed) entries for the
// pair. The response is a bare 200 with no body.
type PreloadAbortRequest struct {
	ExperimentID string `json:"experiment_id"`
	PhaseID      string `json:"phase_id"`
}

// FidelityMissReasons breaks down FidelitySnapshot.ReplayMisses by cause.
type FidelityMissReasons struct {
	KeyAbsent        int64 `json:"key_absent"`
	NotCommitted     int64 `json:"not_committed"`
	BodyBufferFailed int64 `json:"body_buffer_failed"`
}

// FidelityReplayAge summarizes replay-hit staleness: milliseconds between
// an entry's RecordedAt and when it was served.
type FidelityReplayAge struct {
	Max  int64 `json:"max"`
	Mean int64 `json:"mean"`
}

// FidelityPreloadState reports the SDK's current committed replay set for
// the queried (experiment_id, phase_id), if any.
type FidelityPreloadState struct {
	Committed   bool      `json:"committed"`
	Entries     int       `json:"entries"`
	Checksum    string    `json:"checksum"`
	CommittedAt time.Time `json:"committed_at"`
}

// FidelitySnapshot (wire spec §W6) is the SDK's response to
// GET /cachebox/fidelity?experiment_id=&phase_id= -- the complete set of
// counters manteion needs to compute a phase's VALID/INVALID verdict from
// one instance, pulled synchronously at the moment the verdict is computed.
type FidelitySnapshot struct {
	InstanceID             string               `json:"instance_id"`
	Service                string               `json:"service"`
	ExperimentID           string               `json:"experiment_id"`
	PhaseID                string               `json:"phase_id"`
	ReplayHits             int64                `json:"replay_hits"`
	ReplayMisses           int64                `json:"replay_misses"`
	MissReasons            FidelityMissReasons  `json:"miss_reasons"`
	RecordEnqueued         int64                `json:"record_enqueued"`
	RecordPushed           int64                `json:"record_pushed"`
	RecordDropped          int64                `json:"record_dropped"`
	PushRejectedTerminal   int64                `json:"push_rejected_terminal"`
	KeyCollisionsDivergent int64                `json:"key_collisions_divergent"`
	KeyCollisionsIdentical int64                `json:"key_collisions_identical"`
	ReplayAgeMs            FidelityReplayAge    `json:"replay_age_ms"`
	Preload                FidelityPreloadState `json:"preload"`
}
