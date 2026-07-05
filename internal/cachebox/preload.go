package cachebox

import (
	"sync"
	"time"
)

// supportedKeyStrategies is the recognized set of key_strategy values a
// preload begin may declare (design doc Q4: "validates strategy
// supported"). Kept in sync with key.go's KeyStrategy consts.
var supportedKeyStrategies = map[string]bool{
	string(KeyStrategyExact):         true,
	string(KeyStrategyExactWithHost): true,
	string(KeyStrategyExactWithBody): true,
	string(KeyStrategyCanonicalV2):   true,
}

// BeginResult reports the outcome of PreloadStore.Begin. There is no
// TooLarge case: the max_bytes guard is enforced at Chunk time, where
// staged bytes actually accumulate -- begin cannot breach it.
type BeginResult struct {
	OK                  bool
	UnsupportedStrategy bool // -> HTTP 409
}

// ChunkResult reports the outcome of PreloadStore.Chunk.
type ChunkResult struct {
	OK          bool
	TooLarge    bool // -> HTTP 413
	NoBegin     bool // no active staging for this pair -- caller ordering bug
	StagedTotal int
}

// CommitResult reports the outcome of PreloadStore.Commit.
type CommitResult struct {
	OK       bool // true = checksum matched, swap happened
	NoBegin  bool // no active staging for this pair
	Loaded   int  // actual staged count (== requested TotalEntries iff OK)
	Checksum string
}

// preloadStaging holds in-progress (uncommitted) chunks for at most one
// (experiment_id, phase_id) pair, keyed by chunk_seq for idempotent
// redelivery (design doc Q4, wire spec §W4).
type preloadStaging struct {
	expID, phaseID string
	maxBytes       int64

	chunks      map[int][]*Entry // by chunk_seq
	stagedBytes int64
}

func (s *preloadStaging) countLocked() int {
	n := 0
	for _, es := range s.chunks {
		n += len(es)
	}
	return n
}

// entriesLocked returns every staged entry. Order is unspecified: the only
// consumers are SetChecksum (order-independent by construction, §W5) and
// the install map (unordered), so there is nothing to sort for.
func (s *preloadStaging) entriesLocked() []*Entry {
	var all []*Entry
	for _, es := range s.chunks {
		all = append(all, es...)
	}
	return all
}

// PreloadStore coordinates the ATRO-6 staged preload protocol (begin/
// chunk/commit/abort, wire spec §W4) for one CacheBox, installing a
// checksum-verified set into its ReplaySet on commit. A half-delivered
// preload (any state before a matching commit) is never visible to
// replay -- Commit is the only path that calls ReplaySet.Install.
type PreloadStore struct {
	replaySet *ReplaySet
	fidelity  *FidelityRegistry

	mu      sync.Mutex
	staging *preloadStaging
}

// NewPreloadStore builds a PreloadStore that installs into rs on commit,
// reporting preload state to fidelity (ATRO-7, design doc Q6) -- nil is
// fine, all FidelityRegistry methods are nil-safe.
func NewPreloadStore(rs *ReplaySet, fidelity *FidelityRegistry) *PreloadStore {
	return &PreloadStore{replaySet: rs, fidelity: fidelity}
}

// Begin starts a new staged preload for (experimentID, phaseID), clearing
// any prior staging for that pair (design doc Q4/W4) -- including staging
// left over from a prior, never-committed-or-aborted begin. maxBytes <= 0
// means no cap.
func (p *PreloadStore) Begin(experimentID, phaseID, keyStrategy string, maxBytes int64) BeginResult {
	if !supportedKeyStrategies[keyStrategy] {
		return BeginResult{UnsupportedStrategy: true}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.staging = &preloadStaging{
		expID: experimentID, phaseID: phaseID,
		maxBytes: maxBytes,
		chunks:   make(map[int][]*Entry),
	}
	return BeginResult{OK: true}
}

// Chunk appends entries for chunkSeq to the active staging for (experimentID,
// phaseID). Idempotent by chunkSeq: redelivering the same chunkSeq (e.g. a
// client retry) does not double-count bytes or entries.
func (p *PreloadStore) Chunk(experimentID, phaseID string, chunkSeq int, entries []*Entry) ChunkResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	s := p.staging
	if s == nil || s.expID != experimentID || s.phaseID != phaseID {
		return ChunkResult{NoBegin: true}
	}

	if _, seen := s.chunks[chunkSeq]; !seen {
		var size int64
		for _, e := range entries {
			size += int64(e.Size())
		}
		if s.maxBytes > 0 && s.stagedBytes+size > s.maxBytes {
			return ChunkResult{TooLarge: true}
		}
		s.chunks[chunkSeq] = entries
		s.stagedBytes += size
	}
	return ChunkResult{OK: true, StagedTotal: s.countLocked()}
}

// Commit verifies the staged set for (experimentID, phaseID) against
// (totalEntries, checksum) -- wire spec §W5 -- and, on an exact match,
// atomically installs it as that pair's ReplaySet (dropping whatever was
// previously installed, for this pair or any other -- Install always
// replaces, never merges). On mismatch, staging is dropped without
// installing anything; any prior ReplaySet is left completely untouched.
// Either way, staging for this pair is cleared -- callers must Begin again
// to retry.
func (p *PreloadStore) Commit(experimentID, phaseID string, totalEntries int, checksum string) CommitResult {
	p.mu.Lock()
	s := p.staging
	if s == nil || s.expID != experimentID || s.phaseID != phaseID {
		p.mu.Unlock()
		return CommitResult{NoBegin: true}
	}
	entries := s.entriesLocked()
	p.mu.Unlock()

	actualChecksum := SetChecksum(entries)
	actualCount := len(entries)
	match := actualCount == totalEntries && actualChecksum == checksum
	committedAt := time.Now()

	if match {
		installed := make(map[string]*Entry, len(entries))
		for _, e := range entries {
			installed[e.Key] = e
		}
		p.replaySet.Install(PhaseKey(experimentID, phaseID), installed)
	}
	pair := PhasePair{ExperimentID: experimentID, PhaseID: phaseID}
	p.fidelity.SetPreloadState(pair, match, actualCount, actualChecksum, committedAt)

	p.mu.Lock()
	if p.staging == s {
		p.staging = nil
	}
	p.mu.Unlock()

	return CommitResult{OK: match, Loaded: actualCount, Checksum: actualChecksum}
}

// Abort drops any staged (uncommitted) entries for (experimentID, phaseID).
// A no-op if there is no matching active staging.
func (p *PreloadStore) Abort(experimentID, phaseID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.staging != nil && p.staging.expID == experimentID && p.staging.phaseID == phaseID {
		p.staging = nil
	}
}
