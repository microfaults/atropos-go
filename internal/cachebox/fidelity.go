package cachebox

import (
	"sync"
	"time"
)

// PhasePair identifies one (experiment_id, phase_id) pair for fidelity
// counter scoping (design doc Q6): scoping every counter by this composite
// key IS the per-phase reset -- a new phase is a new key, so there is no
// explicit reset step and no reset race.
type PhasePair struct {
	ExperimentID string
	PhaseID      string
}

// FidelityCounters is a point-in-time read of one PhasePair's counters
// (wire spec §W6 field set, minus InstanceID/Service -- this package
// doesn't know its own service identity; the HTTP handler that builds the
// wire FidelitySnapshot supplies those).
type FidelityCounters struct {
	ReplayHits             int64
	ReplayMisses           int64
	MissKeyAbsent          int64
	MissNotCommitted       int64
	MissBodyBufferFailed   int64
	RecordEnqueued         int64
	RecordPushed           int64
	RecordDropped          int64
	PushRejectedTerminal   int64
	KeyCollisionsDivergent int64
	KeyCollisionsIdentical int64
	ReplayAgeMaxMs         int64
	ReplayAgeMeanMs        int64
	PreloadCommitted       bool
	PreloadEntries         int
	PreloadChecksum        string
	PreloadCommittedAt     time.Time
}

// phaseCounters holds the mutable Q6 counter set for one PhasePair. All
// fields are guarded by the single mutex rather than split into atomics:
// replay-age mean requires a sum+count pair updated together, and the
// preload state is four fields updated together, so a single lock covering
// everything is simpler to reason about than a mix of atomics plus a
// mutex for the composite fields -- these counters are updated at most
// once per request/push/commit, not in a tight hot loop.
type phaseCounters struct {
	mu sync.Mutex

	replayHits             int64
	replayMisses           int64
	missKeyAbsent          int64
	missNotCommitted       int64
	missBodyBufferFailed   int64
	recordEnqueued         int64
	recordPushed           int64
	recordDropped          int64
	pushRejectedTerminal   int64
	keyCollisionsDivergent int64
	keyCollisionsIdentical int64

	replayAgeMaxMs int64
	replayAgeSumMs int64
	replayAgeCount int64

	preloadCommitted   bool
	preloadEntries     int
	preloadChecksum    string
	preloadCommittedAt time.Time
}

func (c *phaseCounters) snapshot() FidelityCounters {
	c.mu.Lock()
	defer c.mu.Unlock()
	var meanMs int64
	if c.replayAgeCount > 0 {
		meanMs = c.replayAgeSumMs / c.replayAgeCount
	}
	return FidelityCounters{
		ReplayHits: c.replayHits, ReplayMisses: c.replayMisses,
		MissKeyAbsent: c.missKeyAbsent, MissNotCommitted: c.missNotCommitted, MissBodyBufferFailed: c.missBodyBufferFailed,
		RecordEnqueued: c.recordEnqueued, RecordPushed: c.recordPushed, RecordDropped: c.recordDropped,
		PushRejectedTerminal:   c.pushRejectedTerminal,
		KeyCollisionsDivergent: c.keyCollisionsDivergent, KeyCollisionsIdentical: c.keyCollisionsIdentical,
		ReplayAgeMaxMs: c.replayAgeMaxMs, ReplayAgeMeanMs: meanMs,
		PreloadCommitted: c.preloadCommitted, PreloadEntries: c.preloadEntries,
		PreloadChecksum: c.preloadChecksum, PreloadCommittedAt: c.preloadCommittedAt,
	}
}

// FidelityRegistry is the per-(experiment_id, phase_id) counter registry
// (design doc Q6) that ATRO-1's temporary package-local miss counter was
// always meant to be replaced by. Every method is nil-safe (a nil
// *FidelityRegistry is a no-op source/sink), so components that hold an
// optional reference (RecordBuffer, Recorder, CachePushClient,
// PreloadStore) never need to nil-check before calling it.
type FidelityRegistry struct {
	mu     sync.Mutex
	phases map[PhasePair]*phaseCounters
}

// NewFidelityRegistry returns an empty registry.
func NewFidelityRegistry() *FidelityRegistry {
	return &FidelityRegistry{phases: make(map[PhasePair]*phaseCounters)}
}

func (r *FidelityRegistry) getOrCreate(pair PhasePair) *phaseCounters {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.phases[pair]
	if !ok {
		c = &phaseCounters{}
		r.phases[pair] = c
	}
	return c
}

func (r *FidelityRegistry) get(pair PhasePair) *phaseCounters {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.phases[pair]
}

// isZero reports whether pair has no attributable identity -- callers with
// no CacheBoxContext (legacy rules, or paths that haven't been given one)
// have nothing to scope a count to, so the increment is silently skipped
// rather than attributed to a bogus empty-string pair.
func (pair PhasePair) isZero() bool {
	return pair.ExperimentID == "" || pair.PhaseID == ""
}

// RecordReplayHit records a replay hit and its staleness (recordedAt to
// now) for pair. A no-op if pair is zero-valued.
func (r *FidelityRegistry) RecordReplayHit(pair PhasePair, recordedAt time.Time) {
	if r == nil || pair.isZero() {
		return
	}
	ageMs := time.Since(recordedAt).Milliseconds()
	if ageMs < 0 {
		ageMs = 0
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.replayHits++
	if ageMs > c.replayAgeMaxMs {
		c.replayAgeMaxMs = ageMs
	}
	c.replayAgeSumMs += ageMs
	c.replayAgeCount++
	c.mu.Unlock()
}

// RecordReplayMiss records a replay miss and its reason for pair. A no-op
// if pair is zero-valued (nothing to attribute the miss to) or reason is
// unrecognized.
func (r *FidelityRegistry) RecordReplayMiss(pair PhasePair, reason string) {
	if r == nil || pair.isZero() {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.replayMisses++
	switch reason {
	case MissReasonKeyAbsent:
		c.missKeyAbsent++
	case MissReasonNotCommitted:
		c.missNotCommitted++
	case MissReasonBodyBufferFailed:
		c.missBodyBufferFailed++
	}
	c.mu.Unlock()
}

// RecordEnqueued records that one entry was successfully handed to the
// recorder for pair.
func (r *FidelityRegistry) RecordEnqueued(pair PhasePair) {
	if r == nil || pair.isZero() {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.recordEnqueued++
	c.mu.Unlock()
}

// RecordPushed records n entries successfully pushed for pair.
func (r *FidelityRegistry) RecordPushed(pair PhasePair, n int64) {
	if r == nil || pair.isZero() || n == 0 {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.recordPushed += n
	c.mu.Unlock()
}

// RecordDropped records n entries that never made it to manteion for pair
// (recorder backpressure, buffer overflow, or exhausted push retries).
func (r *FidelityRegistry) RecordDropped(pair PhasePair, n int64) {
	if r == nil || pair.isZero() || n == 0 {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.recordDropped += n
	c.mu.Unlock()
}

// RecordPushRejectedTerminal records n entries lost to a terminal
// (409 phase_not_recording) push rejection for pair. Callers also report
// the same n via RecordDropped -- this is the specific-reason breakdown,
// not a replacement for the aggregate count.
func (r *FidelityRegistry) RecordPushRejectedTerminal(pair PhasePair, n int64) {
	if r == nil || pair.isZero() || n == 0 {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.pushRejectedTerminal += n
	c.mu.Unlock()
}

// RecordCollision records a same-key record-time collision for pair,
// divergent (differing status/body) or identical (design doc Q3).
func (r *FidelityRegistry) RecordCollision(pair PhasePair, divergent bool) {
	if r == nil || pair.isZero() {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	if divergent {
		c.keyCollisionsDivergent++
	} else {
		c.keyCollisionsIdentical++
	}
	c.mu.Unlock()
}

// SetPreloadState records the outcome of a preload commit attempt for
// pair -- called on both a matching commit (committed=true) and a
// mismatch (committed=false, reporting what was actually staged).
func (r *FidelityRegistry) SetPreloadState(pair PhasePair, committed bool, entries int, checksum string, committedAt time.Time) {
	if r == nil || pair.isZero() {
		return
	}
	c := r.getOrCreate(pair)
	c.mu.Lock()
	c.preloadCommitted = committed
	c.preloadEntries = entries
	c.preloadChecksum = checksum
	c.preloadCommittedAt = committedAt
	c.mu.Unlock()
}

// Snapshot returns a point-in-time read of pair's counters. A pair that
// has never been touched returns a zero-valued snapshot (querying never
// creates registry state).
func (r *FidelityRegistry) Snapshot(pair PhasePair) FidelityCounters {
	if r == nil {
		return FidelityCounters{}
	}
	c := r.get(pair)
	if c == nil {
		return FidelityCounters{}
	}
	return c.snapshot()
}
