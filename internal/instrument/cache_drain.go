package instrument

import (
	"log/slog"
	"sync"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// recordingPhaseKey identifies one (experiment_id, phase_id) pair a
// compiled rule set is authorizing recording for.
type recordingPhaseKey struct {
	ExperimentID string
	PhaseID      string
}

// activeRecordingPhases scans rules for cache-box passthrough actions
// carrying a wire.CacheBoxContext and returns the set of (experiment_id,
// phase_id) pairs they authorize recording for. Used to detect phase
// transitions across a rule update (design doc Q2/Q5): a pair present in
// an older call but absent from a newer one has stopped recording.
func activeRecordingPhases(rules []wire.CompiledRule) map[recordingPhaseKey]bool {
	active := make(map[recordingPhaseKey]bool)
	for _, r := range rules {
		if r.CacheBox == nil || r.CacheBox.Mode != "passthrough" || r.CacheBox.Context == nil {
			continue
		}
		ctx := r.CacheBox.Context
		if ctx.ExperimentID == "" || ctx.PhaseID == "" {
			continue
		}
		active[recordingPhaseKey{ExperimentID: ctx.ExperimentID, PhaseID: ctx.PhaseID}] = true
	}
	return active
}

// endedRecordingPhases returns the keys present in prev but absent from
// curr -- phases whose recording stopped as of this rule update.
func endedRecordingPhases(prev, curr map[recordingPhaseKey]bool) []recordingPhaseKey {
	var ended []recordingPhaseKey
	for k := range prev {
		if !curr[k] {
			ended = append(ended, k)
		}
	}
	return ended
}

// cacheDrainTracker watches successive rule syncs for cache-box recording
// phases that have ended and triggers a flush + W3 drain report for each
// (design doc Q2: "the poll-driven bump manteion issues at drain start"
// removes the phase's wire.CacheBoxContext from the rule set). Wire it via
// applyTargets.CacheDrain; apply calls Observe for every authoritative
// (non-nil, including empty) rule set it applies -- the empty set is the
// common drain trigger, since a baseline-recorded service usually has no
// rules besides the synthesized recording rule -- under the core's apply
// mutex, and Drain for the phases it returned once that mutex (and any lock
// of the caller's) is released.
type cacheDrainTracker struct {
	cb     *cachebox.CacheBox
	pusher *cachePushClient
	logger *slog.Logger

	mu     sync.Mutex
	active map[recordingPhaseKey]bool

	// drainMu runs the drains one at a time. A drain's flush takes over
	// whatever batch the push client holds, of any pair, and posts it before
	// the report is built; a second drain built meanwhile would snapshot its
	// pair's counts with that batch still in flight and report them short.
	// Not an apply lock: drains run after those are released, so the I/O stays
	// outside them.
	drainMu sync.Mutex
}

// newCacheDrainTracker builds a tracker that flushes cb and reports through
// pusher whenever Observe sees a previously-active recording phase drop
// out of the rule set. logger defaults to slog.Default() if nil.
func newCacheDrainTracker(cb *cachebox.CacheBox, pusher *cachePushClient, logger *slog.Logger) *cacheDrainTracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &cacheDrainTracker{cb: cb, pusher: pusher, logger: logger, active: map[recordingPhaseKey]bool{}}
}

// Observe diffs rules' active recording phases against the previous call,
// records them as the active set, and returns the phases that dropped out:
// their recording ended with this rule set. It does no I/O, so apply calls it
// under the core's apply mutex and every delivery is observed exactly once,
// in apply order; the drain reports go out through Drain. Nil-safe (a nil
// tracker observes nothing), so applyTargets.CacheDrain is a true optional.
func (t *cacheDrainTracker) Observe(rules []wire.CompiledRule) []recordingPhaseKey {
	if t == nil {
		return nil
	}
	curr := activeRecordingPhases(rules)

	t.mu.Lock()
	defer t.mu.Unlock()
	ended := endedRecordingPhases(t.active, curr)
	t.active = curr
	return ended
}

// Drain synchronously flushes cb and sends a drain report via pusher for
// each phase in ended (what Observe returned). It is network I/O -- the push
// client's flush, then the drain POST, each retried -- so callers run it with
// no apply lock held; concurrent drains wait for each other (drainMu).
// Best-effort: a drain failure is logged, never returned, so it never blocks
// rule application. Nil-safe.
func (t *cacheDrainTracker) Drain(ended []recordingPhaseKey) {
	if t == nil || len(ended) == 0 {
		return
	}
	t.drainMu.Lock()
	defer t.drainMu.Unlock()
	for _, key := range ended {
		resp := t.pusher.SendDrainReport(key.ExperimentID, key.PhaseID, t.cb)
		if !resp.Accepted {
			t.logger.Warn("cache drain report not accepted",
				"experiment_id", key.ExperimentID, "phase_id", key.PhaseID)
		}
	}
}
