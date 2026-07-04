package cachebox

import "sync/atomic"

// PhaseKey builds the canonical diagnostic key for a (experiment_id,
// phase_id) pair, used as ReplaySet's Install/PhaseKey argument. A NUL
// separator (rather than e.g. ":") keeps two different (exp, phase) pairs
// from ever colliding into the same key regardless of what characters the
// ids themselves contain.
func PhaseKey(experimentID, phaseID string) string {
	return experimentID + "\x00" + phaseID
}

// ReplaySet is the immutable set of entries a frozen service replays from.
// It is populated only by Install -- the preload-commit path (ATRO-6) -- and
// consulted only by CacheBox.Lookup under a replay-family decision. There is
// no eviction path: an entry leaves only via a subsequent Install (atomic
// replace, never merge) or Clear. This is the store half of the record/
// replay split (design doc Q4, INV-4) -- Record (see RecordBuffer) never
// writes here.
type ReplaySet struct {
	ptr atomic.Pointer[replaySetData]
}

type replaySetData struct {
	phaseKey string
	entries  map[string]*Entry
}

// NewReplaySet returns an empty ReplaySet.
func NewReplaySet() *ReplaySet {
	return &ReplaySet{}
}

// Get returns the entry for key from the currently-installed set, if any.
// A nil ReplaySet (or one with nothing installed) always misses.
func (rs *ReplaySet) Get(key string) (*Entry, bool) {
	if rs == nil {
		return nil, false
	}
	data := rs.ptr.Load()
	if data == nil {
		return nil, false
	}
	e, ok := data.entries[key]
	return e, ok
}

// Install atomically replaces the entire installed set with entries, tagged
// with phaseKey (the owning (experiment_id, phase_id) pair, for
// diagnostics) -- never merged with whatever was previously installed. The
// caller transfers ownership of entries; it must not mutate the map after
// calling Install.
func (rs *ReplaySet) Install(phaseKey string, entries map[string]*Entry) {
	if rs == nil {
		return
	}
	if entries == nil {
		entries = map[string]*Entry{}
	}
	rs.ptr.Store(&replaySetData{phaseKey: phaseKey, entries: entries})
}

// Clear removes the installed set entirely (e.g. freeze-clear/thaw hygiene,
// ATRO-6). Subsequent Get calls miss until the next Install.
func (rs *ReplaySet) Clear() {
	if rs == nil {
		return
	}
	rs.ptr.Store(nil)
}

// Len reports the number of entries in the currently-installed set (0 if
// none has ever been installed).
func (rs *ReplaySet) Len() int {
	if rs == nil {
		return 0
	}
	data := rs.ptr.Load()
	if data == nil {
		return 0
	}
	return len(data.entries)
}

// PhaseKey reports the diagnostic key passed to the most recent Install, or
// "" if nothing is currently installed.
func (rs *ReplaySet) PhaseKey() string {
	if rs == nil {
		return ""
	}
	data := rs.ptr.Load()
	if data == nil {
		return ""
	}
	return data.phaseKey
}
