package cachebox

// CacheBoxContext (wire spec §W1) is the authoritative-end-to-end bundle
// that scopes a cache-box operation to one experiment phase and pins the
// key strategy used to derive/verify cache keys. It is attached to:
//   - the cache-box portion of a compiled rule (CompiledCacheBox.Context)
//   - the freeze command body (DelayRequest.Context)
//   - a preload begin request (PreloadBeginRequest, flattened)
//
// It lives in this package (rather than the root atropos package, where
// the other wire types live) so internal/evaluator and internal/interceptor
// can reference it -- via evaluator.Decision.CacheBoxContext -- without an
// import cycle through the root package. atropos.CacheBoxContext is a
// transparent type alias for this type, so the wire shape manteion imports
// is unaffected.
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
