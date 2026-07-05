package atropos

// RuleSync is the desired-state payload manteion delivers to SDKs — the body
// of a 200 poll response (GET /api/v1/sdk/rules) and, embedded in
// RegisterResponse, the intent piggybacked on registration. Both ends
// marshal/unmarshal this one struct, so the field set cannot drift between
// control plane and SDK.
//
// Rules and ActiveFaults are deliberately NOT omitempty: under Apply's
// reconciliation an empty array means "clear everything in this category",
// which must be explicit on the wire rather than indistinguishable from
// "field absent". A null/absent rules field (nil after decode) means "no
// change" — the compatibility escape hatch for payloads that don't carry
// rules — while [] is authoritative desired state. Manteion's poll
// endpoint always sends the full (possibly empty) compiled set.
type RuleSync struct {
	Version      uint64         `json:"version"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
	// RecordingPhaseID is the experiment phase the SDK should record cache-box
	// entries INTO (the currently-running baseline phase with persist_cache).
	// Empty when no recording phase is active. Apply hands it to
	// ApplyTargets.PhaseIDSink so the cache-push client tags ingests with the
	// right phase; a recording phase start/stop bumps the rule version so this
	// rides the normal poll fast-path.
	RecordingPhaseID string `json:"recording_phase_id,omitempty"`
}
