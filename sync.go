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
// Recording/replay provenance rides each matched rule's CacheBoxContext
// (design doc Q5/INV-5); there is no ambient recording-phase signal here.
// The former recording_phase_id field (and its ApplyTargets.PhaseIDSink
// consumer) was a second, registration-time delivery path for the same
// fact -- manteion stopped writing it, the SDK delivered "" every poll,
// and two owners for one signal is one too many.
type RuleSync struct {
	Version      uint64         `json:"version"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
}
