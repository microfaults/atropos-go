package atropos

// RuleSync is the desired-state payload manteion delivers to SDKs — the body
// of a 200 poll response (GET /api/v1/sdk/rules) and, embedded in
// RegisterResponse, the intent piggybacked on registration. Both ends
// marshal/unmarshal this one struct, so the field set cannot drift between
// control plane and SDK.
//
// Rules and ActiveFaults are deliberately NOT omitempty: under Apply's
// reconciliation an empty active_faults array means "clear all manual
// faults", which must be explicit on the wire rather than indistinguishable
// from "field absent". (An empty rules list is still treated as "no change"
// by Apply — see its doc.)
type RuleSync struct {
	Version      uint64         `json:"version"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
}
