package wire

import "encoding/json"

// FaultRequest is the platform's single fault wire shape, shared by the
// admin endpoint (POST /admin/fault), manteion's active_faults set, and —
// via the CompiledFault alias — the fault carried inside a CompiledRule.
//
// The category determines which sibling field is populated:
//   - "inline"   → Params holds the type-specific params; no envelope.
//   - "network"  → Network envelope holds host/target/direction/scope;
//     Params holds the toxic-specific params.
//   - "resource" → Params holds the type-specific params; no envelope.
//
// Network is forbidden for non-network categories and required for network.
// Params shapes are the exported faultparams.* structs (one per
// (category, fault_type) pair); absent params decode as all-defaults.
type FaultRequest struct {
	ID         string `json:"id,omitempty"` // slot identity (service+category:type); unused on compiled rules
	Category   string `json:"category"`     // "inline" (default), "network", "resource"
	FaultType  string `json:"fault_type"`   // fault type within category
	DurationMs int64  `json:"duration_ms,omitempty"`
	RampUpMs   int64  `json:"ramp_up_ms,omitempty"`
	RampDownMs int64  `json:"ramp_down_ms,omitempty"`

	Network *NetworkEnvelope `json:"network,omitempty"`
	Params  json.RawMessage  `json:"params,omitempty"`
} //@name atropos.FaultRequest

// EffectiveCategory returns Category, defaulting an empty value to "inline".
func (r *FaultRequest) EffectiveCategory() string {
	if r.Category == "" {
		return "inline"
	}
	return r.Category
}

// FaultStatus is the JSON response for GET /admin/fault.
type FaultStatus struct {
	Active bool            `json:"active"`
	Faults []*FaultRequest `json:"faults,omitempty"` // matches new plan shape {"faults": [...]}
} //@name atropos.FaultStatus

// NetworkEnvelope is the network-category-only envelope. It separates the
// "where does the toxic live and to which traffic does it apply" question
// from the toxic-specific params (delay, rate, etc.).
type NetworkEnvelope struct {
	// Host selects where the toxic runs.
	//   "proxy"  → TCP proxy (requires a NetworkResolver; the in-process default)
	//   "inline" → armed policy on the atroposd sidecar's proxied upstream calls
	// The in-process host has no proxied connection to shape: it skips a rule
	// with Host=="inline" (reported as unsupported) and rejects a single
	// /admin/fault slot with 400. An empty Host means "inline" on a host that
	// owns the proxied connections (the sidecar) and "proxy" in-process.
	Host string `json:"host"`

	// Target is the logical name of the upstream service. Required for both
	// hosts. With Host=="proxy" a NetworkResolver turns it into a
	// (listen, upstream) address pair; with Host=="inline" it is the
	// upstream's host or host:port, matched against the outbound request's
	// host (a target without a port matches any port).
	Target string `json:"target,omitempty"`

	// Direction selects which half of a proxied connection to apply the
	// toxic to: "upstream" (client → server, request bytes) or "downstream"
	// (server → client, response bytes). It defaults to "upstream" with
	// Host=="proxy" and to "downstream" with Host=="inline".
	Direction string `json:"direction,omitempty"`

	// Scope is the fraction of connections (proxy) or requests (inline)
	// to which the toxic applies. Zero means 1.0 (all).
	Scope float64 `json:"scope,omitempty"`
} //@name atropos.NetworkEnvelope
