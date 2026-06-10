package faultparams

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Spec describes one supported (category, fault_type) pair: the catalogue
// entry control planes enumerate and the constructor for its Param struct.
type Spec struct {
	Category    string // "inline" | "network" | "resource"
	Type        string // fault_type within the category
	Description string // one-line human description (UI tooltips, docs)
	New         func() Param
}

// specs is the authoritative catalogue. Order: inline, network, resource;
// alphabetical within each category.
var specs = []Spec{
	{"inline", "error", "Short-circuit the request with an HTTP error response", func() Param { return &InlineError{} }},
	{"inline", "hang", "Block the request until the duration elapses (HTTP-level blackhole)", func() Param { return &InlineHang{} }},
	{"inline", "latency", "Delay request handling in-process by delay + rand(jitter)", func() Param { return &InlineLatency{} }},

	{"network", "blackhole", "Consume client bytes and never respond", func() Param { return &NetworkBlackhole{} }},
	{"network", "drip", "Trickle the stream out in tiny chunks", func() Param { return &NetworkDrip{} }},
	{"network", "latency", "Delay piped bytes by delay + rand(jitter)", func() Param { return &NetworkLatency{} }},
	{"network", "retransmit_delay", "Stall a fraction of reads to mimic TCP retransmissions", func() Param { return &NetworkRetransmitDelay{} }},
	{"network", "rst", "Force-close the connection with a TCP RST", func() Param { return &NetworkRST{} }},
	{"network", "throttle", "Cap stream bandwidth with token pacing", func() Param { return &NetworkThrottle{} }},

	{"resource", "cpu", "Burn CPU toward a target load with a duty-cycle window", func() Param { return &ResourceCPU{} }},
	{"resource", "disk", "Fill disk at a sustained write rate up to a usage cap", func() Param { return &ResourceDisk{} }},
	{"resource", "io", "Generate filesystem read/write pressure across a file set", func() Param { return &ResourceIO{} }},
	{"resource", "memory", "Allocate (and optionally thrash) memory toward a target fraction", func() Param { return &ResourceMemory{} }},
}

// All returns the full catalogue in stable order. The returned slice is a
// copy; callers may reorder freely.
func All() []Spec {
	out := make([]Spec, len(specs))
	copy(out, specs)
	return out
}

// Lookup returns the Spec for (category, faultType), if supported.
func Lookup(category, faultType string) (Spec, bool) {
	for _, s := range specs {
		if s.Category == category && s.Type == faultType {
			return s, true
		}
	}
	return Spec{}, false
}

// ValidateRaw strictly decodes raw params for (category, faultType) and runs
// the Param's Validate. Unknown fields are rejected — this is the control-
// plane gate; the SDK's own decoders remain lenient. nil/empty raw is treated
// as an empty object (all defaults), which still must pass Validate.
func ValidateRaw(category, faultType string, raw json.RawMessage) error {
	spec, ok := Lookup(category, faultType)
	if !ok {
		return fmt.Errorf("unknown fault type %s/%s", category, faultType)
	}
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	p := spec.New()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(p); err != nil {
		return fmt.Errorf("decode %s/%s params: %w", category, faultType, err)
	}
	return p.Validate()
}
