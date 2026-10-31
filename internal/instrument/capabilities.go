package instrument

import "fmt"

// Fault categories a host can advertise at registration and enforce at
// decode time.
const (
	CapInline   = "inline"
	CapNetwork  = "network"
	CapResource = "resource"
	CapCacheBox = "cachebox"

	// CapComposition is the atropos_rules_unsupported_total{category} label
	// for composition rules. No host advertises it yet: the decoder skips
	// compositions per rule under this label until manteion expands them
	// into phases (design review §5). Advertising it would change nothing
	// until a composition evaluator exists -- the skip does not consult the
	// capability set.
	CapComposition = "composition"
)

// Host names.
const (
	HostInProcess = "in-process"
	HostSidecar   = "sidecar"
)

// AllCapabilities is what the in-process host advertises: it executes
// every category (network through host=proxy with a resolver).
func AllCapabilities() []string {
	return []string{CapInline, CapNetwork, CapResource, CapCacheBox}
}

// UnsupportedFaultError says this host cannot execute a rule: its fault's
// category is outside the host's capabilities (or needs authority the host
// lacks, such as a resource quota in-process), or the rule is a composition
// (Category is then CapComposition and FaultType the execution mode).
// Rule decoding skips such rules per rule (counted, logged) instead of
// failing the whole sync (design §5.3, D7).
type UnsupportedFaultError struct {
	Category  string
	FaultType string
}

func (e *UnsupportedFaultError) Error() string {
	return fmt.Sprintf("fault %s/%s: category %q is not supported by this host", e.Category, e.FaultType, e.Category)
}

// WithCapabilities restricts decoding to the given fault categories; a
// fault outside the set decodes to an UnsupportedFaultError. No option
// (nil set) means every category is supported.
func WithCapabilities(caps ...string) DecodeOption {
	set := make(map[string]bool, len(caps))
	for _, c := range caps {
		set[c] = true
	}
	return func(c *decodeConfig) { c.capabilities = set }
}
