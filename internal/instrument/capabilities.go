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
// category is outside the host's capabilities, or the host lacks something
// the fault needs within a category it supports -- cgroup authority for a
// resource quota (FaultType "<type>/quota"), an inline toxic host for a
// network host=inline fault ("<type>/inline"), a NetworkResolver for a
// network host=proxy fault ("<type>/proxy") -- or the rule is a composition
// (Category is then CapComposition and FaultType the execution mode).
// Rule decoding skips such rules per rule (counted, logged) instead of
// failing the whole sync (design §5.3, D7); a single admin slot reports the
// error as a 400.
type UnsupportedFaultError struct {
	Category  string
	FaultType string
}

func (e *UnsupportedFaultError) Error() string {
	return fmt.Sprintf("fault %s/%s is not supported by this host", e.Category, e.FaultType)
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
