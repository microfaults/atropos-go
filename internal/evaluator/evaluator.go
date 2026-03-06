package evaluator

import (
	"context"
	"fmt"

	fault "atropos-go/internal/fault"
)

// InjectionPoint identifies where in the request lifecycle a fault check occurs.
type InjectionPoint int

const (
	Ingress   InjectionPoint = iota // inbound request hitting the service
	Egress                          // outbound call to a dependency
	Transient                       // after request completion (side effects)
	Custom                          // developer-annotated code block
)

func (p InjectionPoint) String() string {
	switch p {
	case Ingress:
		return "ingress"
	case Egress:
		return "egress"
	case Transient:
		return "transient"
	case Custom:
		return "custom"
	default:
		return fmt.Sprintf("point(%d)", int(p))
	}
}

// Request carries context for the evaluator to decide whether to inject a fault.
type Request struct {
	// Point is where this evaluation is happening.
	Point InjectionPoint

	// Labels are arbitrary key-value pairs from the injection site.
	// For HTTP: method, path, headers. For custom: developer-supplied tags.
	Labels map[string]string

	// Payload is the optional parsed request body (e.g., JSON tree).
	// nil if not applicable or not yet parsed.
	Payload any
}

// Mode indicates how the fault should be applied relative to the request.
type Mode int

const (
	// Background means the fault runs independently of the request lifecycle.
	// Used for resource faults (CPU stress) or long-running proxies.
	Background Mode = iota

	// Inline means the fault blocks the request until it completes.
	// Used for latency injection, error injection, blackhole.
	Inline
)

func (m Mode) String() string {
	switch m {
	case Background:
		return "background"
	case Inline:
		return "inline"
	default:
		return fmt.Sprintf("mode(%d)", int(m))
	}
}

// Decision is what the evaluator returns when rules match.
// A nil *Decision from Evaluate means "no fault, pass through normally."
type Decision struct {
	// Fault is the configured fault to inject. Ready to Validate() and Start().
	Fault fault.Fault

	// Reason is a human-readable explanation of why this fault was selected.
	// Recorded in the OTel span for debuggability.
	Reason string

	// Mode controls whether the fault blocks the request (Inline)
	// or runs independently (Background).
	Mode Mode
}

// Evaluator is the rule engine contract. Implementations parse rules,
// match against the current request context, and return a fault to inject.
//
// Evaluate must be safe for concurrent use.
// Returning nil means "no fault, pass through normally."
type Evaluator interface {
	Evaluate(ctx context.Context, req Request) *Decision
}
