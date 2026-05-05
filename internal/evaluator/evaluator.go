package evaluator

import (
	"context"
	"fmt"

	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// InjectionPoint identifies where a fault check occurs.
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

// Request carries context for the evaluator decision.
type Request struct {
	Point  InjectionPoint
	Labels map[string]string
}

// Mode indicates how the fault runs relative to the request.
type Mode int

const (
	Background Mode = iota // independent of request lifecycle
	Inline                 // blocks until fault completes
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

// StartPolicy controls how the fault registry deduplicates service-scoped faults.
type StartPolicy int

const (
	DeduplicateByRule StartPolicy = iota
	DeduplicateByType             // reserved, not yet implemented
	AlwaysStart
)

func (p StartPolicy) String() string {
	switch p {
	case DeduplicateByRule:
		return "deduplicate_by_rule"
	case DeduplicateByType:
		return "deduplicate_by_type"
	case AlwaysStart:
		return "always_start"
	default:
		return fmt.Sprintf("start_policy(%d)", int(p))
	}
}

// CacheBoxAction identifies a cache-box operation to perform for a matched
// request. When non-zero, Decision.Fault must be nil: cache-box and fault
// injection are mutually exclusive actions on a single decision.
type CacheBoxAction int

const (
	// CacheBoxNone means "no cache-box action": fall through to the fault path
	// (or to the real downstream call if Fault is also nil).
	CacheBoxNone CacheBoxAction = iota
	// CacheBoxPassthrough forwards the request to the real downstream service
	// and records the response asynchronously for later replay.
	CacheBoxPassthrough
	// CacheBoxReplay serves the request from the cache store. On miss, the
	// dispatcher falls back to passthrough (and records the miss result).
	CacheBoxReplay
	// CacheBoxReplayDelay serves from the cache but sleeps for a duration
	// sampled from the entry's observed latency (or a fitted distribution) to
	// preserve the frozen service's intrinsic latency contribution.
	CacheBoxReplayDelay
)

// String returns a human-readable action name. The value for
// CacheBoxReplayDelay is "replay_with_delay" to match manteion's rule schema.
func (a CacheBoxAction) String() string {
	switch a {
	case CacheBoxNone:
		return "none"
	case CacheBoxPassthrough:
		return "passthrough"
	case CacheBoxReplay:
		return "replay"
	case CacheBoxReplayDelay:
		return "replay_with_delay"
	default:
		return fmt.Sprintf("cachebox(%d)", int(a))
	}
}

// Decision is what the evaluator returns when rules match. Nil = no action.
//
// Invariant: if CacheBox != CacheBoxNone, Fault must be nil. Evaluator
// implementations must enforce this. The interceptor dispatches cache-box
// decisions separately from fault decisions.
type Decision struct {
	Name        string
	Fault       fault.Fault
	Reason      string
	Mode        Mode
	CacheBox    CacheBoxAction
	StartPolicy StartPolicy
}

// Evaluator is the rule engine contract. Must be safe for concurrent use.
type Evaluator interface {
	Evaluate(ctx context.Context, req Request) *Decision
}
