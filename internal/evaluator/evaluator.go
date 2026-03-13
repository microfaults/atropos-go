package evaluator

import (
	"context"
	"fmt"

	fault "github.com/microfaults/atropos-go/internal/fault"
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
	Point   InjectionPoint
	Labels  map[string]string
	Payload any // optional parsed request body
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

// Decision is what the evaluator returns when rules match. Nil = no fault.
type Decision struct {
	Fault  fault.Fault
	Reason string
	Mode   Mode
}

// Evaluator is the rule engine contract. Must be safe for concurrent use.
type Evaluator interface {
	Evaluate(ctx context.Context, req Request) *Decision
}
