// types.go
package atropos

import (
	"atropos-go/internal/evaluator"
	"atropos-go/internal/fault"
	"atropos-go/internal/interceptor"
	"atropos-go/internal/trace"
)

// --- Fault types ---

// Fault is the interface all fault types implement.
type Fault = fault.Fault

// FaultConfig holds duration and ramp parameters common to all faults.
type FaultConfig = fault.FaultConfig

// Handle provides non-blocking control over a running fault.
type Handle = fault.Handle

// Result reports what happened during a fault.
type Result = fault.Result

// EventEmitter records a timestamped event on a span.
type EventEmitter = fault.EventEmitter

// EventAware faults emit span events via an injected emitter.
type EventAware = fault.EventAware

// --- Evaluator types ---

// Evaluator is the rule engine contract. Must be safe for concurrent use.
type Evaluator = evaluator.Evaluator

// InjectionPoint identifies where a fault check occurs.
type InjectionPoint = evaluator.InjectionPoint

// Request carries context for the evaluator decision.
type Request = evaluator.Request

// Decision is what the evaluator returns when rules match.
type Decision = evaluator.Decision

// Mode indicates how the fault runs relative to the request.
type Mode = evaluator.Mode

// Re-export InjectionPoint constants.
const (
	Ingress   = evaluator.Ingress
	Egress    = evaluator.Egress
	Transient = evaluator.Transient
	Custom    = evaluator.Custom
)

// Re-export Mode constants.
const (
	Background = evaluator.Background
	Inline     = evaluator.Inline
)

// --- Trace types ---

// TraceSpan records attributes, events, and lifecycle signals on a trace span.
type TraceSpan = trace.Span

// --- Interceptor types ---

// Interceptor ties the evaluator, fault execution, and OTel together.
type Interceptor = interceptor.Interceptor

// CheckResult holds the outcome of an injection point check.
type CheckResult = interceptor.CheckResult
