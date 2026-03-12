package fault

import "go.opentelemetry.io/otel/attribute"

// EventEmitter records a timestamped event on a span.
type EventEmitter func(name string, attrs ...attribute.KeyValue)

// EventAware faults emit span events via an injected emitter.
// The interceptor wires this up before Fault.Start().
type EventAware interface {
	SetEventEmitter(fn EventEmitter)
}
