// types.go
package atropos

import (
	"atropos-go/internal/cachebox"
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

// StartPolicy controls how the fault registry deduplicates service-scoped faults.
type StartPolicy = evaluator.StartPolicy

// Re-export StartPolicy constants.
const (
	DeduplicateByRule = evaluator.DeduplicateByRule
	DeduplicateByType = evaluator.DeduplicateByType
	AlwaysStart       = evaluator.AlwaysStart
)

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies.
type NetworkResolver func(target string) (listen, upstream string, err error)

// CacheBoxAction identifies a cache-box operation the evaluator has chosen.
type CacheBoxAction = evaluator.CacheBoxAction

// Re-export CacheBoxAction constants.
const (
	CacheBoxNone        = evaluator.CacheBoxNone
	CacheBoxPassthrough = evaluator.CacheBoxPassthrough
	CacheBoxReplay      = evaluator.CacheBoxReplay
	CacheBoxReplayDelay = evaluator.CacheBoxReplayDelay
)

// --- Static evaluator (small helper for tests and simple setups) ---

// StaticRule is a single match rule for StaticEvaluator.
type StaticRule = evaluator.StaticRule

// StaticEvaluator holds a fixed list of rules and returns the first match.
type StaticEvaluator = evaluator.StaticEvaluator

// NewStaticEvaluator builds a StaticEvaluator from a rule list.
func NewStaticEvaluator(rules ...StaticRule) *StaticEvaluator {
	return evaluator.NewStaticEvaluator(rules...)
}

// --- Cache-box types ---

// CacheBox is the runtime cache-box coordinator.
type CacheBox = cachebox.CacheBox

// CacheBoxConfig is the cache-box constructor config.
type CacheBoxConfig = cachebox.Config

// CacheBoxEntry is a single cached HTTP response.
type CacheBoxEntry = cachebox.Entry

// CacheBoxStore is the cache-box persistence contract.
type CacheBoxStore = cachebox.Store

// CacheBoxMemStoreConfig configures the in-memory cache-box store.
type CacheBoxMemStoreConfig = cachebox.MemStoreConfig

// CacheBoxDelaySource produces delays for replay_with_delay mode.
type CacheBoxDelaySource = cachebox.DelaySource

// CacheBoxStats is the combined store + recorder stats snapshot.
type CacheBoxStats = cachebox.Stats

// KeyStrategy names a built-in cache-box key derivation strategy.
type KeyStrategy = cachebox.KeyStrategy

// Re-export cache-box key strategy constants.
const (
	KeyStrategyExact         = cachebox.KeyStrategyExact
	KeyStrategyExactWithHost = cachebox.KeyStrategyExactWithHost
	KeyStrategyExactWithBody = cachebox.KeyStrategyExactWithBody
)

// NewCacheBox builds a CacheBox coordinator from a config. Safe defaults
// are applied for unset fields. See cachebox.Config for details.
func NewCacheBox(cfg CacheBoxConfig) *CacheBox {
	return cachebox.New(cfg)
}

// NewCacheBoxMemStore builds the default in-memory LRU store. A zero
// maxEntries means unbounded -- use with caution in production.
func NewCacheBoxMemStore(maxEntries int) CacheBoxStore {
	return cachebox.NewMemStore(cachebox.MemStoreConfig{MaxEntries: maxEntries})
}

// --- Trace types ---

// TraceSpan records attributes, events, and lifecycle signals on a trace span.
type TraceSpan = trace.Span

// --- Interceptor types ---

// Interceptor ties the evaluator, fault execution, and OTel together.
type Interceptor = interceptor.Interceptor

// CheckResult holds the outcome of an injection point check.
type CheckResult = interceptor.CheckResult
