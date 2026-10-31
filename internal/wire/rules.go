package wire

import "git.ucsc.edu/microfaults/atropos-go/internal/evaluator"

// CompiledRule is the wire format received from manteion's poll/register
// endpoints. Manteion resolves FaultSpec references and inlines config;
// the SDK decodes and constructs evaluator rules locally.
//
// Exactly one of Fault, Composition, or CacheBox must be set.
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	StartPolicy    string               `json:"start_policy,omitempty"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
	CacheBox       *CompiledCacheBox    `json:"cachebox,omitempty"`
} //@name atropos.CompiledRule

// CompiledCacheBox is a resolved cache-box action for a rule.
//
// KeyStrategy is the legacy, construction-time-only strategy hint and is
// kept for backward compatibility. Context, when present, is authoritative
// end-to-end (design doc Q3): its own KeyStrategy/StrategyVersion/KeyHeaders
// take precedence over the sibling KeyStrategy field above.
type CompiledCacheBox struct {
	Mode        string           `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
	KeyStrategy string           `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
	Context     *CacheBoxContext `json:"context,omitempty"`
} //@name atropos.CompiledCacheBox

// CompiledFault is a resolved fault spec on the wire. It is the same struct
// as FaultRequest — the platform's single fault wire shape — with ID simply
// unused in the compiled-rule context (manteion inlines specs, so
// rule-attached faults need no slot identity).
type CompiledFault = FaultRequest //@name atropos.CompiledFault

// CompiledComposition is a resolved FaultComposition tree with all specs
// inlined. Composition execution is not yet supported by the SDK evaluator
// (deferred to v6); the decoder skips a rule that references one (logged at
// WARN, counted under atropos_rules_unsupported_total{category="composition"})
// instead of failing the set.
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
} //@name atropos.CompiledComposition

// CompiledCompositionMember is a resolved member — either a leaf fault or a
// nested composition.
type CompiledCompositionMember struct {
	Direction   string               `json:"direction,omitempty"`
	Fault       *CompiledFault       `json:"fault,omitempty"`
	Composition *CompiledComposition `json:"composition,omitempty"`
} //@name atropos.CompiledCompositionMember

// StaticRule is the JSON shape GET /admin/rules serves (one element per
// installed rule). Manteion's SDK client decodes into it.
type StaticRule = evaluator.StaticRule //@name atropos.StaticRule
