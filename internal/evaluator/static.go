package evaluator

import (
	"context"
	"sync"
)

// StaticRule is a single rule for StaticEvaluator. The first rule whose
// InjectionPoint matches the request and whose Labels are all present and
// equal on the request is selected; its Decision is returned.
//
// StaticEvaluator is intended for tests and small-scale deployments where a
// compiled rule engine is overkill. For production use manteion pushes rules
// to a proper rule evaluator (not yet implemented).
type StaticRule struct {
	Name     string
	Point    InjectionPoint
	Labels   map[string]string // all must match (AND semantics); empty = wildcard
	Decision Decision          // returned on match (copied, not aliased)
}

// StaticEvaluator holds a fixed list of StaticRules and returns the first
// match. Safe for concurrent use. Rules can be swapped atomically via
// SetRules without blocking concurrent Evaluate calls for longer than a
// mutex acquisition.
type StaticEvaluator struct {
	mu    sync.RWMutex
	rules []StaticRule
}

// NewStaticEvaluator builds an evaluator with a fixed rule list.
func NewStaticEvaluator(rules ...StaticRule) *StaticEvaluator {
	// Copy the input slice so callers can mutate their local copy without
	// affecting the evaluator's view of the rules.
	cp := make([]StaticRule, len(rules))
	copy(cp, rules)
	return &StaticEvaluator{rules: cp}
}

// Evaluate returns the first matching rule's decision, or nil if no rule
// matches. The returned *Decision is a copy; callers may mutate it freely.
func (e *StaticEvaluator) Evaluate(_ context.Context, req Request) *Decision {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for i := range e.rules {
		r := &e.rules[i]
		if r.Point != req.Point {
			continue
		}
		if !labelsMatch(r.Labels, req.Labels) {
			continue
		}
		d := r.Decision
		return &d
	}
	return nil
}

// SetRules atomically replaces the rule list.
func (e *StaticEvaluator) SetRules(rules []StaticRule) {
	cp := make([]StaticRule, len(rules))
	copy(cp, rules)
	e.mu.Lock()
	e.rules = cp
	e.mu.Unlock()
}

// Rules returns a copy of the current rule list.
func (e *StaticEvaluator) Rules() []StaticRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]StaticRule, len(e.rules))
	copy(cp, e.rules)
	return cp
}

// labelsMatch checks that every key in want is present in have with the same
// value. Empty want matches any have (wildcard).
func labelsMatch(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}
