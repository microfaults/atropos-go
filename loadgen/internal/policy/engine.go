package policy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/microfaults/atropos-go/loadgen/internal/attacker"
)

// MetricSource provides values for condition evaluation.
// Extensible: backed by workload.Registry initially, Prometheus later.
type MetricSource interface {
	GetMetric(name string) (float64, bool)
}

// Engine evaluates rules on a periodic tick and triggers attacks when
// conditions are met. It implements a two-phase evaluate to avoid
// lock upgrade races: read rules under RLock, batch-write cooldowns under Lock.
type Engine struct {
	mu        sync.RWMutex
	rules     map[string]*Rule
	cooldowns map[string]time.Time
	source    MetricSource
	attacker  *attacker.Manager
	interval  time.Duration
}

// NewEngine creates a policy engine that evaluates rules every interval.
func NewEngine(source MetricSource, atk *attacker.Manager, interval time.Duration) *Engine {
	return &Engine{
		rules:     make(map[string]*Rule),
		cooldowns: make(map[string]time.Time),
		source:    source,
		attacker:  atk,
		interval:  interval,
	}
}

// AddRule registers a policy rule. Returns error if invalid or duplicate ID.
func (e *Engine) AddRule(rule *Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.rules[rule.ID]; exists {
		return fmt.Errorf("policy: rule %q already exists", rule.ID)
	}
	e.rules[rule.ID] = rule
	return nil
}

// RemoveRule removes a rule and its cooldown entry.
func (e *Engine) RemoveRule(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.rules[id]; !exists {
		return fmt.Errorf("policy: rule %q not found", id)
	}
	delete(e.rules, id)
	delete(e.cooldowns, id)
	return nil
}

// ListRules returns all registered rules.
func (e *Engine) ListRules() []*Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Rule, 0, len(e.rules))
	for _, r := range e.rules {
		out = append(out, r)
	}
	return out
}

// Run starts the evaluation loop. Blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.evaluate(ctx)
		}
	}
}

func (e *Engine) evaluate(ctx context.Context) {
	// Phase 1: read rules and collect triggered IDs.
	e.mu.RLock()
	type trigger struct {
		ruleID string
		rule   *Rule
	}
	var triggered []trigger

	now := time.Now()
	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}
		if last, ok := e.cooldowns[rule.ID]; ok {
			if now.Sub(last) < rule.Cooldown {
				continue
			}
		}
		value, ok := e.source.GetMetric(rule.Condition.Metric)
		if !ok {
			continue
		}
		if rule.Condition.Evaluate(value) {
			triggered = append(triggered, trigger{ruleID: rule.ID, rule: rule})
		}
	}
	e.mu.RUnlock()

	if len(triggered) == 0 {
		return
	}

	// Phase 2: launch attacks and batch-update cooldowns.
	firedIDs := make([]string, 0, len(triggered))
	for _, t := range triggered {
		cfg := attacker.AttackConfig{
			ID:          fmt.Sprintf("policy-%s-%s", t.ruleID, shortID()),
			Target:      t.rule.Action.Target,
			Rate:        t.rule.Action.Rate,
			Duration:    t.rule.Action.Duration,
			DedupBypass: t.rule.Action.DedupBypass,
		}
		if _, err := e.attacker.Launch(ctx, cfg); err != nil {
			log.Printf("policy: rule %q trigger failed: %v", t.rule.Name, err)
			continue
		}
		firedIDs = append(firedIDs, t.ruleID)
		log.Printf("policy: rule %q triggered attack %s", t.rule.Name, cfg.ID)
	}

	if len(firedIDs) > 0 {
		e.mu.Lock()
		ts := time.Now()
		for _, id := range firedIDs {
			e.cooldowns[id] = ts
		}
		e.mu.Unlock()
	}
}

func shortID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
