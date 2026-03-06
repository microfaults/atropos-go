package policy

import (
	"fmt"
	"time"

	"atropos-go/loadgen/internal/attacker"
)

// Rule defines a hypothesis test: "when Condition is true, execute Action."
type Rule struct {
	ID       string        `json:"id"`
	Name     string        `json:"name"`
	Condition Condition    `json:"condition"`
	Action   ActionSpec    `json:"action"`
	Cooldown time.Duration `json:"cooldown"`
	Enabled  bool          `json:"enabled"`
}

// Validate checks that the rule is well-formed.
func (r *Rule) Validate() error {
	if r.ID == "" {
		return fmt.Errorf("policy: rule id is required")
	}
	if r.Name == "" {
		return fmt.Errorf("policy: rule name is required")
	}
	if err := r.Condition.Validate(); err != nil {
		return err
	}
	if r.Cooldown <= 0 {
		return fmt.Errorf("policy: cooldown must be > 0, got %s", r.Cooldown)
	}
	return nil
}

// Condition defines a threshold check against a named metric.
type Condition struct {
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
}

// Validate checks the condition fields.
func (c *Condition) Validate() error {
	if c.Metric == "" {
		return fmt.Errorf("policy: condition metric is required")
	}
	switch c.Operator {
	case "gt", "gte", "lt", "lte", "eq":
		// valid
	default:
		return fmt.Errorf("policy: invalid operator %q (expected gt, gte, lt, lte, eq)", c.Operator)
	}
	return nil
}

// Evaluate checks the condition against a value.
func (c *Condition) Evaluate(value float64) bool {
	switch c.Operator {
	case "gt":
		return value > c.Threshold
	case "gte":
		return value >= c.Threshold
	case "lt":
		return value < c.Threshold
	case "lte":
		return value <= c.Threshold
	case "eq":
		return value == c.Threshold
	default:
		return false
	}
}

// ActionSpec is an attack config template triggered when a rule fires.
type ActionSpec struct {
	Target      attacker.TargetSpec `json:"target"`
	Rate        int                 `json:"rate"`
	Duration    time.Duration       `json:"duration"`
	DedupBypass string              `json:"dedup_bypass,omitempty"`
}
