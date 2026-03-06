package rule_engine

import (
	"fmt"
	"strings"
)

// ─── Node types ─────────────────────────────────────────────────────

// NodeType distinguishes condition nodes from fault (leaf) nodes.
type NodeType int

const (
	// NodeCondition evaluates a Rego policy against request input.
	NodeCondition NodeType = iota
	// NodeFault is a leaf node carrying a complete fault specification.
	NodeFault
)

// ─── FaultSpec ──────────────────────────────────────────────────────

// FaultSpec is the complete fault configuration.
// Condition nodes can embed this in their Rego policy as "fault_spec",
// and fault (leaf) nodes carry it directly.
//
// The fields map onto existing types:
//   - DurationMs/RampUpMs/RampDownMs → fault.FaultConfig
//   - TargetLoad/WindowMs            → resource.Config
//   - ReadRate/FileSize/FileCount/Workers/IOMode → io.Config
type FaultSpec struct {
	// What kind of fault to inject.
	FaultType string `json:"fault_type"` // "cpu", "io", "latency", "packet_drop"

	// Where in the request lifecycle to inject.
	InjectionPoint string `json:"injection_point"` // "inbound", "outbound", "transient", "custom"

	// For outbound faults: which downstream service to target.
	TargetService string `json:"target_service,omitempty"`

	// ── Base fault config (fault.FaultConfig) ──

	DurationMs int64 `json:"duration_ms"`
	RampUpMs   int64 `json:"ramp_up_ms,omitempty"`
	RampDownMs int64 `json:"ramp_down_ms,omitempty"`

	// ── Resource fault config (resource.Config) ──

	// TargetLoad is the fraction of the resource to consume (0.0, 1.0].
	TargetLoad float64 `json:"target_load,omitempty"`
	// WindowMs is the duty-cycle period in milliseconds.
	WindowMs int64 `json:"window_ms,omitempty"`
}

// ─── Rule definition (developer-facing format) ──────────────────────

// RuleDef is the declarative format a developer uses to define a fault-
// injection rule.  One or more RuleDefs are fed into BuildGraph at
// startup to produce the execution graph.
//
// A rule is a small sub-graph: one or more condition nodes, one or more
// fault (leaf) nodes, and edges wiring them together.
//
// Example JSON:
//
//	{
//	  "id":          "checkout_high_value",
//	  "name":        "High-value checkout orders",
//	  "description": "Inject CPU faults on orders over $100",
//	  "conditions": [
//	    {
//	      "id":   "checkout_svc",
//	      "name": "Checkout Service",
//	      "rego": "package checkout_svc\nimport rego.v1\ndefault match := false\nmatch if { input.service == \"checkout\" }"
//	    },
//	    {
//	      "id":   "high_value",
//	      "name": "High-Value Order",
//	      "rego": "package high_value\nimport rego.v1\ndefault match := false\nmatch if { input.total_amount.units > 100 }"
//	    }
//	  ],
//	  "faults": [
//	    {
//	      "id":   "cpu_fault",
//	      "name": "CPU Spike",
//	      "spec": {
//	        "fault_type":      "cpu",
//	        "injection_point": "inbound",
//	        "duration_ms":     10000,
//	        "target_load":     0.8,
//	        "window_ms":       100
//	      }
//	    }
//	  ],
//	  "edges": [
//	    ["checkout_svc", "high_value"],
//	    ["high_value",   "cpu_fault"]
//	  ]
//	}
type RuleDef struct {
	// ID uniquely identifies this rule definition.
	ID string `json:"id"`
	// Name is a human-readable label.
	Name string `json:"name"`
	// Description explains what this rule does.
	Description string `json:"description,omitempty"`

	// Conditions lists the condition nodes in this rule.
	Conditions []ConditionDef `json:"conditions"`
	// Faults lists the fault (leaf) nodes in this rule.
	Faults []FaultDef `json:"faults"`
	// Edges wires the sub-graph together as [from, to] pairs.
	// Each element is a two-element array of node IDs.
	Edges [][2]string `json:"edges"`
}

// ConditionDef defines a single condition node inside a RuleDef.
type ConditionDef struct {
	// ID uniquely identifies this condition node.
	ID string `json:"id"`
	// Name is a human-readable label.
	Name string `json:"name"`
	// Description optionally explains the condition.
	Description string `json:"description,omitempty"`

	// Option A: structured — for dashboards / non-technical users
	Match []MatchRule `json:"match,omitempty"`

	// Option B: Rego — for power users
	// Rego is the Rego policy source code.
	// Must define "package <name>" and a "match" rule.
	// May optionally define a "fault_spec" rule.
	Rego string `json:"rego,omitempty"`
}

type MatchRule struct {
	Field    string `json:"field"`    // JSON path: e.g. "request.headers.host", "user_id", "request.body.amount"
	Operator string `json:"operator"` // "==", "!=", ">", "<", ">=", "<=", "in", "contains"
	Value    any    `json:"value"`    // "xyz", 100, ["us-east", "us-west"]
}

// GenerateRego converts a slice of MatchRules into valid Rego source code.
// All expressions are ANDed together inside a single "match" rule.
//
// Example:
//
//	MatchRule{Field: "user_id", Operator: "==", Value: "xyz"}
//	MatchRule{Field: "total",   Operator: ">",  Value: 100}
//
// Produces:
//
//	package user_match
//	import rego.v1
//	default match := false
//	match if {
//	    input.user_id == "xyz"
//	    input.total > 100
//	}
func GenerateRego(pkgName string, rules []MatchRule) (string, error) {
	if len(rules) == 0 {
		return "", fmt.Errorf("generateRego: no match rules provided")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "package %s\n\n", pkgName)
	b.WriteString("import rego.v1\n\n")
	b.WriteString("default match := false\n\n")
	b.WriteString("match if {\n")

	for _, r := range rules {
		expr, err := matchRuleToRego(r)
		if err != nil {
			return "", fmt.Errorf("generateRego: field %q: %w", r.Field, err)
		}
		fmt.Fprintf(&b, "    %s\n", expr)
	}

	b.WriteString("}\n")
	return b.String(), nil
}

// matchRuleToRego converts a single MatchRule into a Rego expression string.
func matchRuleToRego(r MatchRule) (string, error) {
	inputField := "input." + r.Field
	val := formatRegoValue(r.Value)

	switch r.Operator {
	case "==", "!=", ">", "<", ">=", "<=":
		return fmt.Sprintf("%s %s %s", inputField, r.Operator, val), nil

	case "in":
		// "in" checks if the field value is a member of a set.
		// Rego: input.field in {"a", "b", "c"}
		set, err := formatRegoSet(r.Value)
		if err != nil {
			return "", fmt.Errorf("operator 'in' requires an array value: %w", err)
		}
		return fmt.Sprintf("%s in %s", inputField, set), nil

	case "contains":
		// "contains" checks if a string field contains a substring.
		// Rego: contains(input.field, "substr")
		return fmt.Sprintf("contains(%s, %s)", inputField, val), nil

	default:
		return "", fmt.Errorf("unsupported operator %q", r.Operator)
	}
}

// formatRegoValue formats a Go value as a Rego literal.
func formatRegoValue(v any) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("%q", val)
	case float64:
		// JSON numbers unmarshal as float64.
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatRegoSet converts a Go slice into a Rego set literal: {"a", "b", "c"}.
func formatRegoSet(v any) (string, error) {
	slice, ok := v.([]any)
	if !ok {
		return "", fmt.Errorf("expected []any, got %T", v)
	}
	elems := make([]string, len(slice))
	for i, elem := range slice {
		elems[i] = formatRegoValue(elem)
	}
	return "{" + strings.Join(elems, ", ") + "}", nil
}

// FaultDef defines a single fault (leaf) node inside a RuleDef.
type FaultDef struct {
	// ID uniquely identifies this fault node.
	ID string `json:"id"`
	// Name is a human-readable label.
	Name string `json:"name"`
	// Spec is the complete fault configuration for this leaf.
	Spec FaultSpec `json:"spec"`
}

// BuildGraph constructs a Graph and compiled policy map from a slice
// of RuleDefs.  It feeds every definition through a Builder and returns
// the validated, ready-to-evaluate graph.
//
// For each ConditionDef, if Rego is provided it is used directly.
// Otherwise, the Match rules are auto-compiled into Rego via GenerateRego.
//
//	rules := []RuleDef{ ... }
//	graph, policies, err := BuildGraph(rules, &OPACompiler{})
func BuildGraph(rules []RuleDef, compiler PolicyCompiler) (*Graph, map[string]CompiledPolicy, error) {
	b := NewBuilder(compiler)
	for _, r := range rules {
		for _, c := range r.Conditions {
			regoSrc, err := resolveRego(c)
			if err != nil {
				return nil, nil, fmt.Errorf("rule %q, condition %q: %w", r.ID, c.ID, err)
			}
			if c.Description != "" {
				b.AddConditionDesc(c.ID, c.Name, c.Description, regoSrc)
			} else {
				b.AddCondition(c.ID, c.Name, regoSrc)
			}
		}
		for _, f := range r.Faults {
			b.AddFault(f.ID, f.Name, f.Spec)
		}
		for _, e := range r.Edges {
			b.Link(e[0], e[1])
		}
	}
	return b.Build()
}

// resolveRego returns the Rego source for a ConditionDef.
// If Rego is set, it is returned as-is.
// Otherwise, GenerateRego compiles the Match rules into Rego.
func resolveRego(c ConditionDef) (string, error) {
	if c.Rego != "" {
		return c.Rego, nil
	}
	if len(c.Match) == 0 {
		return "", fmt.Errorf("condition must have either 'rego' or 'match' rules")
	}
	return GenerateRego(c.ID, c.Match)
}

// ─── Node ───────────────────────────────────────────────────────────

// Node is a single node in the rule graph.
type Node struct {
	ID          string
	Name        string // human-readable label
	Description string
	Type        NodeType

	// Policy is the Rego source code (only for NodeCondition).
	// Must define a rule named "match" that evaluates to true/false,
	// and optionally a "fault_spec" object rule.
	Policy string

	// FaultSpec is the complete fault config (only for NodeFault).
	FaultSpec *FaultSpec
}

// ─── Edge ───────────────────────────────────────────────────────────

// Edge connects two nodes in the graph (parent → child).
type Edge struct {
	From string // parent node ID
	To   string // child node ID
}

// ─── Evaluation result ──────────────────────────────────────────────

// EvalResult is produced for each fault leaf (or hybrid condition node)
// reached during graph evaluation.
type EvalResult struct {
	// MatchedPath is the ordered list of node IDs traversed to reach this fault.
	// Example: ["root", "checkout_svc", "high_value", "cpu_fault"]
	MatchedPath []string

	// Spec is the complete fault configuration to instantiate.
	Spec FaultSpec
}

// ─── Interfaces ─────────────────────────────────────────────────────

// Evaluator evaluates a request payload against a rule graph and returns
// all matched fault specifications.
type Evaluator interface {
	Evaluate(input map[string]any) ([]EvalResult, error)
}

// PolicyCompiler compiles Rego source into a reusable, executable policy.
type PolicyCompiler interface {
	Compile(policyID string, regoSource string) (CompiledPolicy, error)
}

// CompiledPolicy is a pre-compiled Rego policy ready for repeated evaluation.
type CompiledPolicy interface {
	// Eval runs the policy against the given input.
	// Returns:
	//   matched   – true if the policy's "match" rule evaluated to true
	//   faultSpec – non-nil if the policy also defined a "fault_spec" rule
	//   err       – evaluation error
	Eval(input map[string]any) (matched bool, faultSpec *FaultSpec, err error)
}
