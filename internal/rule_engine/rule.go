package rule_engine

import (
	"fmt"
	"os"
	"path/filepath"
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

// ─── Enum types ─────────────────────────────────────────────────────

// FaultKind selects which type of fault to inject.
type FaultKind string

const (
	FaultCPU        FaultKind = "cpu"
	FaultIO         FaultKind = "io"
	FaultLatency    FaultKind = "latency"
	FaultPacketDrop FaultKind = "packet_drop"
)

// InjectionPoint is where in the request lifecycle the fault is injected.
type InjectionPoint string

const (
	InjectInbound   InjectionPoint = "inbound"
	InjectOutbound  InjectionPoint = "outbound"
	InjectTransient InjectionPoint = "transient"
	InjectCustom    InjectionPoint = "custom"
)

// IOMode selects the direction of I/O stress.
type IOMode string

const (
	IOModeRead      IOMode = "read"
	IOModeWrite     IOMode = "write"
	IOModeReadWrite IOMode = "readwrite"
)

// ─── FaultSpec ──────────────────────────────────────────────────────

// FaultSpec is the complete fault configuration.
// Condition nodes can embed this in their Rego policy as "fault_spec",
// and fault (leaf) nodes carry it directly.
//
// Base timing (duration, ramp) is common to every fault. Type-specific
// parameters live in the matching sub-struct (CPU, IO, Latency, Network).
// Only the sub-struct corresponding to FaultType should be populated.
//
// Metrics is a free-form map whose keys become span attribute names on
// the fault's trace span (e.g. {"target_load": 0.8, "window_ms": 100}).
type FaultSpec struct {
	// ── Discriminators ──

	// FaultType selects which sub-struct carries the type-specific config.
	FaultType FaultKind `json:"fault_type"`

	// InjectionPoint is where in the request lifecycle to inject.
	InjectionPoint InjectionPoint `json:"injection_point"`

	// TargetService is the downstream service to target (outbound faults).
	TargetService string `json:"target_service,omitempty"`

	// ── Base timing (common to all faults) ──

	DurationMs int64 `json:"duration_ms"`
	RampUpMs   int64 `json:"ramp_up_ms,omitempty"`
	RampDownMs int64 `json:"ramp_down_ms,omitempty"`

	// ── Type-specific params ──

	CPU     *CPUParams     `json:"cpu,omitempty"`
	IO      *IOParams      `json:"io,omitempty"`
	Latency *LatencyParams `json:"latency,omitempty"`
	Network *NetworkParams `json:"network,omitempty"`

	// ── Observability ──

	// Metrics to attach to the fault's trace span.
	// Keys become OTel span attribute names; values are recorded as-is.
	Metrics map[string]any `json:"metrics,omitempty"`
}

// Validate checks that the FaultSpec is well-formed.
func (s *FaultSpec) Validate() error {
	if s.FaultType == "" {
		return fmt.Errorf("fault_spec: fault_type is required")
	}
	if s.InjectionPoint == "" {
		return fmt.Errorf("fault_spec: injection_point is required")
	}
	if s.DurationMs <= 0 {
		return fmt.Errorf("fault_spec: duration_ms must be > 0")
	}

	switch s.FaultType {
	case FaultCPU:
		if s.CPU == nil {
			return fmt.Errorf("fault_spec: cpu params required for fault_type=cpu")
		}
		return s.CPU.Validate()
	case FaultIO:
		if s.IO == nil {
			return fmt.Errorf("fault_spec: io params required for fault_type=io")
		}
		return s.IO.Validate()
	case FaultLatency:
		if s.Latency == nil {
			return fmt.Errorf("fault_spec: latency params required for fault_type=latency")
		}
		return s.Latency.Validate()
	case FaultPacketDrop:
		if s.Network == nil {
			return fmt.Errorf("fault_spec: network params required for fault_type=packet_drop")
		}
		return s.Network.Validate()
	default:
		return fmt.Errorf("fault_spec: unknown fault_type %q", s.FaultType)
	}
}

// CPUParams holds CPU-pressure fault parameters.
type CPUParams struct {
	// TargetLoad is the fraction of available CPU to consume (0.0, 1.0].
	TargetLoad float64 `json:"target_load"`
	// WindowMs is the duty-cycle period in milliseconds.
	WindowMs int64 `json:"window_ms,omitempty"`
}

// Validate checks CPUParams constraints.
func (p *CPUParams) Validate() error {
	if p.TargetLoad <= 0 || p.TargetLoad > 1.0 {
		return fmt.Errorf("cpu: target_load must be in (0.0, 1.0], got %.2f", p.TargetLoad)
	}
	return nil
}

// IOParams holds I/O stress fault parameters.
type IOParams struct {
	ReadRate  int64  `json:"read_rate,omitempty"`
	FileSize  int    `json:"file_size,omitempty"`
	FileCount int    `json:"file_count,omitempty"`
	Workers   int    `json:"workers,omitempty"`
	IOMode    IOMode `json:"io_mode,omitempty"`
}

// Validate checks IOParams constraints.
func (p *IOParams) Validate() error {
	// All fields have sensible defaults, so nothing is strictly required.
	return nil
}

// LatencyParams holds latency-injection parameters.
type LatencyParams struct {
	// DelayMs is the fixed delay to inject in milliseconds.
	DelayMs int64 `json:"delay_ms"`
	// Jitter is a 0.0–1.0 factor applied as ±(Jitter×DelayMs) randomness.
	Jitter float64 `json:"jitter,omitempty"`
}

// Validate checks LatencyParams constraints.
func (p *LatencyParams) Validate() error {
	if p.DelayMs <= 0 {
		return fmt.Errorf("latency: delay_ms must be > 0, got %d", p.DelayMs)
	}
	if p.Jitter < 0 || p.Jitter > 1.0 {
		return fmt.Errorf("latency: jitter must be in [0.0, 1.0], got %.2f", p.Jitter)
	}
	return nil
}

// NetworkParams holds network-fault parameters.
type NetworkParams struct {
	// DropRate is the fraction of packets to drop (0.0, 1.0].
	DropRate float64 `json:"drop_rate"`
}

// Validate checks NetworkParams constraints.
func (p *NetworkParams) Validate() error {
	if p.DropRate <= 0 || p.DropRate > 1.0 {
		return fmt.Errorf("network: drop_rate must be in (0.0, 1.0], got %.2f", p.DropRate)
	}
	return nil
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

	// Option B: Rego inline — for power users
	// Must define "package <name>" and a "match" rule.
	Rego string `json:"rego,omitempty"`

	// Option C: Rego file — path to a .rego file (relative to the rule JSON).
	// Loaded at graph-build time. Same requirements as inline Rego.
	RegoFile string `json:"rego_file,omitempty"`
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
// baseDir is the directory against which relative rego_file paths are
// resolved (typically the directory containing the rule JSON file).
// Pass "" if no file-based Rego references are used.
//
// For each ConditionDef the Rego source is resolved with this priority:
//   1. Inline Rego string
//   2. rego_file path (read at build time)
//   3. Match rules (auto-compiled)
//
//	rules := []RuleDef{ ... }
//	graph, policies, err := BuildGraph(rules, &OPACompiler{}, "/path/to/rules")
func BuildGraph(rules []RuleDef, compiler PolicyCompiler, baseDir string) (*Graph, map[string]CompiledPolicy, error) {
	b := NewBuilder(compiler)
	for _, r := range rules {
		for _, c := range r.Conditions {
			regoSrc, err := resolveRego(c, baseDir)
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
//
// Priority: inline Rego > rego_file > match rules.
func resolveRego(c ConditionDef, baseDir string) (string, error) {
	// 1. Inline Rego string.
	if c.Rego != "" {
		return c.Rego, nil
	}

	// 2. Rego file reference.
	if c.RegoFile != "" {
		path := c.RegoFile
		if !filepath.IsAbs(path) && baseDir != "" {
			path = filepath.Join(baseDir, path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("failed to read rego_file %q: %w", c.RegoFile, err)
		}
		src := strings.TrimSpace(string(data))
		if src == "" {
			return "", fmt.Errorf("rego_file %q is empty", c.RegoFile)
		}
		return src, nil
	}

	// 3. Structured match rules → auto-compile to Rego.
	if len(c.Match) == 0 {
		return "", fmt.Errorf("condition must have 'rego', 'rego_file', or 'match' rules")
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
