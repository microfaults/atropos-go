package rule_engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/open-policy-agent/opa/v1/rego"
)

// OPACompiler implements PolicyCompiler using the OPA Go SDK.
// Each policy is compiled once at startup into a PreparedEvalQuery
// for fast, lock-free evaluation on every request.
type OPACompiler struct{}

// Compile parses and prepares a Rego policy for evaluation.
//
// The Rego source MUST define:
//   - A "match" rule (boolean) — whether the condition is satisfied
//
// It MAY additionally define:
//   - A "fault_spec" rule (object) — the complete fault config returned on match
//
// The policyID is used as the Rego package path for disambiguation.
func (c *OPACompiler) Compile(policyID string, regoSource string) (CompiledPolicy, error) {
	// Derive the package name from the Rego source itself.
	// The source must contain a "package ..." declaration.
	// We query for data.<package>.match and data.<package>.fault_spec.

	// Determine the query path from the policy source.
	// For simplicity, we ask OPA to evaluate two queries:
	//   1. data.<pkg>.match
	//   2. data.<pkg>.fault_spec
	// We parse the package name from the source.
	pkg, err := parsePackageName(regoSource)
	if err != nil {
		return nil, fmt.Errorf("opa: policy %q: %w", policyID, err)
	}

	matchQuery := fmt.Sprintf("data.%s.match", pkg)
	specQuery := fmt.Sprintf("data.%s.fault_spec", pkg)

	// Prepare the match query.
	matchRego := rego.New(
		rego.Query(matchQuery),
		rego.Module(policyID+".rego", regoSource),
	)
	matchPrepared, err := matchRego.PrepareForEval(context.Background())
	if err != nil {
		return nil, fmt.Errorf("opa: failed to compile match query for %q: %w", policyID, err)
	}

	// Prepare the fault_spec query (may return empty results if not defined).
	specRego := rego.New(
		rego.Query(specQuery),
		rego.Module(policyID+".rego", regoSource),
	)
	specPrepared, err := specRego.PrepareForEval(context.Background())
	if err != nil {
		return nil, fmt.Errorf("opa: failed to compile fault_spec query for %q: %w", policyID, err)
	}

	return &OPAPolicy{
		id:      policyID,
		match:   matchPrepared,
		spec:    specPrepared,
		hasSpec: true, // we'll try both; Eval handles empty results
	}, nil
}

// OPAPolicy is a compiled, reusable Rego policy.
type OPAPolicy struct {
	id      string
	match   rego.PreparedEvalQuery
	spec    rego.PreparedEvalQuery
	hasSpec bool
}

// Eval runs the compiled policy against the given input.
// Returns (matched, faultSpec-or-nil, error).
func (p *OPAPolicy) Eval(input map[string]any) (bool, *FaultSpec, error) {
	ctx := context.Background()

	// 1. Evaluate the "match" query.
	matchRS, err := p.match.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return false, nil, fmt.Errorf("opa: eval match for %q: %w", p.id, err)
	}

	matched := extractBool(matchRS)
	if !matched {
		return false, nil, nil
	}

	// 2. If matched, try to extract "fault_spec".
	var faultSpec *FaultSpec
	if p.hasSpec {
		specRS, err := p.spec.Eval(ctx, rego.EvalInput(input))
		if err != nil {
			// fault_spec is optional; if it errors, we still matched.
			return true, nil, nil
		}
		faultSpec = extractFaultSpec(specRS)
	}

	return true, faultSpec, nil
}

// extractBool pulls a boolean from an OPA result set.
// Returns false if the result set is empty or the value is not a bool.
func extractBool(rs rego.ResultSet) bool {
	if len(rs) == 0 {
		return false
	}
	if len(rs[0].Expressions) == 0 {
		return false
	}
	b, ok := rs[0].Expressions[0].Value.(bool)
	return ok && b
}

// extractFaultSpec pulls a FaultSpec from an OPA result set.
// The result is expected to be a JSON-like map from the "fault_spec" rule.
// Returns nil if not present or not a valid spec.
func extractFaultSpec(rs rego.ResultSet) *FaultSpec {
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return nil
	}

	val := rs[0].Expressions[0].Value
	if val == nil {
		return nil
	}

	// OPA returns maps as map[string]interface{}.
	// We marshal → unmarshal through JSON to get a typed FaultSpec.
	raw, err := json.Marshal(val)
	if err != nil {
		return nil
	}

	var spec FaultSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil
	}
	return &spec
}

// parsePackageName extracts the Rego package name from source code.
// It does a simple scan for "package <name>".
func parsePackageName(source string) (string, error) {
	// Simple scanner: find "package" keyword and read the package path.
	const keyword = "package "
	for i := 0; i < len(source); i++ {
		// Skip comments.
		if source[i] == '#' {
			for i < len(source) && source[i] != '\n' {
				i++
			}
			continue
		}
		// Skip whitespace.
		if source[i] == ' ' || source[i] == '\t' || source[i] == '\n' || source[i] == '\r' {
			continue
		}
		// Look for "package ".
		if i+len(keyword) <= len(source) && source[i:i+len(keyword)] == keyword {
			start := i + len(keyword)
			end := start
			for end < len(source) && source[end] != '\n' && source[end] != '\r' && source[end] != ' ' {
				end++
			}
			pkg := source[start:end]
			if pkg == "" {
				return "", fmt.Errorf("empty package name")
			}
			return pkg, nil
		}
		// If we hit a non-whitespace, non-comment character that isn't "package",
		// the source doesn't start with a package declaration.
		return "", fmt.Errorf("expected 'package' declaration, got %q...", source[i:min(i+20, len(source))])
	}
	return "", fmt.Errorf("no package declaration found")
}
