package rule_engine

import (
	"fmt"
	"path/filepath"
)

// ─── Options ────────────────────────────────────────────────────────

// Option configures an Engine at construction time.
type Option func(*engineOpts)

type engineOpts struct {
	compiler PolicyCompiler
}

func defaults() engineOpts {
	return engineOpts{
		compiler: &OPACompiler{},
	}
}

// WithCompiler overrides the default OPA compiler (useful for testing).
func WithCompiler(c PolicyCompiler) Option {
	return func(o *engineOpts) { o.compiler = c }
}

// ─── Engine ─────────────────────────────────────────────────────────

// Engine is the top-level rule engine.  It owns the compiled rule
// graph and exposes a single Evaluate method for request payloads.
//
// Typical startup:
//
//	engine, err := rule_engine.NewEngineFromFile("rules/checkout.json")
//	// on each request:
//	results, err := engine.Evaluate(requestJSON)
type Engine struct {
	graph     *Graph
	policies  map[string]CompiledPolicy
	evaluator Evaluator
}

// NewEngine builds an Engine from pre-loaded RuleDefs.
//
// The baseDir parameter is used to resolve relative rego_file paths in
// condition definitions.  Pass "" if all Rego is inline or match-based.
func NewEngine(rules []RuleDef, baseDir string, opts ...Option) (*Engine, error) {
	o := defaults()
	for _, fn := range opts {
		fn(&o)
	}

	graph, policies, err := BuildGraph(rules, o.compiler, baseDir)
	if err != nil {
		return nil, fmt.Errorf("engine: %w", err)
	}

	return &Engine{
		graph:    graph,
		policies: policies,
		evaluator: NewGraphEvaluator(graph, policies),
	}, nil
}

// NewEngineFromFile loads a rule JSON file (single object or array),
// builds and returns an Engine.  Relative rego_file paths are resolved
// against the file's directory.
func NewEngineFromFile(path string, opts ...Option) (*Engine, error) {
	rules, err := LoadFile(path)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Dir(path)
	return NewEngine(rules, baseDir, opts...)
}

// NewEngineFromDir loads all *.json rule files from a directory,
// builds and returns an Engine.  Relative rego_file paths are resolved
// against the given directory.
func NewEngineFromDir(dir string, opts ...Option) (*Engine, error) {
	rules, err := LoadRuleDir(dir)
	if err != nil {
		return nil, err
	}
	return NewEngine(rules, dir, opts...)
}

// Evaluate runs the input map against the compiled rule graph and
// returns all matched EvalResults.
func (e *Engine) Evaluate(input map[string]any) ([]EvalResult, error) {
	return e.evaluator.Evaluate(input)
}

// Graph returns the underlying rule graph (for introspection / debug).
func (e *Engine) Graph() *Graph {
	return e.graph
}
