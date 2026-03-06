package rule_engine

import "fmt"

// Builder provides a fluent API for constructing a rule Graph
// and pre-compiling all Rego policies at startup.
//
// Usage:
//
//	graph, policies, err := rule_engine.NewBuilder(&rule_engine.OPACompiler{}).
//	    AddCondition("svc_match", "Service Match", regoSource).
//	    AddFault("cpu_fault", "CPU Spike", rule_engine.FaultSpec{...}).
//	    Link("svc_match", "cpu_fault").
//	    Build()
type Builder struct {
	graph    *Graph
	compiler PolicyCompiler
	compiled map[string]CompiledPolicy
	err      error // sticky error; once set, all ops become no-ops
}

// NewBuilder creates a Builder that uses the given PolicyCompiler
// (typically *OPACompiler) to compile Rego policies.
func NewBuilder(compiler PolicyCompiler) *Builder {
	return &Builder{
		graph:    NewGraph(),
		compiler: compiler,
		compiled: make(map[string]CompiledPolicy),
	}
}

// AddCondition adds a condition node with the given Rego policy source.
// The policy must define "package <name>" and a "match" rule.
func (b *Builder) AddCondition(id, name, regoSource string) *Builder {
	if b.err != nil {
		return b
	}
	b.err = b.graph.AddNode(&Node{
		ID:     id,
		Name:   name,
		Type:   NodeCondition,
		Policy: regoSource,
	})
	return b
}

// AddConditionDesc is like AddCondition but also sets a description.
func (b *Builder) AddConditionDesc(id, name, description, regoSource string) *Builder {
	if b.err != nil {
		return b
	}
	b.err = b.graph.AddNode(&Node{
		ID:          id,
		Name:        name,
		Description: description,
		Type:        NodeCondition,
		Policy:      regoSource,
	})
	return b
}

// AddFault adds a fault (leaf) node with a complete FaultSpec.
func (b *Builder) AddFault(id, name string, spec FaultSpec) *Builder {
	if b.err != nil {
		return b
	}
	b.err = b.graph.AddNode(&Node{
		ID:        id,
		Name:      name,
		Type:      NodeFault,
		FaultSpec: &spec,
	})
	return b
}

// Link adds a directed edge from parent → child.
func (b *Builder) Link(from, to string) *Builder {
	if b.err != nil {
		return b
	}
	b.err = b.graph.AddEdge(from, to)
	return b
}

// Build validates the graph, compiles all Rego policies, and returns
// the ready-to-use Graph and compiled policy map.
//
// After Build(), the Builder should not be reused.
func (b *Builder) Build() (*Graph, map[string]CompiledPolicy, error) {
	if b.err != nil {
		return nil, nil, fmt.Errorf("builder: %w", b.err)
	}

	// 1. Validate graph structure.
	if err := b.graph.Validate(); err != nil {
		return nil, nil, fmt.Errorf("builder: %w", err)
	}

	// 2. Compile all condition node policies.
	for id, node := range b.graph.nodes {
		if node.Type != NodeCondition {
			continue
		}
		compiled, err := b.compiler.Compile(id, node.Policy)
		if err != nil {
			return nil, nil, fmt.Errorf("builder: failed to compile policy for node %q: %w", id, err)
		}
		b.compiled[id] = compiled
	}

	return b.graph, b.compiled, nil
}
