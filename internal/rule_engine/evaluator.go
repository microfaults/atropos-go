package rule_engine

import (
	"fmt"
	"sync"
)

// GraphEvaluator implements the Evaluator interface by traversing the
// rule Graph concurrently.  Each root starts its own goroutine, and at
// every condition node with multiple children the children are fanned
// out concurrently.  Short-circuiting is enforced: when a condition
// node doesn't match, its entire subtree is pruned.
type GraphEvaluator struct {
	graph    *Graph
	policies map[string]CompiledPolicy
}

// NewGraphEvaluator creates an evaluator for a built graph.
// policies must contain a compiled policy for every NodeCondition in the graph.
func NewGraphEvaluator(graph *Graph, policies map[string]CompiledPolicy) *GraphEvaluator {
	return &GraphEvaluator{graph: graph, policies: policies}
}

// Evaluate runs the input against the full rule graph and returns every
// EvalResult whose path evaluated to true.
//
// Concurrency model:
//   - Each root is evaluated in its own goroutine.
//   - At any branching condition node, children are evaluated concurrently.
//   - A mutex-protected slice collects results from all goroutines.
//   - Short-circuit: if a condition returns false, the subtree is skipped.
func (e *GraphEvaluator) Evaluate(input map[string]any) ([]EvalResult, error) {
	roots := e.graph.Roots()
	if len(roots) == 0 {
		return nil, nil
	}

	var (
		mu      sync.Mutex
		results []EvalResult
		errOnce sync.Once
		evalErr error
	)

	collect := func(r EvalResult) {
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}

	setErr := func(err error) {
		errOnce.Do(func() { evalErr = err })
	}

	var wg sync.WaitGroup
	for _, root := range roots {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			e.walk(n, input, nil, collect, setErr)
		}(root)
	}
	wg.Wait()

	if evalErr != nil {
		return results, evalErr
	}
	return results, nil
}

// walk recursively traverses the graph from node, building up the
// matched path.  It calls collect for every EvalResult produced.
func (e *GraphEvaluator) walk(
	node *Node,
	input map[string]any,
	path []string,
	collect func(EvalResult),
	setErr func(error),
) {
	currentPath := append(path, node.ID) //nolint:gocritic // intentional copy

	switch node.Type {
	case NodeFault:
		// Leaf node — emit a result.
		if node.FaultSpec == nil {
			return // defensive; Validate should catch this
		}
		collect(EvalResult{
			MatchedPath: copyPath(currentPath),
			Spec:        *node.FaultSpec,
		})

	case NodeCondition:
		policy, ok := e.policies[node.ID]
		if !ok {
			setErr(fmt.Errorf("evaluator: no compiled policy for node %q", node.ID))
			return
		}

		matched, faultSpec, err := policy.Eval(input)
		if err != nil {
			setErr(fmt.Errorf("evaluator: node %q: %w", node.ID, err))
			return
		}

		if !matched {
			return // short-circuit: prune this subtree
		}

		// Hybrid node: if the Rego policy itself returned a fault_spec,
		// emit it as an additional result.
		if faultSpec != nil {
			collect(EvalResult{
				MatchedPath: copyPath(currentPath),
				Spec:        *faultSpec,
			})
		}

		// Continue to children.
		children := e.graph.Children(node.ID)
		if len(children) == 0 {
			return
		}

		if len(children) == 1 {
			// Fast path: no goroutine overhead for single child.
			e.walk(children[0], input, currentPath, collect, setErr)
			return
		}

		// Fan out children concurrently.
		var wg sync.WaitGroup
		for _, child := range children {
			wg.Add(1)
			go func(c *Node) {
				defer wg.Done()
				e.walk(c, input, currentPath, collect, setErr)
			}(child)
		}
		wg.Wait()
	}
}

// copyPath returns a copy of the path slice so goroutines don't share
// the underlying array.
func copyPath(path []string) []string {
	cp := make([]string, len(path))
	copy(cp, path)
	return cp
}
