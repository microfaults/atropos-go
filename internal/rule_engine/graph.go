package rule_engine

import "fmt"

// Graph is a directed acyclic graph of rule nodes.
// It is built once at startup via the Builder and then used read-only
// by the evaluator on every request.
type Graph struct {
	nodes    map[string]*Node   // id → node
	children map[string][]string // id → list of child IDs
	parents  map[string][]string // id → list of parent IDs (for root detection)
}

// NewGraph creates an empty rule graph.
func NewGraph() *Graph {
	return &Graph{
		nodes:    make(map[string]*Node),
		children: make(map[string][]string),
		parents:  make(map[string][]string),
	}
}

// AddNode registers a node. Errors if the ID is already taken.
func (g *Graph) AddNode(n *Node) error {
	if n.ID == "" {
		return fmt.Errorf("graph: node ID must not be empty")
	}
	if _, exists := g.nodes[n.ID]; exists {
		return fmt.Errorf("graph: duplicate node ID %q", n.ID)
	}
	g.nodes[n.ID] = n
	return nil
}

// AddEdge adds a directed edge from → to. Both nodes must exist.
func (g *Graph) AddEdge(from, to string) error {
	if _, ok := g.nodes[from]; !ok {
		return fmt.Errorf("graph: edge source %q not found", from)
	}
	if _, ok := g.nodes[to]; !ok {
		return fmt.Errorf("graph: edge target %q not found", to)
	}
	if from == to {
		return fmt.Errorf("graph: self-loop on %q", from)
	}

	// Check for duplicate edge.
	for _, c := range g.children[from] {
		if c == to {
			return fmt.Errorf("graph: duplicate edge %q → %q", from, to)
		}
	}

	g.children[from] = append(g.children[from], to)
	g.parents[to] = append(g.parents[to], from)
	return nil
}

// Roots returns all nodes with no incoming edges.
// These are the entry points for evaluation.
func (g *Graph) Roots() []*Node {
	var roots []*Node
	for id, node := range g.nodes {
		if len(g.parents[id]) == 0 {
			roots = append(roots, node)
		}
	}
	return roots
}

// Children returns the direct successors of the given node.
func (g *Graph) Children(nodeID string) []*Node {
	childIDs := g.children[nodeID]
	result := make([]*Node, 0, len(childIDs))
	for _, cid := range childIDs {
		if n, ok := g.nodes[cid]; ok {
			result = append(result, n)
		}
	}
	return result
}

// Node returns a node by ID, or nil if not found.
func (g *Graph) Node(id string) *Node {
	return g.nodes[id]
}

// NodeCount returns the total number of nodes.
func (g *Graph) NodeCount() int {
	return len(g.nodes)
}

// EdgeCount returns the total number of edges.
func (g *Graph) EdgeCount() int {
	count := 0
	for _, kids := range g.children {
		count += len(kids)
	}
	return count
}

// Validate checks the graph for structural problems:
//   - No cycles (DAG property)
//   - Fault nodes must be leaves (no outgoing edges)
//   - Condition nodes must have a non-empty Policy
//   - Fault nodes must have a non-nil FaultSpec
//   - At least one root
func (g *Graph) Validate() error {
	// 1. Check node invariants.
	for id, n := range g.nodes {
		switch n.Type {
		case NodeCondition:
			if n.Policy == "" {
				return fmt.Errorf("graph: condition node %q has empty policy", id)
			}
		case NodeFault:
			if n.FaultSpec == nil {
				return fmt.Errorf("graph: fault node %q has nil FaultSpec", id)
			}
			if len(g.children[id]) > 0 {
				return fmt.Errorf("graph: fault node %q must be a leaf but has %d children", id, len(g.children[id]))
			}
		default:
			return fmt.Errorf("graph: node %q has unknown type %d", id, n.Type)
		}
	}

	// 2. At least one root.
	roots := g.Roots()
	if len(roots) == 0 {
		return fmt.Errorf("graph: no root nodes (every node has a parent — cycle?)")
	}

	// 3. Cycle detection via DFS with coloring.
	const (
		white = 0 // unvisited
		gray  = 1 // in current DFS path
		black = 2 // fully processed
	)
	color := make(map[string]int, len(g.nodes))

	var dfs func(id string) error
	dfs = func(id string) error {
		color[id] = gray
		for _, cid := range g.children[id] {
			switch color[cid] {
			case gray:
				return fmt.Errorf("graph: cycle detected involving node %q → %q", id, cid)
			case white:
				if err := dfs(cid); err != nil {
					return err
				}
			}
		}
		color[id] = black
		return nil
	}

	for id := range g.nodes {
		if color[id] == white {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}

	return nil
}
