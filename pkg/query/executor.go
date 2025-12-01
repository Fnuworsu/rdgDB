package query

import (
	"fmt"
	"reflect"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/storage"
)

// GraphStorage interface defines what the executor needs from the storage layer
type GraphStorage interface {
	IterateNodes(callback func(*graph.Node) bool)
	GetNode(id graph.NodeID) (*graph.Node, error)
	GetNeighbors(nodeID graph.NodeID) ([]*graph.Node, error)
	GetIncomingNeighbors(nodeID graph.NodeID) ([]*graph.Node, error)
}

// Execute runs the query against the graph
func (q *Query) Execute(g *storage.Graph) (*Result, error) {
	// 1. Build Execution Plan
	plan, err := BuildExecutionPlan(q)
	if err != nil {
		return nil, err
	}

	// 2. Initialize Context
	ctx := &QueryContext{
		Graph:      g,
		Variables:  make(map[string]interface{}),
		ResultRows: make([]Row, 0),
		// Initialize with one empty match to start the pipeline
		Matches: []BindingTable{make(BindingTable)},
	}

	// 3. Execute Operators
	for _, op := range plan.Operators {
		if err := op.Execute(ctx); err != nil {
			return nil, err
		}
		// If no matches left, stop early
		if len(ctx.Matches) == 0 {
			break
		}
	}

	// 4. Return Results
	// The last operator (Project) should have populated ResultRows
	// If not, we might need to do it here if we want "SELECT *" behavior by default
	// But for now assume ProjectOperator does it.

	columns := []string{}
	if q.Return != nil {
		for _, item := range q.Return.Items {
			name := item.Alias
			if name == "" {
				// Try to infer name from expression
				if id, ok := item.Expr.(*Identifier); ok {
					name = id.Name
				} else if prop, ok := item.Expr.(*PropertyAccess); ok {
					name = prop.Variable + "." + prop.Property
				} else {
					name = "expr"
				}
			}
			columns = append(columns, name)
		}
	}

	return &Result{
		Columns: columns,
		Rows:    ctx.ResultRows,
	}, nil
}

// BuildExecutionPlan converts AST to a linear sequence of operators
func BuildExecutionPlan(q *Query) (*ExecutionPlan, error) {
	plan := &ExecutionPlan{
		Operators: make([]Operator, 0),
	}

	if q.Match == nil {
		return nil, fmt.Errorf("MATCH clause is required")
	}

	// Simple planner: handle first pattern
	// TODO: Handle multiple patterns and joins
	if len(q.Match.Patterns) > 0 {
		pattern := q.Match.Patterns[0]

		// 1. Scan first node
		if len(pattern.Nodes) > 0 {
			startNode := pattern.Nodes[0]
			plan.Operators = append(plan.Operators, &ScanOperator{
				Variable: startNode.Variable,
				Label:    startNode.Label,
			})

			// Filter start node properties
			if len(startNode.Properties) > 0 {
				for k, v := range startNode.Properties {
					// Create a filter: variable.k = v
					plan.Operators = append(plan.Operators, &FilterOperator{
						Predicate: &BinaryExpr{
							Left:     &PropertyAccess{Variable: startNode.Variable, Property: k},
							Operator: "=",
							Right:    &Literal{Value: v},
						},
					})
				}
			}
		}

		// 2. Expand to subsequent nodes
		for i := 0; i < len(pattern.Edges); i++ {
			edge := pattern.Edges[i]
			targetNode := pattern.Nodes[i+1]
			sourceVar := pattern.Nodes[i].Variable

			plan.Operators = append(plan.Operators, &ExpandOperator{
				SourceVar: sourceVar,
				TargetVar: targetNode.Variable,
				EdgeVar:   edge.Variable,
				Direction: edge.Direction,
				EdgeType:  edge.Type,
			})

			// Filter target node properties
			if len(targetNode.Properties) > 0 {
				for k, v := range targetNode.Properties {
					plan.Operators = append(plan.Operators, &FilterOperator{
						Predicate: &BinaryExpr{
							Left:     &PropertyAccess{Variable: targetNode.Variable, Property: k},
							Operator: "=",
							Right:    &Literal{Value: v},
						},
					})
				}
			}
		}
	}

	// 3. Apply WHERE clause
	if q.Where != nil {
		plan.Operators = append(plan.Operators, &FilterOperator{
			Predicate: q.Where.Expr,
		})
	}

	// 4. Apply RETURN clause (Projection)
	if q.Return != nil {
		plan.Operators = append(plan.Operators, &ProjectOperator{
			Items: q.Return.Items,
		})
	}

	// 5. Apply LIMIT
	if q.Limit != nil {
		plan.Operators = append(plan.Operators, &LimitOperator{
			Count: *q.Limit,
		})
	}

	return plan, nil
}

// --- Operator Implementations ---

// ScanOperator implementation
func (s *ScanOperator) Execute(ctx *QueryContext) error {
	g, ok := ctx.Graph.(GraphStorage)
	if !ok {
		return fmt.Errorf("invalid graph storage")
	}

	newMatches := make([]BindingTable, 0)

	// Iterate all nodes
	// In a real system, we would use an index if Label is present
	g.IterateNodes(func(node *graph.Node) bool {
		// Filter by label if specified
		if s.Label != "" && node.Label != s.Label {
			return true // continue
		}

		// Create a new match for each node
		// Cartesian product with existing matches (which is just [{}] initially)
		for _, existingMatch := range ctx.Matches {
			newMatch := copyBindingTable(existingMatch)
			if s.Variable != "" {
				newMatch[s.Variable] = node
			}
			newMatches = append(newMatches, newMatch)
		}
		return true
	})

	ctx.Matches = newMatches
	return nil
}

// FilterOperator implementation
func (f *FilterOperator) Execute(ctx *QueryContext) error {
	filteredMatches := make([]BindingTable, 0)

	for _, match := range ctx.Matches {
		result, err := evaluateExpression(f.Predicate, match)
		if err != nil {
			return err
		}
		if b, ok := result.(bool); ok && b {
			filteredMatches = append(filteredMatches, match)
		}
	}

	ctx.Matches = filteredMatches
	return nil
}

// ExpandOperator implementation
func (e *ExpandOperator) Execute(ctx *QueryContext) error {
	// g is not used directly, we cast to *storage.Graph later
	// but we should verify it implements GraphStorage at least
	if _, ok := ctx.Graph.(GraphStorage); !ok {
		return fmt.Errorf("invalid graph storage")
	}

	newMatches := make([]BindingTable, 0)

	for _, match := range ctx.Matches {
		sourceNodeObj, ok := match[e.SourceVar]
		if !ok {
			return fmt.Errorf("variable %s not found", e.SourceVar)
		}
		sourceNode, ok := sourceNodeObj.(*graph.Node)
		if !ok {
			return fmt.Errorf("variable %s is not a node", e.SourceVar)
		}

		// Handle direction
		// TODO: This simple implementation doesn't capture the Edge object itself in the binding
		// We need to iterate edges to capture edge variables and properties
		// For now, let's just get neighbors and assume we don't need edge details unless requested
		// But wait, if we need to filter on edge type, we need to check edges.

		// Let's use the low-level adjacency list to filter by type and direction

		// Outgoing
		if e.Direction == DirectionOut || e.Direction == DirectionBoth {
			sourceNode.Mu.RLock()
			outEdges := make([]graph.EdgeID, len(sourceNode.OutEdges))
			copy(outEdges, sourceNode.OutEdges)
			sourceNode.Mu.RUnlock()

			realGraph, ok := ctx.Graph.(*storage.Graph)
			if !ok {
				// Fallback or error
				continue
			}

			for _, edgeID := range outEdges {
				edge, err := realGraph.GetEdge(edgeID)
				if err != nil {
					continue
				}

				// Filter by type
				if e.EdgeType != "" && edge.Label != e.EdgeType {
					continue
				}

				targetNode, err := realGraph.GetNode(edge.Target)
				if err != nil {
					continue
				}

				// Create match
				newMatch := copyBindingTable(match)
				if e.TargetVar != "" {
					newMatch[e.TargetVar] = targetNode
				}
				if e.EdgeVar != "" {
					newMatch[e.EdgeVar] = edge
				}
				newMatches = append(newMatches, newMatch)
			}
		}

		// Incoming (similar logic)
		if e.Direction == DirectionIn || e.Direction == DirectionBoth {
			sourceNode.Mu.RLock()
			inEdges := make([]graph.EdgeID, len(sourceNode.InEdges))
			copy(inEdges, sourceNode.InEdges)
			sourceNode.Mu.RUnlock()

			realGraph, ok := ctx.Graph.(*storage.Graph)
			if !ok {
				continue
			}

			for _, edgeID := range inEdges {
				edge, err := realGraph.GetEdge(edgeID)
				if err != nil {
					continue
				}

				if e.EdgeType != "" && edge.Label != e.EdgeType {
					continue
				}

				targetNode, err := realGraph.GetNode(edge.Source)
				if err != nil {
					continue
				}

				newMatch := copyBindingTable(match)
				if e.TargetVar != "" {
					newMatch[e.TargetVar] = targetNode
				}
				if e.EdgeVar != "" {
					newMatch[e.EdgeVar] = edge
				}
				newMatches = append(newMatches, newMatch)
			}
		}
	}

	ctx.Matches = newMatches
	return nil
}

// ProjectOperator implementation
func (p *ProjectOperator) Execute(ctx *QueryContext) error {
	ctx.ResultRows = make([]Row, 0, len(ctx.Matches))

	for _, match := range ctx.Matches {
		row := make(Row)
		for _, item := range p.Items {
			val, err := evaluateExpression(item.Expr, match)
			if err != nil {
				return err
			}

			name := item.Alias
			if name == "" {
				// Simple alias generation
				if id, ok := item.Expr.(*Identifier); ok {
					name = id.Name
				} else if prop, ok := item.Expr.(*PropertyAccess); ok {
					name = prop.Variable + "." + prop.Property
				} else {
					name = fmt.Sprintf("col_%d", len(row))
				}
			}
			row[name] = val
		}
		ctx.ResultRows = append(ctx.ResultRows, row)
	}
	return nil
}

// LimitOperator implementation
func (l *LimitOperator) Execute(ctx *QueryContext) error {
	if len(ctx.ResultRows) > l.Count {
		ctx.ResultRows = ctx.ResultRows[:l.Count]
	}
	// Also limit matches if we are not at the end?
	// Usually Limit is last, so ResultRows is what matters.
	if len(ctx.Matches) > l.Count {
		ctx.Matches = ctx.Matches[:l.Count]
	}
	return nil
}

// --- Helpers ---

func copyBindingTable(bt BindingTable) BindingTable {
	newBt := make(BindingTable)
	for k, v := range bt {
		newBt[k] = v
	}
	return newBt
}

func evaluateExpression(expr Expression, match BindingTable) (interface{}, error) {
	switch e := expr.(type) {
	case *Literal:
		return e.Value, nil
	case *Identifier:
		val, ok := match[e.Name]
		if !ok {
			return nil, fmt.Errorf("variable %s not found", e.Name)
		}
		return val, nil
	case *PropertyAccess:
		obj, ok := match[e.Variable]
		if !ok {
			return nil, fmt.Errorf("variable %s not found", e.Variable)
		}

		// Check if obj is Node or Edge
		if node, ok := obj.(*graph.Node); ok {
			val, exists := node.GetProperty(e.Property)
			if !exists {
				return nil, nil // Property not found is null
			}
			return val, nil
		} else if edge, ok := obj.(*graph.Edge); ok {
			val, exists := edge.GetProperty(e.Property)
			if !exists {
				return nil, nil
			}
			return val, nil
		}
		return nil, fmt.Errorf("variable %s is not a node or edge", e.Variable)

	case *BinaryExpr:
		left, err := evaluateExpression(e.Left, match)
		if err != nil {
			return nil, err
		}
		right, err := evaluateExpression(e.Right, match)
		if err != nil {
			return nil, err
		}

		return compareValues(left, e.Operator, right)
	}
	return nil, fmt.Errorf("unknown expression type: %T", expr)
}

func compareValues(left interface{}, op string, right interface{}) (bool, error) {
	// Simple comparison logic for MVP
	// TODO: Handle type coercion and more types

	switch op {
	case "=":
		return reflect.DeepEqual(left, right), nil
	case "!=":
		return !reflect.DeepEqual(left, right), nil
	case "AND":
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if !ok1 || !ok2 {
			return false, fmt.Errorf("AND requires boolean operands")
		}
		return l && r, nil
	case "OR":
		l, ok1 := left.(bool)
		r, ok2 := right.(bool)
		if !ok1 || !ok2 {
			return false, fmt.Errorf("OR requires boolean operands")
		}
		return l || r, nil
	case ">":
		return compareNumbers(left, right) > 0, nil
	case "<":
		return compareNumbers(left, right) < 0, nil
	case ">=":
		return compareNumbers(left, right) >= 0, nil
	case "<=":
		return compareNumbers(left, right) <= 0, nil
	}

	return false, fmt.Errorf("unknown operator: %s", op)
}

func compareNumbers(a, b interface{}) int {
	// Convert to float64 for comparison
	v1 := toFloat(a)
	v2 := toFloat(b)

	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func toFloat(v interface{}) float64 {
	switch i := v.(type) {
	case int:
		return float64(i)
	case int64:
		return float64(i)
	case float64:
		return i
	case float32:
		return float64(i)
	}
	return 0
}
