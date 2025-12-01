// Package query - AST (Abstract Syntax Tree) type definitions
package query

// Query represents a complete RQL query
type Query struct {
	Match   *MatchClause
	Where   *WhereClause
	Return  *ReturnClause
	OrderBy *OrderByClause
	Limit   *int
}

// MatchClause represents the MATCH part of a query
type MatchClause struct {
	Patterns []Pattern
}

// Pattern represents a graph pattern like (a)-[r]->(b)
type Pattern struct {
	Nodes []NodePattern
	Edges []EdgePattern
}

// NodePattern represents a node in a pattern
type NodePattern struct {
	Variable   string                 // e.g., "p", "friend"
	Label      string                 // e.g., "Person", "Company"
	Properties map[string]interface{} // Inline properties {name: "Alice"}
}

// Direction represents edge direction
type Direction int

const (
	DirectionOut  Direction = iota // ->
	DirectionIn                    // <-
	DirectionBoth                  // - (undirected)
)

// EdgePattern represents an edge/relationship
type EdgePattern struct {
	Variable  string    // e.g., "r"
	Type      string    // e.g., "KNOWS"
	Direction Direction // OUT, IN, BOTH
	MinHops   *int      // For variable-length paths [*1..3]
	MaxHops   *int
}

// WhereClause represents filter conditions
type WhereClause struct {
	Expr Expression
}

// Expression interface for WHERE clause expressions
type Expression interface {
	expressionNode()
}

// BinaryExpr represents binary operations (AND, OR, =, <, >, etc.)
type BinaryExpr struct {
	Left     Expression
	Operator string // "AND", "OR", "=", "!=", "<", ">", "<=", ">="
	Right    Expression
}

func (b *BinaryExpr) expressionNode() {}

// PropertyAccess represents property access like p.name
type PropertyAccess struct {
	Variable string // "p", "friend"
	Property string // "name", "age"
}

func (p *PropertyAccess) expressionNode() {}

// Literal represents constant values
type Literal struct {
	Value interface{} // string, int, float64, bool
}

func (l *Literal) expressionNode() {}

// Identifier represents a variable reference
type Identifier struct {
	Name string
}

func (i *Identifier) expressionNode() {}

// ReturnClause specifies what to return
type ReturnClause struct {
	Items    []ReturnItem
	Distinct bool
}

// ReturnItem represents a single return expression
type ReturnItem struct {
	Expr  Expression
	Alias string // Optional alias
}

// OrderByClause for sorting results
type OrderByClause struct {
	Fields []OrderByField
}

// OrderByField represents a single sort field
type OrderByField struct {
	Expr       Expression
	Descending bool
}

// Result represents query execution results
type Result struct {
	Columns []string
	Rows    []Row
}

// Row represents a single result row
type Row map[string]interface{}

// QueryContext holds runtime query execution context
type QueryContext struct {
	Graph      interface{} // Reference to graph storage
	Variables  map[string]interface{}
	ResultRows []Row
}

// NewQuery creates a new query
func NewQuery() *Query {
	return &Query{}
}

// AddPattern adds a pattern to the MATCH clause
func (q *Query) AddPattern(pattern Pattern) {
	if q.Match == nil {
		q.Match = &MatchClause{
			Patterns: make([]Pattern, 0),
		}
	}
	q.Match.Patterns = append(q.Match.Patterns, pattern)
}

// SetWhere sets the WHERE clause
func (q *Query) SetWhere(expr Expression) {
	q.Where = &WhereClause{Expr: expr}
}

// AddReturnItem adds an item to the RETURN clause
func (q *Query) AddReturnItem(item ReturnItem) {
	if q.Return == nil {
		q.Return = &ReturnClause{
			Items: make([]ReturnItem, 0),
		}
	}
	q.Return.Items = append(q.Return.Items, item)
}

// SetLimit sets the LIMIT value
func (q *Query) SetLimit(limit int) {
	q.Limit = &limit
}

// BindingTable represents variable bindings during execution
type BindingTable map[string]interface{}

// ExecutionPlan represents a compiled query execution plan
type ExecutionPlan struct {
	Operators []Operator
}

// Operator interface for execution plan operators
type Operator interface {
	Execute(ctx *QueryContext) error
}

// ScanOperator scans nodes with optional label filter
type ScanOperator struct {
	Variable string
	Label    string // Optional
}

func (s *ScanOperator) Execute(ctx *QueryContext) error {
	// Implementation in executor
	return nil
}

// FilterOperator applies WHERE predicates
type FilterOperator struct {
	Predicate Expression
}

func (f *FilterOperator) Execute(ctx *QueryContext) error {
	// Implementation in executor
	return nil
}

// ExpandOperator traverses from nodes to neighbors
type ExpandOperator struct {
	SourceVar string
	TargetVar string
	EdgeVar   string
	Direction Direction
	EdgeType  string
	MinHops   int
	MaxHops   int
}

func (e *ExpandOperator) Execute(ctx *QueryContext) error {
	// Implementation in executor
	return nil
}

// ProjectOperator extracts RETURN values
type ProjectOperator struct {
	Items []ReturnItem
}

func (p *ProjectOperator) Execute(ctx *QueryContext) error {
	// Implementation in executor
	return nil
}

// LimitOperator limits result count
type LimitOperator struct {
	Count int
}

func (l *LimitOperator) Execute(ctx *QueryContext) error {
	// Implementation in executor
	return nil
}
