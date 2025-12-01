package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_SimpleMatch(t *testing.T) {
	input := `MATCH (n) RETURN n`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Match)
	assert.Len(t, query.Match.Patterns, 1)

	pattern := query.Match.Patterns[0]
	assert.Len(t, pattern.Nodes, 1)
	assert.Equal(t, "n", pattern.Nodes[0].Variable)
}

func TestParser_MatchWithLabel(t *testing.T) {
	input := `MATCH (p:Person) RETURN p`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Match)

	node := query.Match.Patterns[0].Nodes[0]
	assert.Equal(t, "p", node.Variable)
	assert.Equal(t, "Person", node.Label)
}

func TestParser_MatchWithEdge(t *testing.T) {
	input := `MATCH (a)-[r]->(b) RETURN a, b`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Match)

	pattern := query.Match.Patterns[0]
	assert.Len(t, pattern.Nodes, 2)
	assert.Len(t, pattern.Edges, 1)

	assert.Equal(t, "a", pattern.Nodes[0].Variable)
	assert.Equal(t, "b", pattern.Nodes[1].Variable)
	assert.Equal(t, "r", pattern.Edges[0].Variable)
	assert.Equal(t, DirectionOut, pattern.Edges[0].Direction)
}

func TestParser_MatchWithTypedEdge(t *testing.T) {
	input := `MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p, f`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)

	pattern := query.Match.Patterns[0]
	assert.Equal(t, "Person", pattern.Nodes[0].Label)
	assert.Equal(t, "Person", pattern.Nodes[1].Label)
	assert.Equal(t, "KNOWS", pattern.Edges[0].Type)
}

func TestParser_WhereClause(t *testing.T) {
	input := `MATCH (p:Person) WHERE p.age > 25 RETURN p`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Where)

	binExpr, ok := query.Where.Expr.(*BinaryExpr)
	require.True(t, ok)

	propAccess, ok := binExpr.Left.(*PropertyAccess)
	require.True(t, ok)
	assert.Equal(t, "p", propAccess.Variable)
	assert.Equal(t, "age", propAccess.Property)

	assert.Equal(t, ">", binExpr.Operator)

	literal, ok := binExpr.Right.(*Literal)
	require.True(t, ok)
	assert.Equal(t, 25, literal.Value)
}

func TestParser_WhereWithAnd(t *testing.T) {
	input := `MATCH (p:Person) WHERE p.age > 25 AND p.city = "SF" RETURN p`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Where)

	binExpr, ok := query.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "AND", binExpr.Operator)
}

func TestParser_ReturnMultiple(t *testing.T) {
	input := `MATCH (p:Person) RETURN p.name, p.age`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Return)
	assert.Len(t, query.Return.Items, 2)

	// First return item
	prop1, ok := query.Return.Items[0].Expr.(*PropertyAccess)
	require.True(t, ok)
	assert.Equal(t, "p", prop1.Variable)
	assert.Equal(t, "name", prop1.Property)

	// Second return item
	prop2, ok := query.Return.Items[1].Expr.(*PropertyAccess)
	require.True(t, ok)
	assert.Equal(t, "p", prop2.Variable)
	assert.Equal(t, "age", prop2.Property)
}

func TestParser_LimitClause(t *testing.T) {
	input := `MATCH (p:Person) RETURN p LIMIT 10`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Limit)
	assert.Equal(t, 10, *query.Limit)
}

func TestParser_CompleteQuery(t *testing.T) {
	input := `
		MATCH (p:Person)-[:KNOWS]->(friend:Person)
		WHERE p.name = "Alice" AND friend.age > 25
		RETURN friend.name, friend.age
		LIMIT 10
	`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	require.NotNil(t, query.Match)
	require.NotNil(t, query.Where)
	require.NotNil(t, query.Return)
	require.NotNil(t, query.Limit)

	assert.Equal(t, 10, *query.Limit)
}

func TestParser_InlineProperties(t *testing.T) {
	input := `MATCH (p:Person {name: "Alice", age: 30}) RETURN p`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)
	node := query.Match.Patterns[0].Nodes[0]

	assert.Equal(t, "Person", node.Label)
	assert.Equal(t, "Alice", node.Properties["name"])
	assert.Equal(t, 30, node.Properties["age"])
}

func TestParser_IncomingEdge(t *testing.T) {
	input := `MATCH (a)<-[:FOLLOWS]-(b) RETURN a, b`

	p := NewParser(input)
	query, err := p.Parse()

	require.NoError(t, err)

	edge := query.Match.Patterns[0].Edges[0]
	assert.Equal(t, DirectionIn, edge.Direction)
	assert.Equal(t, "FOLLOWS", edge.Type)
}

func TestParser_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing paren", "MATCH (n RETURN n"},
		{"missing bracket", "MATCH (a)-[>(b) RETURN a"},
		{"invalid WHERE", "MATCH (n) WHERE RETURN n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.input)
			_, err := p.Parse()
			assert.Error(t, err, "should error for: %s", tt.name)
		})
	}
}
