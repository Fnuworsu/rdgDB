package query

import (
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestGraph(t *testing.T) *storage.Graph {
	g := storage.NewGraph()

	// Alice (30, SF)
	alice, _ := g.AddNode("Person", graph.Properties{"name": "Alice", "age": 30, "city": "SF"})

	// Bob (25, NY)
	bob, _ := g.AddNode("Person", graph.Properties{"name": "Bob", "age": 25, "city": "NY"})

	// Charlie (35, SF)
	charlie, _ := g.AddNode("Person", graph.Properties{"name": "Charlie", "age": 35, "city": "SF"})

	// Alice KNOWS Bob
	g.AddEdge(alice.ID, bob.ID, "KNOWS", nil)

	// Bob KNOWS Charlie
	g.AddEdge(bob.ID, charlie.ID, "KNOWS", nil)

	// Alice WORKS_AT Google
	google, _ := g.AddNode("Company", graph.Properties{"name": "Google"})
	g.AddEdge(alice.ID, google.ID, "WORKS_AT", nil)

	return g
}

func TestExecute_Scan(t *testing.T) {
	g := createTestGraph(t)

	// MATCH (n:Person) RETURN n.name
	query := NewQuery()
	query.AddPattern(Pattern{
		Nodes: []NodePattern{{Variable: "n", Label: "Person"}},
	})
	query.AddReturnItem(ReturnItem{
		Expr: &PropertyAccess{Variable: "n", Property: "name"},
	})

	result, err := query.Execute(g)
	require.NoError(t, err)

	assert.Len(t, result.Rows, 3)

	names := make(map[string]bool)
	for _, row := range result.Rows {
		name := row["n.name"].(string)
		names[name] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])
	assert.True(t, names["Charlie"])
}

func TestExecute_Filter(t *testing.T) {
	g := createTestGraph(t)

	// MATCH (n:Person) WHERE n.age > 28 RETURN n.name
	query := NewQuery()
	query.AddPattern(Pattern{
		Nodes: []NodePattern{{Variable: "n", Label: "Person"}},
	})
	query.SetWhere(&BinaryExpr{
		Left:     &PropertyAccess{Variable: "n", Property: "age"},
		Operator: ">",
		Right:    &Literal{Value: 28},
	})
	query.AddReturnItem(ReturnItem{
		Expr: &PropertyAccess{Variable: "n", Property: "name"},
	})

	result, err := query.Execute(g)
	require.NoError(t, err)

	assert.Len(t, result.Rows, 2) // Alice (30) and Charlie (35)

	names := make(map[string]bool)
	for _, row := range result.Rows {
		name := row["n.name"].(string)
		names[name] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])
	assert.False(t, names["Bob"])
}

func TestExecute_Expand(t *testing.T) {
	g := createTestGraph(t)

	// MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name
	query := NewQuery()
	query.AddPattern(Pattern{
		Nodes: []NodePattern{
			{Variable: "a", Label: "Person"},
			{Variable: "b"},
		},
		Edges: []EdgePattern{
			{Variable: "r", Type: "KNOWS", Direction: DirectionOut},
		},
	})
	query.AddReturnItem(ReturnItem{Expr: &PropertyAccess{Variable: "a", Property: "name"}})
	query.AddReturnItem(ReturnItem{Expr: &PropertyAccess{Variable: "b", Property: "name"}})

	result, err := query.Execute(g)
	require.NoError(t, err)

	assert.Len(t, result.Rows, 2) // Alice->Bob, Bob->Charlie

	foundAliceBob := false
	foundBobCharlie := false

	for _, row := range result.Rows {
		a := row["a.name"].(string)
		b := row["b.name"].(string)

		if a == "Alice" && b == "Bob" {
			foundAliceBob = true
		}
		if a == "Bob" && b == "Charlie" {
			foundBobCharlie = true
		}
	}

	assert.True(t, foundAliceBob)
	assert.True(t, foundBobCharlie)
}

func TestExecute_Limit(t *testing.T) {
	g := createTestGraph(t)

	// MATCH (n:Person) RETURN n.name LIMIT 2
	query := NewQuery()
	query.AddPattern(Pattern{
		Nodes: []NodePattern{{Variable: "n", Label: "Person"}},
	})
	query.AddReturnItem(ReturnItem{
		Expr: &PropertyAccess{Variable: "n", Property: "name"},
	})
	query.SetLimit(2)

	result, err := query.Execute(g)
	require.NoError(t, err)

	assert.Len(t, result.Rows, 2)
}

func TestExecute_InlineProperties(t *testing.T) {
	g := createTestGraph(t)

	// MATCH (n:Person {name: "Alice"}) RETURN n.age
	query := NewQuery()
	query.AddPattern(Pattern{
		Nodes: []NodePattern{{
			Variable:   "n",
			Label:      "Person",
			Properties: map[string]interface{}{"name": "Alice"},
		}},
	})
	query.AddReturnItem(ReturnItem{
		Expr: &PropertyAccess{Variable: "n", Property: "age"},
	})

	result, err := query.Execute(g)
	require.NoError(t, err)

	assert.Len(t, result.Rows, 1)
	assert.Equal(t, 30, result.Rows[0]["n.age"])
}
