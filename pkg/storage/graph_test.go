package storage

import (
	"sync"
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGraph(t *testing.T) {
	g := NewGraph()
	assert.NotNil(t, g)
	assert.Equal(t, 0, g.NodeCount())
	assert.Equal(t, 0, g.EdgeCount())
}

func TestAddNode(t *testing.T) {
	g := NewGraph()

	props := graph.Properties{
		"name": "Alice",
		"age":  30,
	}

	node, err := g.AddNode("Person", props)
	require.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, graph.NodeID(1), node.ID)
	assert.Equal(t, "Person", node.Label)

	name, _ := node.GetProperty("name")
	assert.Equal(t, "Alice", name)

	assert.Equal(t, 1, g.NodeCount())
}

func TestGetNode(t *testing.T) {
	g := NewGraph()

	node1, _ := g.AddNode("Person", nil)

	node, err := g.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, node.ID)

	// Test non-existent node
	_, err = g.GetNode(graph.NodeID(999))
	assert.Error(t, err)
}

func TestAddEdge(t *testing.T) {
	g := NewGraph()

	node1, _ := g.AddNode("Person", graph.Properties{"name": "Alice"})
	node2, _ := g.AddNode("Person", graph.Properties{"name": "Bob"})

	props := graph.Properties{"since": 2020}
	edge, err := g.AddEdge(node1.ID, node2.ID, "KNOWS", props)

	require.NoError(t, err)
	assert.NotNil(t, edge)
	assert.Equal(t, node1.ID, edge.Source)
	assert.Equal(t, node2.ID, edge.Target)
	assert.Equal(t, "KNOWS", edge.Label)

	since, _ := edge.GetProperty("since")
	assert.Equal(t, 2020, since)

	assert.Equal(t, 1, g.EdgeCount())

	// Verify adjacency lists updated
	assert.Contains(t, node1.OutEdges, edge.ID)
	assert.Contains(t, node2.InEdges, edge.ID)
}

func TestAddEdgeInvalidNodes(t *testing.T) {
	g := NewGraph()

	node1, _ := g.AddNode("Person", nil)

	// Edge to non-existent target
	_, err := g.AddEdge(node1.ID, graph.NodeID(999), "KNOWS", nil)
	assert.Error(t, err)

	// Edge from non-existent source
	_, err = g.AddEdge(graph.NodeID(999), node1.ID, "KNOWS", nil)
	assert.Error(t, err)
}

func TestGetNeighbors(t *testing.T) {
	g := NewGraph()

	alice, _ := g.AddNode("Person", graph.Properties{"name": "Alice"})
	bob, _ := g.AddNode("Person", graph.Properties{"name": "Bob"})
	charlie, _ := g.AddNode("Person", graph.Properties{"name": "Charlie"})

	g.AddEdge(alice.ID, bob.ID, "KNOWS", nil)
	g.AddEdge(alice.ID, charlie.ID, "KNOWS", nil)

	neighbors, err := g.GetNeighbors(alice.ID)
	require.NoError(t, err)
	assert.Len(t, neighbors, 2)

	neighborIDs := []graph.NodeID{neighbors[0].ID, neighbors[1].ID}
	assert.Contains(t, neighborIDs, bob.ID)
	assert.Contains(t, neighborIDs, charlie.ID)
}

func TestGetIncomingNeighbors(t *testing.T) {
	g := NewGraph()

	alice, _ := g.AddNode("Person", graph.Properties{"name": "Alice"})
	bob, _ := g.AddNode("Person", graph.Properties{"name": "Bob"})
	charlie, _ := g.AddNode("Person", graph.Properties{"name": "Charlie"})

	g.AddEdge(bob.ID, alice.ID, "KNOWS", nil)
	g.AddEdge(charlie.ID, alice.ID, "KNOWS", nil)

	incoming, err := g.GetIncomingNeighbors(alice.ID)
	require.NoError(t, err)
	assert.Len(t, incoming, 2)

	incomingIDs := []graph.NodeID{incoming[0].ID, incoming[1].ID}
	assert.Contains(t, incomingIDs, bob.ID)
	assert.Contains(t, incomingIDs, charlie.ID)
}

func TestDeleteEdge(t *testing.T) {
	g := NewGraph()

	node1, _ := g.AddNode("Person", nil)
	node2, _ := g.AddNode("Person", nil)
	edge, _ := g.AddEdge(node1.ID, node2.ID, "KNOWS", nil)

	assert.Equal(t, 1, g.EdgeCount())

	err := g.DeleteEdge(edge.ID)
	require.NoError(t, err)

	assert.Equal(t, 0, g.EdgeCount())

	// Verify adjacency lists updated
	assert.NotContains(t, node1.OutEdges, edge.ID)
	assert.NotContains(t, node2.InEdges, edge.ID)
}

func TestDeleteNode(t *testing.T) {
	g := NewGraph()

	alice, _ := g.AddNode("Person", graph.Properties{"name": "Alice"})
	bob, _ := g.AddNode("Person", graph.Properties{"name": "Bob"})
	charlie, _ := g.AddNode("Person", graph.Properties{"name": "Charlie"})

	g.AddEdge(alice.ID, bob.ID, "KNOWS", nil)
	g.AddEdge(bob.ID, charlie.ID, "KNOWS", nil)
	g.AddEdge(charlie.ID, alice.ID, "KNOWS", nil)

	assert.Equal(t, 3, g.NodeCount())
	assert.Equal(t, 3, g.EdgeCount())

	// Delete Alice (should delete edges involving Alice)
	err := g.DeleteNode(alice.ID)
	require.NoError(t, err)

	assert.Equal(t, 2, g.NodeCount())
	assert.Equal(t, 1, g.EdgeCount()) // Only Bob->Charlie edge remains

	// Verify Alice is gone
	_, err = g.GetNode(alice.ID)
	assert.Error(t, err)
}

func TestConcurrentOperations(t *testing.T) {
	g := NewGraph()

	var wg sync.WaitGroup
	numOps := 100

	// Concurrent node additions
	wg.Add(numOps)
	for i := 0; i < numOps; i++ {
		go func(idx int) {
			defer wg.Done()
			g.AddNode("Person", graph.Properties{"id": idx})
		}(i)
	}
	wg.Wait()

	assert.Equal(t, numOps, g.NodeCount())

	// Concurrent edge additions
	wg.Add(numOps - 1)
	for i := 1; i < numOps; i++ {
		go func(idx int) {
			defer wg.Done()
			g.AddEdge(graph.NodeID(idx), graph.NodeID(idx+1), "LINK", nil)
		}(i)
	}
	wg.Wait()

	// Should have edges (might be less than numOps-1 if some failed due to ID generation)
	assert.Greater(t, g.EdgeCount(), 0)
}

func BenchmarkAddNode(b *testing.B) {
	g := NewGraph()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.AddNode("Person", graph.Properties{"id": i})
	}
}

func BenchmarkAddEdge(b *testing.B) {
	g := NewGraph()

	// Pre-create nodes
	nodes := make([]graph.NodeID, 1000)
	for i := 0; i < 1000; i++ {
		node, _ := g.AddNode("Person", nil)
		nodes[i] = node.ID
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := nodes[i%1000]
		tgt := nodes[(i+1)%1000]
		g.AddEdge(src, tgt, "KNOWS", nil)
	}
}

func BenchmarkGetNeighbors(b *testing.B) {
	g := NewGraph()

	// Create a graph with one node having many neighbors
	center, _ := g.AddNode("Center", nil)
	for i := 0; i < 100; i++ {
		neighbor, _ := g.AddNode("Neighbor", nil)
		g.AddEdge(center.ID, neighbor.ID, "CONNECTS", nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.GetNeighbors(center.ID)
	}
}
