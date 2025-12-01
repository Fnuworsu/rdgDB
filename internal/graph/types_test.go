package graph

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNode(t *testing.T) {
	nodeID := NodeID(1)
	label := "Person"

	node := NewNode(nodeID, label)

	assert.Equal(t, nodeID, node.ID)
	assert.Equal(t, label, node.Label)
	assert.NotNil(t, node.Properties)
	assert.NotNil(t, node.OutEdges)
	assert.NotNil(t, node.InEdges)
	assert.False(t, node.CreatedAt.IsZero())
	assert.False(t, node.UpdatedAt.IsZero())
}

func TestNodeProperties(t *testing.T) {
	node := NewNode(1, "Person")

	// Test setting property
	node.SetProperty("name", "Alice")
	node.SetProperty("age", 30)

	// Test getting property
	name, ok := node.GetProperty("name")
	require.True(t, ok)
	assert.Equal(t, "Alice", name)

	age, ok := node.GetProperty("age")
	require.True(t, ok)
	assert.Equal(t, 30, age)

	// Test non-existent property
	_, ok = node.GetProperty("nonexistent")
	assert.False(t, ok)
}

func TestNodeEdges(t *testing.T) {
	node := NewNode(1, "Person")

	// Add outgoing edges
	node.AddOutEdge(EdgeID(100))
	node.AddOutEdge(EdgeID(101))

	assert.Len(t, node.OutEdges, 2)
	assert.Contains(t, node.OutEdges, EdgeID(100))
	assert.Contains(t, node.OutEdges, EdgeID(101))

	// Add incoming edges
	node.AddInEdge(EdgeID(200))

	assert.Len(t, node.InEdges, 1)
	assert.Contains(t, node.InEdges, EdgeID(200))
}

func TestNewEdge(t *testing.T) {
	edgeID := EdgeID(100)
	source := NodeID(1)
	target := NodeID(2)
	label := "KNOWS"

	edge := NewEdge(edgeID, source, target, label)

	assert.Equal(t, edgeID, edge.ID)
	assert.Equal(t, source, edge.Source)
	assert.Equal(t, target, edge.Target)
	assert.Equal(t, label, edge.Label)
	assert.NotNil(t, edge.Properties)
	assert.False(t, edge.CreatedAt.IsZero())
}

func TestEdgeProperties(t *testing.T) {
	edge := NewEdge(1, 10, 20, "KNOWS")

	// Set properties
	edge.SetProperty("since", 2020)
	edge.SetProperty("strength", 0.95)

	// Get properties
	since, ok := edge.GetProperty("since")
	require.True(t, ok)
	assert.Equal(t, 2020, since)

	strength, ok := edge.GetProperty("strength")
	require.True(t, ok)
	assert.Equal(t, 0.95, strength)
}

func TestConcurrentNodeAccess(t *testing.T) {
	node := NewNode(1, "Person")

	// Simulate concurrent writes
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(val int) {
			node.SetProperty("count", val)
			node.AddOutEdge(EdgeID(val))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify no race conditions (test should pass with -race flag)
	assert.NotNil(t, node)
	assert.Len(t, node.OutEdges, 10)
}

func TestUpdateTimestamp(t *testing.T) {
	node := NewNode(1, "Person")
	originalTime := node.UpdatedAt

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	node.SetProperty("name", "Bob")

	assert.True(t, node.UpdatedAt.After(originalTime))
}
