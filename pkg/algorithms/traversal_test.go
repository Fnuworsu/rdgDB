package algorithms

import (
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestGraph(t *testing.T) *storage.Graph {
	g := storage.NewGraph()

	// Create a simple graph:
	// 1 -> 2 -> 3 -> 4
	// |         ^
	// v         |
	// 5 -> 6 ---+

	// Nodes
	n1, _ := g.AddNode("Node", nil) // ID 1
	n2, _ := g.AddNode("Node", nil) // ID 2
	n3, _ := g.AddNode("Node", nil) // ID 3
	n4, _ := g.AddNode("Node", nil) // ID 4
	n5, _ := g.AddNode("Node", nil) // ID 5
	n6, _ := g.AddNode("Node", nil) // ID 6

	// Edges
	g.AddEdge(n1.ID, n2.ID, "LINK", nil)
	g.AddEdge(n2.ID, n3.ID, "LINK", nil)
	g.AddEdge(n3.ID, n4.ID, "LINK", nil)
	g.AddEdge(n1.ID, n5.ID, "LINK", nil)
	g.AddEdge(n5.ID, n6.ID, "LINK", nil)
	g.AddEdge(n6.ID, n3.ID, "LINK", nil)

	return g
}

func TestBFS_Traversal(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)

	// BFS from 1 should visit all nodes
	result, err := BFS(g, startID, nil, 0)
	require.NoError(t, err)

	assert.Len(t, result.VisitedOrder, 6)
	assert.Contains(t, result.VisitedOrder, graph.NodeID(1))
	assert.Contains(t, result.VisitedOrder, graph.NodeID(6))
}

func TestBFS_ShortestPath(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)
	targetID := graph.NodeID(3) // 1->2->3 (dist 2) or 1->5->6->3 (dist 3)

	result, err := BFS(g, startID, &targetID, 0)
	require.NoError(t, err)

	assert.True(t, result.Found)
	assert.Equal(t, 2, result.Distance)

	// Path should be 1->2->3
	expectedPath := []graph.NodeID{1, 2, 3}
	assert.Equal(t, expectedPath, result.Path)
}

func TestBFS_DepthLimit(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)

	// Depth 1: 1, 2, 5
	result, err := BFS(g, startID, nil, 1)
	require.NoError(t, err)

	// Should visit 1, 2, 5
	assert.Len(t, result.VisitedOrder, 3)
	assert.Contains(t, result.VisitedOrder, graph.NodeID(2))
	assert.Contains(t, result.VisitedOrder, graph.NodeID(5))
	assert.NotContains(t, result.VisitedOrder, graph.NodeID(3))
}

func TestDFS_Traversal(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)

	result, err := DFS(g, startID, nil, 0)
	require.NoError(t, err)

	assert.Len(t, result.VisitedOrder, 6)
}

func TestDFS_PathFinding(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)
	targetID := graph.NodeID(4)

	result, err := DFS(g, startID, &targetID, 0)
	require.NoError(t, err)

	assert.True(t, result.Found)
	assert.Equal(t, graph.NodeID(4), result.Path[len(result.Path)-1])
	assert.Equal(t, graph.NodeID(1), result.Path[0])
}

func TestDFS_DepthLimit(t *testing.T) {
	g := createTestGraph(t)
	startID := graph.NodeID(1)

	// Depth 1 should only reach direct neighbors
	result, err := DFS(g, startID, nil, 1)
	require.NoError(t, err)

	// Should visit 1, 2, 5 (order depends on map iteration, but count is 3)
	assert.Len(t, result.VisitedOrder, 3)
}

func TestAlgorithms_InvalidNode(t *testing.T) {
	g := createTestGraph(t)
	invalidID := graph.NodeID(999)

	_, err := BFS(g, invalidID, nil, 0)
	assert.Error(t, err)

	_, err = DFS(g, invalidID, nil, 0)
	assert.Error(t, err)
}
