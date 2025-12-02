package algorithms

import (
	"testing"

	"github.com/fnuworsu/rdgDB/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageRank_Simple(t *testing.T) {
	g := storage.NewGraph()

	// Simple 3-node cycle: A -> B -> C -> A
	n1, _ := g.AddNode("Node", nil)
	n2, _ := g.AddNode("Node", nil)
	n3, _ := g.AddNode("Node", nil)

	g.AddEdge(n1.ID, n2.ID, "LINK", nil)
	g.AddEdge(n2.ID, n3.ID, "LINK", nil)
	g.AddEdge(n3.ID, n1.ID, "LINK", nil)

	scores, err := PageRank(g, DefaultPageRankConfig())
	require.NoError(t, err)

	// In a perfect cycle, all scores should be equal (1/3)
	expected := 1.0 / 3.0
	assert.InDelta(t, expected, scores[n1.ID], 0.001)
	assert.InDelta(t, expected, scores[n2.ID], 0.001)
	assert.InDelta(t, expected, scores[n3.ID], 0.001)
}

func TestPageRank_Star(t *testing.T) {
	g := storage.NewGraph()

	// Star graph: Center (1) pointed to by leaves (2, 3, 4)
	center, _ := g.AddNode("Center", nil)
	l1, _ := g.AddNode("Leaf", nil)
	l2, _ := g.AddNode("Leaf", nil)
	l3, _ := g.AddNode("Leaf", nil)

	g.AddEdge(l1.ID, center.ID, "LINK", nil)
	g.AddEdge(l2.ID, center.ID, "LINK", nil)
	g.AddEdge(l3.ID, center.ID, "LINK", nil)

	scores, err := PageRank(g, DefaultPageRankConfig())
	require.NoError(t, err)

	// Center should have highest score
	assert.True(t, scores[center.ID] > scores[l1.ID])
	assert.True(t, scores[center.ID] > scores[l2.ID])
	assert.True(t, scores[center.ID] > scores[l3.ID])
}

func TestPageRank_Disconnected(t *testing.T) {
	g := storage.NewGraph()

	n1, _ := g.AddNode("Node", nil)
	n2, _ := g.AddNode("Node", nil)

	scores, err := PageRank(g, DefaultPageRankConfig())
	require.NoError(t, err)

	// With damping factor 0.85, nodes with no incoming edges converge to (1-d)/N
	// (1 - 0.85) / 2 = 0.075
	expected := (1.0 - 0.85) / 2.0
	assert.InDelta(t, expected, scores[n1.ID], 0.001)
	assert.InDelta(t, expected, scores[n2.ID], 0.001)
}

func TestPageRank_Empty(t *testing.T) {
	g := storage.NewGraph()
	scores, err := PageRank(g, DefaultPageRankConfig())
	require.NoError(t, err)
	assert.Empty(t, scores)
}
