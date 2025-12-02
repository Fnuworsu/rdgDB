package algorithms

import (
	"math"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/storage"
)

const (
	defaultDampingFactor = 0.85
	defaultIterations    = 20
	defaultTolerance     = 0.0001
)

// PageRankConfig holds configuration for PageRank execution
type PageRankConfig struct {
	DampingFactor float64
	Iterations    int
	Tolerance     float64
}

// DefaultPageRankConfig returns default configuration
func DefaultPageRankConfig() PageRankConfig {
	return PageRankConfig{
		DampingFactor: defaultDampingFactor,
		Iterations:    defaultIterations,
		Tolerance:     defaultTolerance,
	}
}

// PageRank computes the PageRank score for all nodes in the graph
// Returns a map of NodeID -> Score
func PageRank(g *storage.Graph, config PageRankConfig) (map[graph.NodeID]float64, error) {
	nodeCount := g.NodeCount()
	if nodeCount == 0 {
		return make(map[graph.NodeID]float64), nil
	}

	// Initialize scores
	scores := make(map[graph.NodeID]float64)
	initialScore := 1.0 / float64(nodeCount)

	// Get all nodes first to avoid locking repeatedly during iteration
	// For very large graphs, this would need optimization (streaming/chunking)
	var nodes []*graph.Node
	g.IterateNodes(func(n *graph.Node) bool {
		nodes = append(nodes, n)
		scores[n.ID] = initialScore
		return true
	})

	// Pre-calculate outgoing degree for all nodes
	outDegree := make(map[graph.NodeID]int)
	for _, node := range nodes {
		node.Mu.RLock()
		outDegree[node.ID] = len(node.OutEdges)
		node.Mu.RUnlock()
	}

	// Iterative calculation
	for i := 0; i < config.Iterations; i++ {
		newScores := make(map[graph.NodeID]float64)
		diff := 0.0

		// Calculate new score for each node
		for _, node := range nodes {
			// Score from incoming edges
			incomingScore := 0.0

			// We need incoming neighbors to calculate score
			// This is expensive if not indexed.
			// Graph storage has GetIncomingNeighbors which uses InEdges list.
			incoming, err := g.GetIncomingNeighbors(node.ID)
			if err != nil {
				continue
			}

			for _, neighbor := range incoming {
				degree := outDegree[neighbor.ID]
				if degree > 0 {
					incomingScore += scores[neighbor.ID] / float64(degree)
				}
			}

			// Apply damping factor
			newScore := (1.0-config.DampingFactor)/float64(nodeCount) + (config.DampingFactor * incomingScore)
			newScores[node.ID] = newScore

			diff += math.Abs(newScore - scores[node.ID])
		}

		scores = newScores

		// Check convergence
		if diff < config.Tolerance {
			break
		}
	}

	return scores, nil
}
