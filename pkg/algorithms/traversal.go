package algorithms

import (
	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/storage"
)

// TraversalResult holds the result of a traversal
type TraversalResult struct {
	VisitedOrder []graph.NodeID
	Path         []graph.NodeID
	Found        bool
	Distance     int
}

// BFS performs Breadth-First Search starting from startNode
// Returns visited nodes in order, or path to target if targetNode is not nil
func BFS(g *storage.Graph, startNode graph.NodeID, targetNode *graph.NodeID, maxDepth int) (*TraversalResult, error) {
	// Check if start node exists
	if _, err := g.GetNode(startNode); err != nil {
		return nil, err
	}

	queue := []graph.NodeID{startNode}
	visited := make(map[graph.NodeID]bool)
	visited[startNode] = true

	// Track parent for path reconstruction: child -> parent
	parentMap := make(map[graph.NodeID]graph.NodeID)

	// Track distance
	distanceMap := make(map[graph.NodeID]int)
	distanceMap[startNode] = 0

	result := &TraversalResult{
		VisitedOrder: []graph.NodeID{},
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		result.VisitedOrder = append(result.VisitedOrder, current)

		dist := distanceMap[current]
		if maxDepth > 0 && dist >= maxDepth {
			continue
		}

		// Check if target found
		if targetNode != nil && current == *targetNode {
			result.Found = true
			result.Distance = dist
			result.Path = reconstructPath(parentMap, startNode, current)
			return result, nil
		}

		neighbors, err := g.GetNeighbors(current)
		if err != nil {
			continue
		}

		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				visited[neighbor.ID] = true
				parentMap[neighbor.ID] = current
				distanceMap[neighbor.ID] = dist + 1
				queue = append(queue, neighbor.ID)
			}
		}
	}

	return result, nil
}

// DFS performs Depth-First Search starting from startNode
func DFS(g *storage.Graph, startNode graph.NodeID, targetNode *graph.NodeID, maxDepth int) (*TraversalResult, error) {
	if _, err := g.GetNode(startNode); err != nil {
		return nil, err
	}

	visited := make(map[graph.NodeID]bool)
	parentMap := make(map[graph.NodeID]graph.NodeID)
	result := &TraversalResult{
		VisitedOrder: []graph.NodeID{},
	}

	found := dfsRecursive(g, startNode, targetNode, maxDepth, 0, visited, parentMap, result)

	if found && targetNode != nil {
		result.Found = true
		// Calculate distance (path length - 1)
		path := reconstructPath(parentMap, startNode, *targetNode)
		result.Path = path
		result.Distance = len(path) - 1
	}

	return result, nil
}

func dfsRecursive(
	g *storage.Graph,
	current graph.NodeID,
	target *graph.NodeID,
	maxDepth int,
	currentDepth int,
	visited map[graph.NodeID]bool,
	parentMap map[graph.NodeID]graph.NodeID,
	result *TraversalResult,
) bool {
	visited[current] = true
	result.VisitedOrder = append(result.VisitedOrder, current)

	if target != nil && current == *target {
		return true
	}

	if maxDepth > 0 && currentDepth >= maxDepth {
		return false
	}

	neighbors, err := g.GetNeighbors(current)
	if err != nil {
		return false
	}

	for _, neighbor := range neighbors {
		if !visited[neighbor.ID] {
			parentMap[neighbor.ID] = current
			if dfsRecursive(g, neighbor.ID, target, maxDepth, currentDepth+1, visited, parentMap, result) {
				return true
			}
		}
	}

	return false
}

func reconstructPath(parentMap map[graph.NodeID]graph.NodeID, start, end graph.NodeID) []graph.NodeID {
	path := []graph.NodeID{end}
	curr := end
	for curr != start {
		parent, ok := parentMap[curr]
		if !ok {
			return nil // Should not happen if path exists
		}
		path = append([]graph.NodeID{parent}, path...)
		curr = parent
	}
	return path
}
