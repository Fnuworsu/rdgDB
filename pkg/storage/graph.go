// Package storage implements the in-memory graph storage engine
package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fnuworsu/rdgDB/internal/graph"
)

// Graph represents the in-memory graph storage
type Graph struct {
	// Primary indexes
	nodes map[graph.NodeID]*graph.Node
	edges map[graph.EdgeID]*graph.Edge

	// ID generators
	nextNodeID atomic.Uint64
	nextEdgeID atomic.Uint64

	// Locks for thread-safety
	nodesMu sync.RWMutex
	edgesMu sync.RWMutex

	// Optional: Secondary indexes can be added here
	// nodesByLabel map[string][]graph.NodeID
	// edgesByLabel map[string][]graph.EdgeID
}

// NewGraph creates a new in-memory graph storage
func NewGraph() *Graph {
	g := &Graph{
		nodes: make(map[graph.NodeID]*graph.Node),
		edges: make(map[graph.EdgeID]*graph.Edge),
	}
	// Start IDs from 1 (0 can be reserved for null/invalid)
	g.nextNodeID.Store(1)
	g.nextEdgeID.Store(1)
	return g
}

// AddNode creates a new node in the graph
func (g *Graph) AddNode(label string, properties graph.Properties) (*graph.Node, error) {
	nodeID := graph.NodeID(g.nextNodeID.Add(1) - 1)

	node := graph.NewNode(nodeID, label)
	if properties != nil {
		for k, v := range properties {
			node.SetProperty(k, v)
		}
	}

	g.nodesMu.Lock()
	g.nodes[nodeID] = node
	g.nodesMu.Unlock()

	return node, nil
}

// GetNode retrieves a node by ID
func (g *Graph) GetNode(id graph.NodeID) (*graph.Node, error) {
	g.nodesMu.RLock()
	node, exists := g.nodes[id]
	g.nodesMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("node %d not found", id)
	}
	return node, nil
}

// AddEdge creates a new edge between two nodes
func (g *Graph) AddEdge(source, target graph.NodeID, label string, properties graph.Properties) (*graph.Edge, error) {
	// Verify nodes exist
	srcNode, err := g.GetNode(source)
	if err != nil {
		return nil, fmt.Errorf("source node: %w", err)
	}

	tgtNode, err := g.GetNode(target)
	if err != nil {
		return nil, fmt.Errorf("target node: %w", err)
	}

	// Create edge
	edgeID := graph.EdgeID(g.nextEdgeID.Add(1) - 1)
	edge := graph.NewEdge(edgeID, source, target, label)

	if properties != nil {
		for k, v := range properties {
			edge.SetProperty(k, v)
		}
	}

	// Store edge
	g.edgesMu.Lock()
	g.edges[edgeID] = edge
	g.edgesMu.Unlock()

	// Update adjacency lists
	srcNode.AddOutEdge(edgeID)
	tgtNode.AddInEdge(edgeID)

	return edge, nil
}

// GetEdge retrieves an edge by ID
func (g *Graph) GetEdge(id graph.EdgeID) (*graph.Edge, error) {
	g.edgesMu.RLock()
	edge, exists := g.edges[id]
	g.edgesMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("edge %d not found", id)
	}
	return edge, nil
}

// GetNeighbors returns all neighbors of a node (nodes connected by outgoing edges)
func (g *Graph) GetNeighbors(nodeID graph.NodeID) ([]*graph.Node, error) {
	node, err := g.GetNode(nodeID)
	if err != nil {
		return nil, err
	}

	neighbors := make([]*graph.Node, 0, len(node.OutEdges))

	node.Mu.RLock()
	outEdges := make([]graph.EdgeID, len(node.OutEdges))
	copy(outEdges, node.OutEdges)
	node.Mu.RUnlock()

	for _, edgeID := range outEdges {
		edge, err := g.GetEdge(edgeID)
		if err != nil {
			continue // Skip missing edges
		}

		neighbor, err := g.GetNode(edge.Target)
		if err != nil {
			continue // Skip missing nodes
		}

		neighbors = append(neighbors, neighbor)
	}

	return neighbors, nil
}

// GetIncomingNeighbors returns all nodes with edges pointing to the given node
func (g *Graph) GetIncomingNeighbors(nodeID graph.NodeID) ([]*graph.Node, error) {
	node, err := g.GetNode(nodeID)
	if err != nil {
		return nil, err
	}

	neighbors := make([]*graph.Node, 0, len(node.InEdges))

	node.Mu.RLock()
	inEdges := make([]graph.EdgeID, len(node.InEdges))
	copy(inEdges, node.InEdges)
	node.Mu.RUnlock()

	for _, edgeID := range inEdges {
		edge, err := g.GetEdge(edgeID)
		if err != nil {
			continue
		}

		neighbor, err := g.GetNode(edge.Source)
		if err != nil {
			continue
		}

		neighbors = append(neighbors, neighbor)
	}

	return neighbors, nil
}

// NodeCount returns the number of nodes in the graph
func (g *Graph) NodeCount() int {
	g.nodesMu.RLock()
	defer g.nodesMu.RUnlock()
	return len(g.nodes)
}

// EdgeCount returns the number of edges in the graph
func (g *Graph) EdgeCount() int {
	g.edgesMu.RLock()
	defer g.edgesMu.RUnlock()
	return len(g.edges)
}

// DeleteNode removes a node and all its associated edges
func (g *Graph) DeleteNode(id graph.NodeID) error {
	node, err := g.GetNode(id)
	if err != nil {
		return err
	}

	// Delete all outgoing edges
	node.Mu.RLock()
	outEdges := make([]graph.EdgeID, len(node.OutEdges))
	copy(outEdges, node.OutEdges)
	inEdges := make([]graph.EdgeID, len(node.InEdges))
	copy(inEdges, node.InEdges)
	node.Mu.RUnlock()

	for _, edgeID := range outEdges {
		g.DeleteEdge(edgeID)
	}

	for _, edgeID := range inEdges {
		g.DeleteEdge(edgeID)
	}

	// Remove node
	g.nodesMu.Lock()
	delete(g.nodes, id)
	g.nodesMu.Unlock()

	return nil
}

// DeleteEdge removes an edge from the graph
func (g *Graph) DeleteEdge(id graph.EdgeID) error {
	edge, err := g.GetEdge(id)
	if err != nil {
		return err
	}

	// Remove from adjacency lists
	srcNode, _ := g.GetNode(edge.Source)
	if srcNode != nil {
		g.removeOutEdge(srcNode, id)
	}

	tgtNode, _ := g.GetNode(edge.Target)
	if tgtNode != nil {
		g.removeInEdge(tgtNode, id)
	}

	// Delete edge
	g.edgesMu.Lock()
	delete(g.edges, id)
	g.edgesMu.Unlock()

	return nil
}

func (g *Graph) removeOutEdge(node *graph.Node, edgeID graph.EdgeID) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	for i, eid := range node.OutEdges {
		if eid == edgeID {
			node.OutEdges = append(node.OutEdges[:i], node.OutEdges[i+1:]...)
			break
		}
	}
}

func (g *Graph) removeInEdge(node *graph.Node, edgeID graph.EdgeID) {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	for i, eid := range node.InEdges {
		if eid == edgeID {
			node.InEdges = append(node.InEdges[:i], node.InEdges[i+1:]...)
			break
		}
	}
}
