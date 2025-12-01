// Package graph defines core graph data types
package graph

import (
	"sync"
	"time"
)

// NodeID is a unique identifier for a node
type NodeID uint64

// EdgeID is a unique identifier for an edge
type EdgeID uint64

// PropertyValue represents a property value of various types
type PropertyValue interface{}

// Properties is a map of property names to values
type Properties map[string]PropertyValue

// Node represents a vertex in the graph
type Node struct {
	ID         NodeID     `json:"id"`
	Label      string     `json:"label"`      // Node type/label
	Properties Properties `json:"properties"` // Arbitrary properties

	// Adjacency lists for fast traversal
	OutEdges []EdgeID `json:"out_edges"` // Outgoing edges
	InEdges  []EdgeID `json:"in_edges"`  // Incoming edges (for reverse traversal)

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Mu sync.RWMutex `json:"-"` // Protects concurrent access to this node (exported for cross-package use)
}

// Edge represents a relationship between two nodes
type Edge struct {
	ID         EdgeID     `json:"id"`
	Source     NodeID     `json:"source"`     // Source node ID
	Target     NodeID     `json:"target"`     // Target node ID
	Label      string     `json:"label"`      // Edge type/label
	Properties Properties `json:"properties"` // Edge properties

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Mu sync.RWMutex `json:"-"` // Protects concurrent access to this edge (exported for cross-package use)
}

// GetProperty safely retrieves a property from a node
func (n *Node) GetProperty(key string) (PropertyValue, bool) {
	n.Mu.RLock()
	defer n.Mu.RUnlock()
	val, ok := n.Properties[key]
	return val, ok
}

// SetProperty safely sets a property on a node
func (n *Node) SetProperty(key string, value PropertyValue) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	if n.Properties == nil {
		n.Properties = make(Properties)
	}
	n.Properties[key] = value
	n.UpdatedAt = time.Now()
}

// AddOutEdge adds an outgoing edge
func (n *Node) AddOutEdge(edgeID EdgeID) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	n.OutEdges = append(n.OutEdges, edgeID)
	n.UpdatedAt = time.Now()
}

// AddInEdge adds an incoming edge
func (n *Node) AddInEdge(edgeID EdgeID) {
	n.Mu.Lock()
	defer n.Mu.Unlock()
	n.InEdges = append(n.InEdges, edgeID)
	n.UpdatedAt = time.Now()
}

// GetProperty safely retrieves a property from an edge
func (e *Edge) GetProperty(key string) (PropertyValue, bool) {
	e.Mu.RLock()
	defer e.Mu.RUnlock()
	val, ok := e.Properties[key]
	return val, ok
}

// SetProperty safely sets a property on an edge
func (e *Edge) SetProperty(key string, value PropertyValue) {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	if e.Properties == nil {
		e.Properties = make(Properties)
	}
	e.Properties[key] = value
	e.UpdatedAt = time.Now()
}

// NewNode creates a new node with the given label
func NewNode(id NodeID, label string) *Node {
	now := time.Now()
	return &Node{
		ID:         id,
		Label:      label,
		Properties: make(Properties),
		OutEdges:   make([]EdgeID, 0),
		InEdges:    make([]EdgeID, 0),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// NewEdge creates a new edge
func NewEdge(id EdgeID, source, target NodeID, label string) *Edge {
	now := time.Now()
	return &Edge{
		ID:         id,
		Source:     source,
		Target:     target,
		Label:      label,
		Properties: make(Properties),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}
