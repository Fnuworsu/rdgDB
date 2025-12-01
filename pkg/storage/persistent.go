// Package storage implements persistent graph storage with WAL
package storage

import (
	"fmt"
	"sync"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/wal"
)

// PersistentGraph wraps Graph with WAL and snapshot support
type PersistentGraph struct {
	*Graph
	wal             *wal.WAL
	snapshotManager *wal.SnapshotManager
	walEnabled      bool
	mu              sync.RWMutex
}

// NewPersistentGraph creates a new persistent graph with WAL and snapshots
func NewPersistentGraph(walDir, snapshotDir string) (*PersistentGraph, error) {
	g := NewGraph()

	// Initialize WAL
	walLog, err := wal.NewWAL(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Initialize snapshot manager
	snapMgr, err := wal.NewSnapshotManager(snapshotDir)
	if err != nil {
		walLog.Close()
		return nil, fmt.Errorf("failed to create snapshot manager: %w", err)
	}

	pg := &PersistentGraph{
		Graph:           g,
		wal:             walLog,
		snapshotManager: snapMgr,
		walEnabled:      true,
	}

	// Attempt recovery
	if err := pg.Recover(); err != nil {
		return nil, fmt.Errorf("failed to recover: %w", err)
	}

	return pg, nil
}

// AddNode creates a new node and logs to WAL
func (pg *PersistentGraph) AddNode(label string, properties graph.Properties) (*graph.Node, error) {
	node, err := pg.Graph.AddNode(label, properties)
	if err != nil {
		return nil, err
	}

	// Log to WAL
	if pg.walEnabled {
		if err := pg.wal.LogAddNode(node.ID, label, properties); err != nil {
			// Rollback in-memory change
			pg.Graph.DeleteNode(node.ID)
			return nil, fmt.Errorf("failed to log node addition: %w", err)
		}
	}

	return node, nil
}

// AddEdge creates a new edge and logs to WAL
func (pg *PersistentGraph) AddEdge(source, target graph.NodeID, label string, properties graph.Properties) (*graph.Edge, error) {
	edge, err := pg.Graph.AddEdge(source, target, label, properties)
	if err != nil {
		return nil, err
	}

	// Log to WAL
	if pg.walEnabled {
		if err := pg.wal.LogAddEdge(edge.ID, source, target, label, properties); err != nil {
			// Rollback
			pg.Graph.DeleteEdge(edge.ID)
			return nil, fmt.Errorf("failed to log edge addition: %w", err)
		}
	}

	return edge, nil
}

// DeleteNode deletes a node and logs to WAL
func (pg *PersistentGraph) DeleteNode(id graph.NodeID) error {
	if err := pg.Graph.DeleteNode(id); err != nil {
		return err
	}

	// Log to WAL
	if pg.walEnabled {
		if err := pg.wal.LogDeleteNode(id); err != nil {
			return fmt.Errorf("failed to log node deletion: %w", err)
		}
	}

	return nil
}

// DeleteEdge deletes an edge and logs to WAL
func (pg *PersistentGraph) DeleteEdge(id graph.EdgeID) error {
	if err := pg.Graph.DeleteEdge(id); err != nil {
		return err
	}

	// Log to WAL
	if pg.walEnabled {
		if err := pg.wal.LogDeleteEdge(id); err != nil {
			return fmt.Errorf("failed to log edge deletion: %w", err)
		}
	}

	return nil
}

// Snapshot creates a snapshot of the current graph state
func (pg *PersistentGraph) Snapshot() error {
	pg.mu.RLock()
	defer pg.mu.RUnlock()

	// Get current WAL index
	walIndex := pg.wal.GetCurrentIndex()

	// Create snapshot
	if err := pg.snapshotManager.CreateSnapshot(walIndex, pg.nodes, pg.edges); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Truncate WAL up to snapshot point
	if err := pg.wal.Truncate(walIndex); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	return nil
}

// Recover restores graph state from snapshot and WAL
func (pg *PersistentGraph) Recover() error {
	// Disable WAL during recovery to avoid double-logging
	pg.walEnabled = false
	defer func() { pg.walEnabled = true }()

	// Load latest snapshot
	snapshot, err := pg.snapshotManager.LoadLatestSnapshot()
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot != nil {
		// Restore from snapshot
		fmt.Printf("Recovering from snapshot (index %d)...\n", snapshot.Metadata.Index)

		for _, node := range snapshot.Nodes {
			pg.Graph.nodes[node.ID] = node
			if uint64(node.ID) >= pg.Graph.nextNodeID.Load() {
				pg.Graph.nextNodeID.Store(uint64(node.ID) + 1)
			}
		}

		for _, edge := range snapshot.Edges {
			pg.Graph.edges[edge.ID] = edge
			if uint64(edge.ID) >= pg.Graph.nextEdgeID.Load() {
				pg.Graph.nextEdgeID.Store(uint64(edge.ID) + 1)
			}
		}
	}

	// Replay WAL entries after snapshot
	fmt.Println("Replaying WAL...")
	err = pg.wal.Replay(func(entry wal.LogEntry) error {
		return pg.applyWALEntry(entry)
	})

	if err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	fmt.Printf("Recovery complete: %d nodes, %d edges\n", pg.NodeCount(), pg.EdgeCount())
	return nil
}

// applyWALEntry applies a single WAL entry to the graph
func (pg *PersistentGraph) applyWALEntry(entry wal.LogEntry) error {
	switch entry.OpType {
	case wal.OpAddNode:
		nodeID := graph.NodeID(uint64(entry.Data["node_id"].(float64)))
		label := entry.Data["label"].(string)
		props := convertProperties(entry.Data["properties"])

		node := graph.NewNode(nodeID, label)
		for k, v := range props {
			node.SetProperty(k, v)
		}

		pg.Graph.nodes[nodeID] = node
		if uint64(nodeID) >= pg.Graph.nextNodeID.Load() {
			pg.Graph.nextNodeID.Store(uint64(nodeID) + 1)
		}

	case wal.OpAddEdge:
		edgeID := graph.EdgeID(uint64(entry.Data["edge_id"].(float64)))
		source := graph.NodeID(uint64(entry.Data["source"].(float64)))
		target := graph.NodeID(uint64(entry.Data["target"].(float64)))
		label := entry.Data["label"].(string)
		props := convertProperties(entry.Data["properties"])

		edge := graph.NewEdge(edgeID, source, target, label)
		for k, v := range props {
			edge.SetProperty(k, v)
		}

		pg.Graph.edges[edgeID] = edge
		if uint64(edgeID) >= pg.Graph.nextEdgeID.Load() {
			pg.Graph.nextEdgeID.Store(uint64(edgeID) + 1)
		}

		// Update adjacency lists
		if srcNode, ok := pg.Graph.nodes[source]; ok {
			srcNode.AddOutEdge(edgeID)
		}
		if tgtNode, ok := pg.Graph.nodes[target]; ok {
			tgtNode.AddInEdge(edgeID)
		}

	case wal.OpDeleteNode:
		nodeID := graph.NodeID(uint64(entry.Data["node_id"].(float64)))
		pg.Graph.DeleteNode(nodeID)

	case wal.OpDeleteEdge:
		edgeID := graph.EdgeID(uint64(entry.Data["edge_id"].(float64)))
		pg.Graph.DeleteEdge(edgeID)
	}

	return nil
}

// convertProperties converts map[string]interface{} from JSON to graph.Properties
func convertProperties(data interface{}) graph.Properties {
	if data == nil {
		return graph.Properties{}
	}

	props := graph.Properties{}
	if m, ok := data.(map[string]interface{}); ok {
		for k, v := range m {
			props[k] = v
		}
	}
	return props
}

// Close closes WAL and snapshot manager
func (pg *PersistentGraph) Close() error {
	if pg.wal != nil {
		return pg.wal.Close()
	}
	return nil
}
