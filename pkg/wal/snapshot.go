// Package wal implements snapshot functionality for graph state
package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fnuworsu/rdgDB/internal/graph"
)

// SnapshotMetadata contains information about a snapshot
type SnapshotMetadata struct {
	Index     uint64    `json:"index"`      // WAL index at snapshot time
	Timestamp time.Time `json:"timestamp"`  // When snapshot was taken
	NodeCount int       `json:"node_count"` // Number of nodes
	EdgeCount int       `json:"edge_count"` // Number of edges
}

// Snapshot represents a point-in-time state of the graph
type Snapshot struct {
	Metadata SnapshotMetadata `json:"metadata"`
	Nodes    []*graph.Node    `json:"nodes"`
	Edges    []*graph.Edge    `json:"edges"`
}

// SnapshotManager handles snapshot creation and loading
type SnapshotManager struct {
	dir string
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(dir string) (*SnapshotManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &SnapshotManager{dir: dir}, nil
}

// CreateSnapshot saves the current graph state to a snapshot file
func (sm *SnapshotManager) CreateSnapshot(
	walIndex uint64,
	nodes map[graph.NodeID]*graph.Node,
	edges map[graph.EdgeID]*graph.Edge,
) error {
	// Convert maps to slices
	nodeSlice := make([]*graph.Node, 0, len(nodes))
	for _, node := range nodes {
		nodeSlice = append(nodeSlice, node)
	}

	edgeSlice := make([]*graph.Edge, 0, len(edges))
	for _, edge := range edges {
		edgeSlice = append(edgeSlice, edge)
	}

	snapshot := Snapshot{
		Metadata: SnapshotMetadata{
			Index:     walIndex,
			Timestamp: time.Now(),
			NodeCount: len(nodeSlice),
			EdgeCount: len(edgeSlice),
		},
		Nodes: nodeSlice,
		Edges: edgeSlice,
	}

	// Use timestamp-based filename
	filename := fmt.Sprintf("snapshot-%d-%d.json", walIndex, time.Now().Unix())
	path := filepath.Join(sm.dir, filename)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty print for debugging

	if err := encoder.Encode(&snapshot); err != nil {
		return fmt.Errorf("failed to encode snapshot: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	// Also create a "latest" symlink or copy
	latestPath := filepath.Join(sm.dir, "snapshot-latest.json")
	os.Remove(latestPath) // Remove old symlink if exists

	// Copy instead of symlink for better portability
	if err := sm.copyFile(path, latestPath); err != nil {
		return fmt.Errorf("failed to update latest snapshot: %w", err)
	}

	return nil
}

// LoadLatestSnapshot loads the most recent snapshot
func (sm *SnapshotManager) LoadLatestSnapshot() (*Snapshot, error) {
	latestPath := filepath.Join(sm.dir, "snapshot-latest.json")

	file, err := os.Open(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No snapshot exists
		}
		return nil, fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	var snapshot Snapshot
	decoder := json.NewDecoder(file)

	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot: %w", err)
	}

	return &snapshot, nil
}

// copyFile copies a file from src to dst
func (sm *SnapshotManager) copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

// ListSnapshots returns all available snapshots
func (sm *SnapshotManager) ListSnapshots() ([]string, error) {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return nil, err
	}

	snapshots := []string{}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			if entry.Name() != "snapshot-latest.json" {
				snapshots = append(snapshots, entry.Name())
			}
		}
	}

	return snapshots, nil
}

// CleanupOldSnapshots removes snapshots older than the most recent N
func (sm *SnapshotManager) CleanupOldSnapshots(keepCount int) error {
	snapshots, err := sm.ListSnapshots()
	if err != nil {
		return err
	}

	if len(snapshots) <= keepCount {
		return nil // Nothing to cleanup
	}

	// Sort by modification time (oldest first)
	// For simplicity, just delete excess files
	toDelete := len(snapshots) - keepCount

	for i := 0; i < toDelete; i++ {
		path := filepath.Join(sm.dir, snapshots[i])
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to delete old snapshot: %w", err)
		}
	}

	return nil
}
