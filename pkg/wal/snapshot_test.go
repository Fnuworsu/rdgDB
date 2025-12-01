package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSnapshotManager(t *testing.T) {
	dir := t.TempDir()

	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)
	assert.NotNil(t, sm)

	// Verify directory was created
	_, err = os.Stat(dir)
	assert.NoError(t, err)
}

func TestCreateSnapshot(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	// Create sample graph data
	nodes := map[graph.NodeID]*graph.Node{
		1: graph.NewNode(1, "Person"),
		2: graph.NewNode(2, "Person"),
	}
	nodes[1].SetProperty("name", "Alice")
	nodes[2].SetProperty("name", "Bob")

	edges := map[graph.EdgeID]*graph.Edge{
		1: graph.NewEdge(1, 1, 2, "KNOWS"),
	}

	// Create snapshot
	err = sm.CreateSnapshot(100, nodes, edges)
	require.NoError(t, err)

	// Verify snapshot file was created
	latestPath := filepath.Join(dir, "snapshot-latest.json")
	_, err = os.Stat(latestPath)
	assert.NoError(t, err)
}

func TestLoadLatestSnapshot(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	// Create sample data
	nodes := map[graph.NodeID]*graph.Node{
		1: graph.NewNode(1, "Person"),
		2: graph.NewNode(2, "Person"),
	}
	edges := map[graph.EdgeID]*graph.Edge{
		100: graph.NewEdge(100, 1, 2, "KNOWS"),
	}

	// Create snapshot
	err = sm.CreateSnapshot(50, nodes, edges)
	require.NoError(t, err)

	// Load snapshot
	snapshot, err := sm.LoadLatestSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	assert.Equal(t, uint64(50), snapshot.Metadata.Index)
	assert.Equal(t, 2, snapshot.Metadata.NodeCount)
	assert.Equal(t, 1, snapshot.Metadata.EdgeCount)
	assert.Len(t, snapshot.Nodes, 2)
	assert.Len(t, snapshot.Edges, 1)
}

func TestLoadLatestSnapshot_NoSnapshot(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	// Load when no snapshot exists
	snapshot, err := sm.LoadLatestSnapshot()
	require.NoError(t, err)
	assert.Nil(t, snapshot)
}

func TestSnapshotPreservesData(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	// Create nodes with properties
	nodes := map[graph.NodeID]*graph.Node{
		1: graph.NewNode(1, "Person"),
	}
	nodes[1].SetProperty("name", "Alice")
	nodes[1].SetProperty("age", 30)

	edges := map[graph.EdgeID]*graph.Edge{}

	// Snapshot
	err = sm.CreateSnapshot(10, nodes, edges)
	require.NoError(t, err)

	// Load and verify properties preserved
	snapshot, err := sm.LoadLatestSnapshot()
	require.NoError(t, err)

	assert.Equal(t, graph.NodeID(1), snapshot.Nodes[0].ID)
	assert.Equal(t, "Person", snapshot.Nodes[0].Label)

	// Note: Properties might need type assertion after JSON round-trip
	name, ok := snapshot.Nodes[0].Properties["name"]
	assert.True(t, ok)
	assert.Equal(t, "Alice", name)
}

func TestListSnapshots(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	nodes := map[graph.NodeID]*graph.Node{}
	edges := map[graph.EdgeID]*graph.Edge{}

	// Create multiple snapshots
	sm.CreateSnapshot(1, nodes, edges)
	sm.CreateSnapshot(2, nodes, edges)
	sm.CreateSnapshot(3, nodes, edges)

	// List snapshots
	snapshots, err := sm.ListSnapshots()
	require.NoError(t, err)

	// Should have 3 snapshot files (excluding latest symlink)
	assert.GreaterOrEqual(t, len(snapshots), 3)
}

func TestCleanupOldSnapshots(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	nodes := map[graph.NodeID]*graph.Node{}
	edges := map[graph.EdgeID]*graph.Edge{}

	// Create 5 snapshots
	for i := 1; i <= 5; i++ {
		sm.CreateSnapshot(uint64(i), nodes, edges)
	}

	// Keep only 2 most recent
	err = sm.CleanupOldSnapshots(2)
	require.NoError(t, err)

	snapshots, err := sm.ListSnapshots()
	require.NoError(t, err)

	// Should have at most 2 snapshots now
	assert.LessOrEqual(t, len(snapshots), 2)
}

func TestSnapshotWithLargeGraph(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSnapshotManager(dir)
	require.NoError(t, err)

	// Create a larger graph
	nodes := make(map[graph.NodeID]*graph.Node)
	for i := 1; i <= 100; i++ {
		nodes[graph.NodeID(i)] = graph.NewNode(graph.NodeID(i), "Node")
	}

	edges := make(map[graph.EdgeID]*graph.Edge)
	for i := 1; i < 100; i++ {
		edges[graph.EdgeID(i)] = graph.NewEdge(
			graph.EdgeID(i),
			graph.NodeID(i),
			graph.NodeID(i+1),
			"LINK",
		)
	}

	// Create snapshot
	err = sm.CreateSnapshot(1000, nodes, edges)
	require.NoError(t, err)

	// Load and verify
	snapshot, err := sm.LoadLatestSnapshot()
	require.NoError(t, err)

	assert.Equal(t, 100, snapshot.Metadata.NodeCount)
	assert.Equal(t, 99, snapshot.Metadata.EdgeCount)
	assert.Len(t, snapshot.Nodes, 100)
	assert.Len(t, snapshot.Edges, 99)
}
