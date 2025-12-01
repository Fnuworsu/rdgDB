package storage

import (
	"os"
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPersistentGraph(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	assert.NotNil(t, pg)
	assert.NotNil(t, pg.wal)
	assert.NotNil(t, pg.snapshotManager)
}

func TestPersistentAddNode(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	props := graph.Properties{"name": "Alice"}
	node, err := pg.AddNode("Person", props)
	require.NoError(t, err)

	assert.Equal(t, "Person", node.Label)
	name, _ := node.GetProperty("name")
	assert.Equal(t, "Alice", name)
}

func TestPersistentAddEdge(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	node1, _ := pg.AddNode("Person", nil)
	node2, _ := pg.AddNode("Person", nil)

	edge, err := pg.AddEdge(node1.ID, node2.ID, "KNOWS", nil)
	require.NoError(t, err)

	assert.Equal(t, node1.ID, edge.Source)
	assert.Equal(t, node2.ID, edge.Target)
}

func TestPersistence_Restart(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	// Create graph and add data
	pg1, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)

	node1, _ := pg1.AddNode("Person", graph.Properties{"name": "Alice"})
	node2, _ := pg1.AddNode("Person", graph.Properties{"name": "Bob"})
	pg1.AddEdge(node1.ID, node2.ID, "KNOWS", nil)

	pg1.Close()

	// Reopen and verify data persisted
	pg2, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg2.Close()

	assert.Equal(t, 2, pg2.NodeCount())
	assert.Equal(t, 1, pg2.EdgeCount())

	// Verify nodes recovered
	recoveredNode, err := pg2.GetNode(node1.ID)
	require.NoError(t, err)
	name, _ := recoveredNode.GetProperty("name")
	assert.Equal(t, "Alice", name)
}

func TestSnapshot_Recovery(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	// Create graph, add data, snapshot
	pg1, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		pg1.AddNode("Person", graph.Properties{"id": i})
	}

	// Take snapshot
	err = pg1.Snapshot()
	require.NoError(t, err)

	// Add more data after snapshot
	for i := 6; i <= 10; i++ {
		pg1.AddNode("Person", graph.Properties{"id": i})
	}

	pg1.Close()

	// Recovery should restore all 10 nodes
	pg2, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg2.Close()

	assert.Equal(t, 10, pg2.NodeCount())
}

func TestDeleteOperations_Persistence(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg1, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)

	node1, _ := pg1.AddNode("Person", nil)
	node2, _ := pg1.AddNode("Person", nil)
	edge, _ := pg1.AddEdge(node1.ID, node2.ID, "KNOWS", nil)

	// Delete edge
	err = pg1.DeleteEdge(edge.ID)
	require.NoError(t, err)

	pg1.Close()

	// Verify deletion persisted
	pg2, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg2.Close()

	assert.Equal(t, 2, pg2.NodeCount())
	assert.Equal(t, 0, pg2.EdgeCount())
}

func TestRecovery_EmptyState(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	// Create graph with no data
	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	assert.Equal(t, 0, pg.NodeCount())
	assert.Equal(t, 0, pg.EdgeCount())
}

func TestSnapshot_Truncates_WAL(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	// Add data
	for i := 1; i <= 100; i++ {
		pg.AddNode("Node", nil)
	}

	// Check WAL file size before snapshot
	walPath := walDir + "/wal.log"
	info1, _ := os.Stat(walPath)
	sizeBefore := info1.Size()

	// Snapshot should truncate WAL
	err = pg.Snapshot()
	require.NoError(t, err)

	// Add more data
	for i := 1; i <= 10; i++ {
		pg.AddNode("Node", nil)
	}

	info2, _ := os.Stat(walPath)
	sizeAfter := info2.Size()

	// WAL should be much smaller after truncation
	assert.Less(t, sizeAfter, sizeBefore)
}

func TestConcurrentOperations_WithPersistence(t *testing.T) {
	walDir := t.TempDir()
	snapDir := t.TempDir()

	pg, err := NewPersistentGraph(walDir, snapDir)
	require.NoError(t, err)
	defer pg.Close()

	// Add nodes concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			pg.AddNode("Person", graph.Properties{"id": id})
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	assert.Equal(t, 10, pg.NodeCount())
}
