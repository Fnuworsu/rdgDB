package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWAL(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	assert.NotNil(t, wal)
	assert.Equal(t, uint64(1), wal.nextIndex)

	// Verify log file exists
	logPath := filepath.Join(dir, "wal.log")
	_, err = os.Stat(logPath)
	assert.NoError(t, err)
}

func TestLogAddNode(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	props := graph.Properties{"name": "Alice", "age": 30}
	err = wal.LogAddNode(graph.NodeID(1), "Person", props)
	require.NoError(t, err)
}

func TestLogAddEdge(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	props := graph.Properties{"since": 2020}
	err = wal.LogAddEdge(graph.EdgeID(100), graph.NodeID(1), graph.NodeID(2), "KNOWS", props)
	require.NoError(t, err)
}

func TestReplay(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)

	// Log some operations
	wal.LogAddNode(graph.NodeID(1), "Person", graph.Properties{"name": "Alice"})
	wal.LogAddNode(graph.NodeID(2), "Person", graph.Properties{"name": "Bob"})
	wal.LogAddEdge(graph.EdgeID(1), graph.NodeID(1), graph.NodeID(2), "KNOWS", nil)

	wal.Close()

	// Replay the log
	wal2, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal2.Close()

	var entries []LogEntry
	err = wal2.Replay(func(entry LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, entries, 3)
	assert.Equal(t, OpAddNode, entries[0].OpType)
	assert.Equal(t, OpAddNode, entries[1].OpType)
	assert.Equal(t, OpAddEdge, entries[2].OpType)

	// Verify next index is correct
	assert.Equal(t, uint64(4), wal2.nextIndex)
}

func TestMultipleOperations(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	// Add multiple nodes
	for i := 1; i <= 10; i++ {
		err := wal.LogAddNode(graph.NodeID(i), "Person", nil)
		require.NoError(t, err)
	}

	// Replay and count
	entries := []LogEntry{}
	err = wal.Replay(func(entry LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, entries, 10)
	for i, entry := range entries {
		assert.Equal(t, uint64(i+1), entry.Index)
		assert.Equal(t, OpAddNode, entry.OpType)
	}
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)

	// Add 10 entries
	for i := 1; i <= 10; i++ {
		wal.LogAddNode(graph.NodeID(i), "Person", nil)
	}

	// Truncate before index 6 (keep entries 6-10)
	err = wal.Truncate(6)
	require.NoError(t, err)

	// Replay and verify only 5 entries remain
	entries := []LogEntry{}
	err = wal.Replay(func(entry LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, entries, 5)
	assert.Equal(t, uint64(6), entries[0].Index)
	assert.Equal(t, uint64(10), entries[4].Index)

	wal.Close()
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create WAL and write data
	wal1, err := NewWAL(dir)
	require.NoError(t, err)

	wal1.LogAddNode(graph.NodeID(1), "Person", graph.Properties{"name": "Test"})
	wal1.Close()

	// Reopen and verify data persisted
	wal2, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal2.Close()

	entries := []LogEntry{}
	err = wal2.Replay(func(entry LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, entries, 1)
	assert.Equal(t, "Test", entries[0].Data["properties"].(map[string]interface{})["name"])
}

func TestDeleteOperations(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir)
	require.NoError(t, err)
	defer wal.Close()

	// Log deletions
	err = wal.LogDeleteNode(graph.NodeID(1))
	require.NoError(t, err)

	err = wal.LogDeleteEdge(graph.EdgeID(100))
	require.NoError(t, err)

	// Replay and verify
	entries := []LogEntry{}
	err = wal.Replay(func(entry LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, entries, 2)
	assert.Equal(t, OpDeleteNode, entries[0].OpType)
	assert.Equal(t, OpDeleteEdge, entries[1].OpType)
}
