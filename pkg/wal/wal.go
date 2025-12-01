// Package wal implements a Write-Ahead Log for durability
package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fnuworsu/rdgDB/internal/graph"
)

// OpType represents the type of operation in the WAL
type OpType string

const (
	OpAddNode     OpType = "ADD_NODE"
	OpAddEdge     OpType = "ADD_EDGE"
	OpDeleteNode  OpType = "DELETE_NODE"
	OpDeleteEdge  OpType = "DELETE_EDGE"
	OpSetNodeProp OpType = "SET_NODE_PROP"
	OpSetEdgeProp OpType = "SET_EDGE_PROP"
)

// LogEntry represents a single entry in the WAL
type LogEntry struct {
	Index     uint64                 `json:"index"`
	Timestamp time.Time              `json:"timestamp"`
	OpType    OpType                 `json:"op_type"`
	Data      map[string]interface{} `json:"data"`
}

// WAL represents the write-ahead log
type WAL struct {
	dir       string
	file      *os.File
	encoder   *json.Encoder
	nextIndex uint64
	mu        sync.Mutex
}

// NewWAL creates a new write-ahead log
func NewWAL(dir string) (*WAL, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open or create the log file
	logPath := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		dir:       dir,
		file:      file,
		encoder:   json.NewEncoder(file),
		nextIndex: 1,
	}

	// Determine next index by reading existing entries
	if err := wal.loadLastIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to load last index: %w", err)
	}

	return wal, nil
}

// loadLastIndex scans the log to find the last index
func (w *WAL) loadLastIndex() error {
	// Reopen file for reading
	readFile, err := os.Open(filepath.Join(w.dir, "wal.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // New log file
		}
		return err
	}
	defer readFile.Close()

	decoder := json.NewDecoder(readFile)
	var lastIndex uint64 = 0

	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode entry: %w", err)
		}
		if entry.Index > lastIndex {
			lastIndex = entry.Index
		}
	}

	w.nextIndex = lastIndex + 1
	return nil
}

// Append adds a new entry to the WAL
func (w *WAL) Append(opType OpType, data map[string]interface{}) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := LogEntry{
		Index:     w.nextIndex,
		Timestamp: time.Now(),
		OpType:    opType,
		Data:      data,
	}

	if err := w.encoder.Encode(&entry); err != nil {
		return 0, fmt.Errorf("failed to encode entry: %w", err)
	}

	// Flush to disk (fsync for durability)
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync WAL: %w", err)
	}

	index := w.nextIndex
	w.nextIndex++
	return index, nil
}

// LogAddNode logs a node addition
func (w *WAL) LogAddNode(nodeID graph.NodeID, label string, properties graph.Properties) error {
	data := map[string]interface{}{
		"node_id":    nodeID,
		"label":      label,
		"properties": properties,
	}
	_, err := w.Append(OpAddNode, data)
	return err
}

// LogAddEdge logs an edge addition
func (w *WAL) LogAddEdge(edgeID graph.EdgeID, source, target graph.NodeID, label string, properties graph.Properties) error {
	data := map[string]interface{}{
		"edge_id":    edgeID,
		"source":     source,
		"target":     target,
		"label":      label,
		"properties": properties,
	}
	_, err := w.Append(OpAddEdge, data)
	return err
}

// LogDeleteNode logs a node deletion
func (w *WAL) LogDeleteNode(nodeID graph.NodeID) error {
	data := map[string]interface{}{
		"node_id": nodeID,
	}
	_, err := w.Append(OpDeleteNode, data)
	return err
}

// LogDeleteEdge logs an edge deletion
func (w *WAL) LogDeleteEdge(edgeID graph.EdgeID) error {
	data := map[string]interface{}{
		"edge_id": edgeID,
	}
	_, err := w.Append(OpDeleteEdge, data)
	return err
}

// Replay reads all entries from the WAL and calls the handler for each
func (w *WAL) Replay(handler func(entry LogEntry) error) error {
	readFile, err := os.Open(filepath.Join(w.dir, "wal.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No log to replay
		}
		return err
	}
	defer readFile.Close()

	decoder := json.NewDecoder(readFile)

	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode entry during replay: %w", err)
		}

		if err := handler(entry); err != nil {
			return fmt.Errorf("handler failed for entry %d: %w", entry.Index, err)
		}
	}

	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Truncate removes all entries before the given index (used after snapshotting)
func (w *WAL) Truncate(beforeIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read all entries after beforeIndex
	readFile, err := os.Open(filepath.Join(w.dir, "wal.log"))
	if err != nil {
		return err
	}

	var entriesToKeep []LogEntry
	decoder := json.NewDecoder(readFile)

	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			readFile.Close()
			return err
		}

		if entry.Index >= beforeIndex {
			entriesToKeep = append(entriesToKeep, entry)
		}
	}
	readFile.Close()

	// Close current file
	if err := w.file.Close(); err != nil {
		return err
	}

	// Rewrite the log with only entries to keep
	logPath := filepath.Join(w.dir, "wal.log")
	file, err := os.OpenFile(logPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.encoder = json.NewEncoder(file)

	// Write retained entries
	for _, entry := range entriesToKeep {
		if err := w.encoder.Encode(&entry); err != nil {
			return err
		}
	}

	return w.file.Sync()
}

// GetCurrentIndex returns the current WAL index
func (w *WAL) GetCurrentIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextIndex - 1
}
