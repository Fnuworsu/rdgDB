package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fnuworsu/rdgDB/pkg/storage"
)

const (
	defaultDataDir     = "./data"
	defaultWALDir      = "./data/wal"
	defaultSnapshotDir = "./data/snapshots"
)

func main() {
	fmt.Println("rdgDB Server - Real-Time Distributed Graph Database")
	fmt.Println("====================================================")

	// Get data directory from environment or use default
	dataDir := os.Getenv("RDGDB_DATA_DIR")
	if dataDir == "" {
		dataDir = defaultDataDir
	}

	walDir := filepath.Join(dataDir, "wal")
	snapshotDir := filepath.Join(dataDir, "snapshots")

	fmt.Printf("Data directory: %s\n", dataDir)
	fmt.Printf("WAL directory: %s\n", walDir)
	fmt.Printf("Snapshot directory: %s\n\n", snapshotDir)

	// Initialize the persistent graph storage (recovers from disk if exists)
	fmt.Println("Initializing graph storage...")
	graph, err := storage.NewPersistentGraph(walDir, snapshotDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize graph: %v\n", err)
		os.Exit(1)
	}
	defer graph.Close()

	fmt.Printf("✓ Graph storage ready: %d nodes, %d edges\n",
		graph.NodeCount(), graph.EdgeCount())
	fmt.Println()

	// Set up graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start periodic snapshotting in background
	go func() {
		ticker := time.NewTicker(5 * time.Minute) // Snapshot every 5 minutes
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				fmt.Println("Creating periodic snapshot...")
				if err := graph.Snapshot(); err != nil {
					fmt.Fprintf(os.Stderr, "Snapshot failed: %v\n", err)
				} else {
					fmt.Println("✓ Snapshot complete")
				}
			case <-sigCh:
				return
			}
		}
	}()

	// TODO: Add server initialization
	// - gRPC server setup
	// - Raft consensus initialization
	// - Cluster coordinator
	// - Query engine

	fmt.Println("Server ready (Phase 2: Persistent storage)")
	fmt.Println("Data persists across restarts - clients can continue existing sessions")
	fmt.Println("Press Ctrl+C to shutdown gracefully")
	fmt.Println()

	// Wait for shutdown signal
	<-sigCh

	fmt.Println("\nShutdown signal received, creating final snapshot...")
	if err := graph.Snapshot(); err != nil {
		fmt.Fprintf(os.Stderr, "Final snapshot failed: %v\n", err)
	} else {
		fmt.Println("✓ Final snapshot complete")
	}

	fmt.Println("Server shutdown complete")
}
