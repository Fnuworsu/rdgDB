package main

import (
	"fmt"

	"github.com/fnuworsu/rdgDB/pkg/storage"
)

func main() {
	fmt.Println("rdgDB Server - Real-Time Distributed Graph Database")
	fmt.Println("====================================================")
	fmt.Println("Status: Development Mode")
	fmt.Println()

	// Initialize the graph storage
	graph := storage.NewGraph()
	fmt.Printf("Graph storage initialized: %d nodes, %d edges\n",
		graph.NodeCount(), graph.EdgeCount())

	// TODO: Add server initialization
	// - gRPC server setup
	// - Raft consensus initialization
	// - Cluster coordinator
	// - Query engine

	fmt.Println("\nServer ready (Phase 1: Core storage only)")
	fmt.Println("Press Ctrl+C to exit")

	// Block forever (will add proper server loop later)
	select {}
}
