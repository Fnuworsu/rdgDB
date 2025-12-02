package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fnuworsu/rdgDB/internal/graph"
	"github.com/fnuworsu/rdgDB/pkg/query"
	"github.com/fnuworsu/rdgDB/pkg/storage"
)

const banner = `
╔═══════════════════════════════════════════╗
║   rdgDB - Graph Database REPL             ║
║   Version: 0.3.0 (Phase 3)                ║
╚═══════════════════════════════════════════╝
`

const (
	defaultDataDir = "./data"
)

func main() {
	fmt.Print(banner)

	// Initialize storage
	dataDir := os.Getenv("RDGDB_DATA_DIR")
	if dataDir == "" {
		dataDir = defaultDataDir
	}
	walDir := filepath.Join(dataDir, "wal")
	snapshotDir := filepath.Join(dataDir, "snapshots")

	fmt.Printf("Initializing storage at %s...\n", dataDir)
	g, err := storage.NewPersistentGraph(walDir, snapshotDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize graph: %v\n", err)
		os.Exit(1)
	}
	defer g.Close()

	fmt.Printf("✓ Connected to graph: %d nodes, %d edges\n", g.NodeCount(), g.EdgeCount())
	fmt.Println("Type 'help' for available commands, 'exit' to quit")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("rdgDB> ")

		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}

		if processCommand(input, g) {
			break // exit requested
		}
	}

	fmt.Println("Goodbye!")
}

func processCommand(cmd string, g *storage.PersistentGraph) bool {
	// Handle meta-commands
	if strings.HasPrefix(strings.ToLower(cmd), "exit") ||
		strings.HasPrefix(strings.ToLower(cmd), "quit") ||
		cmd == "q" {
		return true
	}

	if cmd == "help" || cmd == "?" {
		printHelp()
		return false
	}

	if cmd == "status" {
		printStatus(g)
		return false
	}

	if cmd == "seed" {
		seedData(g)
		return false
	}

	// Treat as query
	executeQuery(cmd, g)
	return false
}

func seedData(g *storage.PersistentGraph) {
	fmt.Println("Seeding database with test data...")

	// Create Nodes
	alice, _ := g.AddNode("Person", graph.Properties{"name": "Alice", "age": 30, "city": "New York"})
	bob, _ := g.AddNode("Person", graph.Properties{"name": "Bob", "age": 25, "city": "San Francisco"})
	charlie, _ := g.AddNode("Person", graph.Properties{"name": "Charlie", "age": 35, "city": "London"})
	google, _ := g.AddNode("Company", graph.Properties{"name": "Google", "hq": "Mountain View"})

	// Create Edges
	g.AddEdge(alice.ID, bob.ID, "KNOWS", graph.Properties{"since": 2020})
	g.AddEdge(bob.ID, charlie.ID, "KNOWS", nil)
	g.AddEdge(alice.ID, google.ID, "WORKS_AT", graph.Properties{"role": "Engineer"})
	g.AddEdge(bob.ID, google.ID, "WORKS_AT", graph.Properties{"role": "Designer"})

	fmt.Println("✓ Created 4 nodes and 4 edges")
}

func executeQuery(input string, g *storage.PersistentGraph) {
	start := time.Now()

	// 1. Parse
	parser := query.NewParser(input)
	q, err := parser.Parse()
	if err != nil {
		fmt.Printf("Parse Error: %v\n", err)
		return
	}

	// 2. Execute
	// Pass the underlying in-memory graph to the executor
	result, err := q.Execute(g.Graph)
	if err != nil {
		fmt.Printf("Execution Error: %v\n", err)
		return
	}

	duration := time.Since(start)

	// 3. Print Results
	printResult(result)
	fmt.Printf("\n(%d rows, %s)\n", len(result.Rows), duration)
}

func printResult(res *query.Result) {
	if len(res.Rows) == 0 {
		fmt.Println("(no rows)")
		return
	}

	// Calculate column widths
	widths := make([]int, len(res.Columns))
	for i, col := range res.Columns {
		widths[i] = len(col)
	}

	for _, row := range res.Rows {
		for i, col := range res.Columns {
			val := fmt.Sprintf("%v", row[col])
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Print Header
	for i, col := range res.Columns {
		fmt.Printf("%-*s  ", widths[i], col)
	}
	fmt.Println()

	// Print Separator
	for i := range res.Columns {
		fmt.Print(strings.Repeat("-", widths[i]) + "  ")
	}
	fmt.Println()

	// Print Rows
	for _, row := range res.Rows {
		for i, col := range res.Columns {
			val := fmt.Sprintf("%v", row[col])
			fmt.Printf("%-*s  ", widths[i], val)
		}
		fmt.Println()
	}
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  help, ?       - Show this help message")
	fmt.Println("  status        - Show database status")
	fmt.Println("  exit, quit, q - Exit the REPL")
	fmt.Println()
	fmt.Println("Query Examples:")
	fmt.Println("  MATCH (n:Person) RETURN n.name")
	fmt.Println("  MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name")
}

func printStatus(g *storage.PersistentGraph) {
	fmt.Printf("Nodes: %d\n", g.NodeCount())
	fmt.Printf("Edges: %d\n", g.EdgeCount())
	fmt.Println("Storage: Persistent (WAL + Snapshots)")
}
