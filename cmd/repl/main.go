package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const banner = `
╔═══════════════════════════════════════════╗
║   rdgDB - Graph Database REPL             ║
║   Version: 0.1.0 (Development)            ║
╚═══════════════════════════════════════════╝
`

func main() {
	fmt.Print(banner)
	fmt.Println("Type 'help' for available commands, 'exit' to quit")

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

		if processCommand(input) {
			break // exit requested
		}
	}

	fmt.Println("Goodbye!")
}

func processCommand(cmd string) bool {
	cmd = strings.ToLower(cmd)

	switch cmd {
	case "exit", "quit", "q":
		return true

	case "help", "?":
		printHelp()

	case "version":
		fmt.Println("rdgDB version 0.1.0 (Development)")

	case "status":
		fmt.Println("Status: Not connected to server")
		fmt.Println("Phase: 1 - Core Storage Engine (Complete)")

	default:
		// TODO: Connect to server and execute query
		fmt.Printf("Query execution not yet implemented: %s\n", cmd)
		fmt.Println("Hint: Use 'help' to see available commands")
	}

	return false
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  help, ?       - Show this help message")
	fmt.Println("  version       - Show version information")
	fmt.Println("  status        - Show server status")
	fmt.Println("  exit, quit, q - Exit the REPL")
	fmt.Println()
	fmt.Println("Query execution coming in Phase 3!")
}
