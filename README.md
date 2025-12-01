# rdgDB - Real-Time Distributed Graph Database

A production-grade, distributed graph database built from scratch in Go, designed for high-throughput real-time streaming data processing with strong consistency guarantees.

## Features

- **High Performance**: Millions of operations per second through sharding and parallelization
- **Strong Consistency**: Raft consensus for linearizable reads and writes
- **Distributed Architecture**: Horizontal scalability with data partitioning
- **High Availability**: Multi-replica deployment with automatic failover
- **SQL-like Query Language**: Familiar syntax with graph pattern matching
- **Built-in Algorithms**: BFS, DFS, shortest path, PageRank, and more
- **Real-time Streaming**: Native support for continuous data ingestion
- **Interactive REPL**: Command-line interface for queries and administration

## Architecture

rdgDB is a **CP system** (Consistent and Partition-tolerant) in CAP terms:
- **Consistency**: Raft consensus ensures all replicas see the same data
- **Partition Tolerance**: System continues operating despite network partitions
- **High Availability**: Achieved through replication (typical 3-5 node clusters per shard)

### Key Components

- **Storage Engine**: In-memory graph with adjacency list representation
- **Consensus Layer**: Raft-based replication for each data shard
- **Query Engine**: SQL-inspired language with graph traversal extensions
- **Coordinator**: Metadata service for cluster topology and shard assignments
- **Streaming Ingestion**: Real-time data pipeline integration

## Project Status

**Under Active Development**

Current phase: **Phase 0 - Project Foundation**

See [task.md](/.gemini/antigravity/brain/a897bb02-cf3f-4acc-8bab-ec7f4f562310/task.md) for detailed progress.

## Getting Started

### Prerequisites

- Go 1.21 or later
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/fnuworsu/rdgDB.git
cd rdgDB

# Download dependencies
go mod download

# Run tests
./scripts/test.sh

# Build the server
go build -o bin/rdgdb-server ./cmd/server

# Build the REPL client
go build -o bin/rdgdb-repl ./cmd/repl
```

### Development Workflow

This project uses feature-based branching:

```bash
# Create a new feature branch
./scripts/branch.sh create feature/my-feature

# Make changes and write tests

# Run tests
./scripts/test.sh

# Merge when tests pass
./scripts/branch.sh merge feature/my-feature
```

## Project Structure

```
rdgDB/
├── cmd/                    # Main applications
│   ├── server/            # Database server
│   └── repl/              # Interactive REPL client
├── pkg/                   # Public libraries
│   ├── storage/          # In-memory storage engine
│   ├── wal/              # Write-ahead log
│   ├── query/            # Query parser & executor
│   ├── algorithms/       # Graph algorithms
│   ├── partition/        # Sharding logic
│   ├── coordinator/      # Cluster metadata
│   ├── consensus/        # Raft integration
│   └── ingest/           # Streaming ingestion
├── internal/             # Private packages
│   ├── graph/           # Core graph types
│   └── util/            # Utilities
├── api/proto/           # gRPC/protobuf definitions
├── scripts/             # Build and deployment scripts
├── tests/               # Integration tests
└── docs/                # Documentation
```

## Testing

```bash
# Run all unit tests
./scripts/test.sh

# Run with race detector
./scripts/test.sh --race

# Run with coverage report
./scripts/test.sh --coverage

# Run integration tests
./scripts/test.sh --integration

# Run benchmarks
./scripts/test.sh --bench
```

## Contributing

This project follows a strict testing policy:
- All features must have comprehensive unit tests
- Integration tests for multi-component interactions
- Minimum 80% code coverage
- All tests must pass before merging

## Roadmap

- [x] Phase 0: Project setup and foundation
- [ ] Phase 1: Core graph storage engine
- [ ] Phase 2: Persistence layer (WAL + snapshots)
- [ ] Phase 3: Query language and execution
- [ ] Phase 4: Graph algorithms
- [ ] Phase 5: Distribution and sharding
- [ ] Phase 6: Raft consensus integration
- [ ] Phase 7: High availability
- [ ] Phase 8: Performance optimization
- [ ] Phase 9: Real-time streaming
- [ ] Phase 10: REPL interface
- [ ] Phase 11: Testing and monitoring
- [ ] Phase 12: Production readiness

## License

MIT License - see LICENSE file for details

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [Property Graph Query Language (PGQL)](https://pgql-lang.org/)
- [Dgraph Architecture](https://dgraph.io/docs/)
- [Graph Database Concepts](https://neo4j.com/developer/graph-database/)
