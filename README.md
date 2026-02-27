# arni

> [!IMPORTANT]
> **For Contributors & AI Agents**: All development follows a mandatory two-phase workflow:
> 1. **Planning**: [`.claude/commands/task-generate.md`](.claude/commands/task-generate.md) - Create structured WBS
> 2. **Execution**: [`.claude/commands/task-execute.md`](.claude/commands/task-execute.md) - Implement with context
> 
> **See [`WORKFLOW.md`](WORKFLOW.md) for complete details.**

A Rust library for everything named for Árni Magnússon (1663–1730) who established the largest collection of Norse manuscripts.

## Overview

Arni is a multi-database adapter library for Rust that provides a unified interface for working with various databases (PostgreSQL, MongoDB, Oracle, SQL Server, DuckDB). All query results are returned as Polars DataFrames for easy data analysis and manipulation.

## Project Structure

```
arni/
├── crates/
│   ├── arni/                      # Main library crate
│   │   ├── src/
│   │   │   ├── adapters/          # Database adapter implementations
│   │   │   │   ├── mod.rs         # Adapter module exports
│   │   │   │   └── postgres.rs    # PostgreSQL adapter
│   │   │   ├── config/            # Configuration management
│   │   │   │   └── mod.rs         # Config types and loaders
│   │   │   ├── traits/            # Core traits
│   │   │   │   └── mod.rs         # Connection and DbAdapter traits
│   │   │   ├── error.rs           # Error types
│   │   │   ├── types.rs           # DataFrame wrapper and common types
│   │   │   └── lib.rs             # Library entry point
│   │   ├── tests/
│   │   │   └── integration/       # Integration tests
│   │   └── examples/              # Usage examples
│   └── arni-cli/                  # Command-line interface
│       └── src/
│           └── main.rs
├── docs/                          # Documentation
├── Cargo.toml                     # Workspace configuration
└── README.md
```

## Features

- **Unified Interface**: Common trait-based API for all database adapters
- **DataFrame Results**: All queries return Polars DataFrames
- **Async Support**: Built on Tokio for efficient async operations
- **Configuration Management**: YAML/TOML configuration file support
- **Multiple Databases**: PostgreSQL, MongoDB, Oracle, SQL Server, DuckDB

## Development Status

🚧 **Early Development** - This project is currently in active development. APIs may change.

Currently supported databases:
- ✅ PostgreSQL (in progress)
- ✅ MongoDB (in progress)
- ✅ Oracle (in progress)
- ⏳ SQL Server (planned)
- ⏳ DuckDB (planned)

## Local Development with Databases

The project includes a Docker Compose configuration for running **5 database systems** locally for integration testing:

- **PostgreSQL 16** (alpine) - Port 5432
- **MySQL 8.0** - Port 3306
- **Azure SQL Edge** (SQL Server) - Port 1433
- **Oracle 23ai Free** - Ports 1521/5500
- **MongoDB 7** - Port 27017

Each container includes health checks, persistent data storage, and pre-populated test data.

### Quick Start

```bash
# Start all databases
podman-compose up -d

# Verify containers are running
podman ps

# Run integration tests
cargo test --test '*' -- --ignored

# Stop databases
podman-compose down
```

### Database Initialization Scripts

The `scripts/` directory contains initialization scripts that automatically execute on first container start:

- `init-postgres.sql` - PostgreSQL test schema and sample data
- `init-mysql.sql` - MySQL test schema and sample data
- `init-mssql.sql` - SQL Server test database, schema, and sample data
- `init-oracle.sql` - Oracle test schema with sequences, triggers, and sample data
- `init-mongodb.js` - MongoDB test collection with indexes and sample data

All scripts create a consistent test schema (`users` table/collection) with 5 sample records and appropriate indexes.

For detailed setup instructions, connection details, troubleshooting, and advanced usage, see the **[Local Databases Guide](docs/local-databases.md)**.

## Quick Start

### Development

Use the Makefile for common development tasks:

```bash
make help           # Show all available commands
make build          # Build in debug mode
make test           # Run all tests
make check          # Fast compilation check
make fmt            # Format code
make clippy         # Lint code
make dev            # Watch mode (auto-rebuild on changes)
```

See the [scripts README](scripts/README.md) for more detailed information about individual scripts.

### Common Workflows

```bash
# Quick validation
make check-all      # Format check, lint, compile check, test

# Pre-commit checks
make pre-commit     # Format, fix lints, check, test

# Development with auto-reload
make dev-test       # Watch and run tests on changes

# Full CI pipeline
make ci-check       # Format check, lint, build release, test, coverage
```

## License

MIT
