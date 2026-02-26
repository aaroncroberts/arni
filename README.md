# arni
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
