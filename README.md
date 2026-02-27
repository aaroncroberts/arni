<!-- markdownlint-disable MD041 -->
```text
    _    ____  _   _ ___
   / \  |  _ \| \ | |_ _|
  / _ \ | |_) |  \| || |
 / ___ \|  _ <| |\  || |
/_/   \_\_| \_\_| \_|___|
```

**Unified database access for Rust — every query returns a Polars DataFrame.**

[![CI](https://github.com/acroberts16/arni/actions/workflows/ci.yml/badge.svg)](https://github.com/acroberts16/arni/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/acroberts16/arni)](https://codecov.io/gh/acroberts16/arni)
[![Crates.io](https://img.shields.io/crates/v/arni)](https://crates.io/crates/arni)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Named for Árni Magnússon (1663–1730), the Icelandic scholar who gathered and preserved the largest known collection of Norse manuscripts, **arni** brings that same principle to your data layer.
Connect to PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, DuckDB, or SQLite through a single trait-based API — and receive every result as a Polars DataFrame, ready for analysis.

## Overview

Arni is a multi-database adapter library for Rust that provides a unified interface for working with various databases (PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, DuckDB, SQLite). All query results are returned as Polars DataFrames for easy data analysis and manipulation.

## Project Structure

```text
arni/
├── crates/
│   ├── arni-data/                 # Core adapter library (published)
│   │   ├── src/
│   │   │   ├── adapters/          # One file per database (postgres, mysql, sqlite, …)
│   │   │   ├── adapter.rs         # DbAdapter trait, ConnectionConfig, shared types
│   │   │   └── lib.rs             # Re-exports; feature-gated adapter modules
│   │   ├── tests/                 # Integration tests (require live databases)
│   │   └── examples/              # Runnable usage examples
│   ├── arni-cli/                  # `arni` binary — CLI wrapper around arni-data
│   │   └── src/
│   │       ├── main.rs            # Command definitions and handlers
│   │       ├── db.rs              # Adapter factory + connect helper
│   │       └── config.rs          # YAML connection profile store
│   ├── arni-logging/              # Structured logging infrastructure
│   └── arni/                      # Legacy placeholder crate (not used by CLI)
├── docs/                          # Architecture and usage documentation
├── scripts/                       # Dev/CI helper scripts
├── Cargo.toml                     # Workspace configuration
└── README.md
```

## Features

- **Unified Interface**: Common trait-based API across all seven database adapters
- **DataFrame Results**: Every query returns a Polars DataFrame — no adapter-specific code in your business logic
- **Async-First**: Built on Tokio; connection pools, RwLock-guarded clients throughout
- **Schema Introspection**: Tables, views, indexes, foreign keys, stored procedures, and server metadata
- **Bulk Operations**: Batch insert, update, and delete with automatic chunking
- **YAML Configuration**: File-based connection config with environment variable support

## Adapter Support Matrix

> **Key**: ✅ Implemented · ⚠️ Partial · ❌ Not applicable · 🔧 Planned

| Operation | PostgreSQL | MySQL | MSSQL | MongoDB | Oracle | DuckDB | SQLite |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `connect` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `execute_query` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `export_dataframe` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `list_tables` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `describe_table` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| &nbsp;&nbsp;↳ `row_count` | ⚠️ approx | ⚠️ approx | ✅ | ✅ | ⚠️ approx | ✅ | ✅ |
| &nbsp;&nbsp;↳ `size_bytes` | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| &nbsp;&nbsp;↳ `created_at` | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| `get_server_info` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `get_views` | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ |
| `get_view_definition` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| `get_indexes` | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ✅ |
| `get_foreign_keys` | ✅ | ✅ | ✅ | ❌ | ✅ | ⚠️ | ✅ |
| `list_stored_procedures` | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| `bulk_insert` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `bulk_update` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `bulk_delete` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

**Notes:**

- `row_count ⚠️ approx` — PostgreSQL, MySQL, Oracle use catalog statistics (fast, not exact); use `SELECT COUNT(*)` for precision
- `get_views ⚠️` MongoDB — views not enumerable via driver; returns empty list
- `get_indexes ⚠️` DuckDB — index introspection limited; returns empty list
- `get_foreign_keys ⚠️` DuckDB — FKs exist in schema but are not enforced; returns empty list
- `❌` MongoDB — document model has no views, foreign keys, or stored procedures by design
- `❌` DuckDB / SQLite — no stored procedure engine

## Quick Start

Add to `Cargo.toml`:

```toml
[dependencies]
arni-data = { version = "0.1", features = ["duckdb"] }  # or "postgres", "mysql", etc.
```

```rust
use std::collections::HashMap;
use arni_data::{adapters::duckdb::DuckDbAdapter, ConnectionConfig, DatabaseType, DbAdapter};
use polars::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect — swap DuckDbAdapter for PostgresAdapter, MySqlAdapter, etc. with no API changes
    let config = ConnectionConfig {
        id: "demo".into(), name: "demo".into(), db_type: DatabaseType::DuckDB,
        host: None, port: None, database: ":memory:".into(),
        username: None, use_ssl: false, parameters: HashMap::new(),
    };
    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;

    // Write a Polars DataFrame into the database
    let df = df!["name" => ["Alice", "Bob", "Carol"], "score" => [92.5f64, 87.0, 95.1]]?;
    adapter.export_dataframe(&df, "players", None, true).await?;

    // Query it back — every result is a Polars DataFrame
    let result = adapter.query_df("SELECT * FROM players ORDER BY score DESC").await?;
    println!("{result}");
    // shape: (3, 2)
    // ┌───────┬───────┐
    // │ name  ┆ score │
    // │ ---   ┆ ---   │
    // │ str   ┆ f64   │
    // ╞═══════╪═══════╡
    // │ Carol ┆ 95.1  │
    // │ Alice ┆ 92.5  │
    // │ Bob   ┆ 87.0  │
    // └───────┴───────┘
    Ok(())
}
```

See [`crates/arni-data/examples/quickstart.rs`](crates/arni-data/examples/quickstart.rs) for the full runnable version including schema introspection.

## CLI Usage

The `arni` binary wraps `arni-data` behind a YAML connection-profile system.

### Setup

```bash
# Add a connection profile (saved to ~/.arni/connections.yml)
arni config add my-pg \
  --type postgres --host localhost --database mydb --username myuser

# List all profiles
arni config list

# Test connectivity without running a query
arni connect my-pg
```

### Query

```bash
# Pretty-print as table (default)
arni query my-pg --sql "SELECT id, name FROM users LIMIT 5"

# JSON output
arni query my-pg --sql "SELECT * FROM orders" --format json

# CSV output
arni query my-pg --sql "SELECT * FROM products" --format csv
```

### Schema introspection

```bash
# List all tables
arni metadata my-pg --tables

# List views and schemas together
arni metadata my-pg --views --schemas

# Describe columns of a specific table
arni metadata my-pg --columns --table users

# Search for tables whose names contain "order"
arni metadata my-pg --search order

# Show indexes on a table
arni metadata my-pg --indexes --table orders
```

### Export

```bash
# Export query result to a file
arni export my-pg \
  --sql "SELECT * FROM users" \
  --format parquet \
  --output users.parquet

arni export my-pg \
  --sql "SELECT * FROM events WHERE date > '2024-01-01'" \
  --format csv \
  --output events.csv
```

## Documentation

| Guide | Description |
| :--- | :--- |
| **[Getting Started](docs/getting-started.md)** | Five-minute introduction: first query, bulk ops, CLI tour, common errors |
| **[Architecture Guide](docs/architecture.md)** | Codebase internals, trait hierarchy, and how to add a new adapter |
| **[Examples](crates/arni-data/examples/README.md)** | Runnable programs: analytics, multi-adapter comparison |
| **[Local Databases](docs/local-databases.md)** | Spin up PostgreSQL, MySQL, MongoDB and more via Docker/Podman |

## Local Development with Databases

The project includes a Docker Compose configuration for running **5 database systems** locally for integration testing:

- **PostgreSQL 16** (alpine) - Port 5432
- **MySQL 8.0** - Port 3306
- **Azure SQL Edge** (SQL Server) - Port 1433
- **Oracle 23ai Free** - Ports 1521/5500
- **MongoDB 7** - Port 27017

Each container includes health checks, persistent data storage, and pre-populated test data.

### Start the Databases

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

## Development Workflow

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

## Contributing

> [!IMPORTANT]
> **For Contributors & AI Agents**: All development follows a mandatory two-phase workflow:
>
> 1. **Planning**: [`.claude/commands/task-generate.md`](.claude/commands/task-generate.md) - Create structured WBS
> 2. **Execution**: [`.claude/commands/task-execute.md`](.claude/commands/task-execute.md) - Implement with context
>
> **See [`WORKFLOW.md`](WORKFLOW.md) for complete details.**

## License

MIT
