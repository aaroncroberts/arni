<!-- markdownlint-disable MD041 -->
```text
    _    ____  _   _ ___
   / \  |  _ \| \ | |_ _|
  / _ \ | |_) |  \| || |
 / ___ \|  _ <| |\  || |
/_/   \_\_| \_\_| \_|___|
```

**Unified database access for Rust — lightweight `QueryResult` by default, Polars DataFrames when you need them.**

[![CI](https://github.com/aaroncroberts/arni/actions/workflows/ci.yml/badge.svg)](https://github.com/aaroncroberts/arni/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/aaroncroberts/arni)](https://codecov.io/gh/aaroncroberts/arni)
[![Crates.io](https://img.shields.io/crates/v/arni)](https://crates.io/crates/arni)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Named for Árni Magnússon (1663–1730), the Icelandic scholar who gathered and preserved the largest known collection of Norse manuscripts, **arni** brings that same principle to your data layer.
Connect to PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, DuckDB, or SQLite through a single trait-based API.
Base queries return a lightweight `QueryResult`; add the `polars` feature to get full DataFrame support.

## Overview

Writing database code in Rust today means picking a driver per database, learning its async model,
writing your own type-mapping, and starting over when you swap backends. arni removes that friction.

**One trait. Seven databases. Three ways to use it:**

- **Library** — embed arni in your Rust application. Write your query logic once against `DbAdapter`;
  switch from DuckDB to Postgres to MySQL by changing one line. Use DuckDB in-memory for zero-setup
  unit tests, stream large result sets row-by-row with `execute_query_stream`, or pull results
  straight into Polars DataFrames for analytics.

- **MCP server** — run `arni mcp` and register it with Claude Desktop or Claude Code. Claude gains
  live access to your database schemas and data: it calls `describe_table` before writing a struct,
  queries live data during an incident investigation, and drafts migrations that account for your
  real indexes and foreign keys — all without you copy-pasting schemas into the chat.

- **CLI** — inspect tables, run queries, export to CSV/Parquet/JSON, and perform bulk operations
  from the shell. One binary, no database-specific client tools required.

Database drivers are individually opt-in via feature flags — compile only what you need.
See [docs/use-cases.md](docs/use-cases.md) for concrete examples of each surface in action.

## Project Structure

```text
arni/
├── crates/
│   ├── arni/                      # Core library crate (use this in your Cargo.toml)
│   │   ├── src/
│   │   │   ├── adapters/          # One file per database (postgres, mysql, sqlite, …)
│   │   │   ├── adapter.rs         # DbAdapter trait, ConnectionConfig, shared types
│   │   │   ├── config.rs          # ArniConfig, ConfigProfile — YAML/TOML loader
│   │   │   ├── registry.rs        # ConnectionRegistry — shared adapter pool
│   │   │   └── lib.rs             # Re-exports; feature-gated adapter modules
│   │   ├── tests/                 # Integration tests (per adapter, require live databases)
│   │   └── examples/              # Runnable usage examples
│   ├── arni-cli/                  # `arni` binary — CLI wrapper
│   │   └── src/
│   │       ├── main.rs            # Command definitions and handlers
│   │       ├── db.rs              # Adapter factory + connect helper
│   │       └── config.rs          # YAML connection profile store
│   ├── arni-mcp/                  # MCP server — exposes arni as AI tool calls
│   └── arni-logging/              # Structured logging infrastructure
├── docs/                          # Architecture and usage documentation
├── scripts/                       # Dev/CI helper scripts
├── Cargo.toml                     # Workspace configuration
└── README.md
```

## Features

- **Unified Interface**: Common `DbAdapter` trait across all seven database adapters
- **Lightweight by default**: Base API returns `QueryResult` — no Polars, no heavy compile
- **Opt-in DataFrames**: Add `features = ["polars"]` for `query_df`, `export_dataframe`, CSV/JSON/Parquet/Excel
- **Fine-grained feature flags**: `postgres`, `mysql`, `sqlite`, `mongodb`, `mssql`, `oracle`, `duckdb` — pick what you need (see [docs/feature-flags.md](docs/feature-flags.md))
- **System DuckDB**: `duckdb` feature links against a system install; `duckdb-bundled` builds from source when no system install is available
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
| `execute_query_stream`¹ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `execute_query_mapped`¹ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `execute_query_json`² | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `execute_query_csv`³ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
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

- ¹ `execute_query_stream` / `execute_query_mapped` — no extra feature flag; requires implementing [`FromQueryRow`](https://docs.rs/arni/latest/arni/trait.FromQueryRow.html) for your type. All seven adapters implemented. Note: MSSQL materializes internally due to tiberius driver lifetime constraints; all others stream row-by-row.
- ² `execute_query_json` — requires `--features json`
- ³ `execute_query_csv` — requires `--features csv`
- `row_count ⚠️ approx` — PostgreSQL, MySQL, Oracle use catalog statistics (fast, not exact); use `SELECT COUNT(*)` for precision
- `get_views ⚠️` MongoDB — views not enumerable via driver; returns empty list
- `get_indexes ⚠️` DuckDB — index introspection limited; returns empty list
- `get_foreign_keys ⚠️` DuckDB — FKs exist in schema but are not enforced; returns empty list
- `❌` MongoDB — document model has no views, foreign keys, or stored procedures by design
- `❌` DuckDB / SQLite — no stored procedure engine

## Quick Start

### Without Polars (lightweight — any database)

```toml
[dependencies]
# PostgreSQL only, no polars — minimal compile
arni = { version = "0.4", default-features = false, features = ["postgres"] }
```

```rust
use arni::{adapters::postgres::PostgresAdapter, ConnectionConfig, DatabaseType, DbAdapter};

let result = adapter.execute_query("SELECT * FROM users LIMIT 5").await?;
for row in &result.rows {
    println!("{:?}", row);
}
```

### With Polars (DataFrame API)

```toml
[dependencies]
# DuckDB in-memory + DataFrame output
arni = { version = "0.4", default-features = false, features = ["duckdb", "polars"] }
```

```rust
use std::collections::HashMap;
use arni::{adapters::duckdb::DuckDbAdapter, ConnectionConfig, DatabaseType, DbAdapter};
use arni::polars::prelude::*;  // polars re-exported — no direct dep needed

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ConnectionConfig {
        id: "demo".into(), name: "demo".into(), db_type: DatabaseType::DuckDB,
        host: None, port: None, database: ":memory:".into(),
        username: None, use_ssl: false, parameters: HashMap::new(),
        pool_config: None,
    };
    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;

    // Write a DataFrame into the database
    let df = df!["name" => ["Alice", "Bob", "Carol"], "score" => [92.5f64, 87.0, 95.1]]?;
    adapter.export_dataframe(&df, "players", None, true).await?;

    // Query back as a DataFrame
    let result = adapter.query_df("SELECT * FROM players ORDER BY score DESC").await?;
    println!("{result}");
    Ok(())
}
```

See [`crates/arni/examples/quickstart.rs`](crates/arni/examples/quickstart.rs) for the full runnable version.
See [docs/feature-flags.md](docs/feature-flags.md) for all feature flags, install examples, and system DuckDB setup.

## CLI Usage

The `arni` binary wraps the `arni` library behind a YAML connection-profile system.

### Setup

```bash
# Add a connection profile (saved to ~/.arni/config.yaml)
arni config add \
  --name my-pg \
  --type postgres --host localhost --database mydb --username myuser

# List all profiles
arni config list

# Test connectivity and print server info
arni connect --profile my-pg
```

### Query

```bash
# Pretty-print as table (default)
arni query "SELECT id, name FROM users LIMIT 5" --profile my-pg

# JSON output
arni query "SELECT * FROM orders" --profile my-pg --format json

# CSV output
arni query "SELECT * FROM products" --profile my-pg --format csv
```

### Schema introspection

```bash
# List all tables
arni metadata --profile my-pg --tables

# List views and schemas together
arni metadata --profile my-pg --views --schemas

# Describe columns of a specific table
arni metadata --profile my-pg --columns --table users

# Search for tables whose names contain "order"
arni metadata --profile my-pg --search order

# Show indexes on a table
arni metadata --profile my-pg --indexes --table orders
```

### Export

```bash
# Export query result to a file
arni export "SELECT * FROM users" \
  --profile my-pg \
  --format parquet \
  --output users.parquet

arni export "SELECT * FROM events WHERE date > '2024-01-01'" \
  --profile my-pg \
  --format csv \
  --output events.csv
```

## Documentation

| Guide | Description |
| :--- | :--- |
| **[Getting Started](docs/getting-started.md)** | Five-minute introduction: first query, bulk ops, CLI tour, common errors |
| **[Use Cases](docs/use-cases.md)** | Real-world patterns for the library, MCP server, and CLI — when to use each and what it looks like |
| **[MCP Server](docs/mcp.md)** | Connect Claude and other AI agents directly to your databases |
| **[Configuration Reference](docs/configuration.md)** | Full YAML/TOML schema, all fields, environment variable substitution |
| **[Architecture Guide](docs/architecture.md)** | Codebase internals, trait hierarchy, and how to add a new adapter |
| **[Examples](crates/arni/examples/README.md)** | Runnable programs: analytics, multi-adapter comparison |
| **[Axum API Example](examples/axum-api/README.md)** | Full Axum HTTP server using arni as a library — zero-config SQLite, swap to any database |
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
