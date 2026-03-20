# Architecture & Developer Guide

This document explains how arni is structured internally and serves as the reference for contributors who want to understand the codebase or add a new database adapter.

---

## Project Structure

```text
arni/
├── crates/
│   ├── arni/                      # Core library crate (add this to your Cargo.toml)
│   │   ├── src/
│   │   │   ├── adapter.rs         # DbAdapter trait, FilterExpr, supporting types
│   │   │   ├── adapters/
│   │   │   │   ├── duckdb.rs      # DuckDB adapter
│   │   │   │   ├── mongodb.rs     # MongoDB adapter
│   │   │   │   ├── mssql.rs       # SQL Server adapter (Tiberius)
│   │   │   │   ├── mysql.rs       # MySQL adapter (sqlx)
│   │   │   │   ├── oracle.rs      # Oracle adapter (oracle-rs)
│   │   │   │   ├── postgres.rs    # PostgreSQL adapter (tokio-postgres)
│   │   │   │   └── sqlite.rs      # SQLite adapter (sqlx)
│   │   │   ├── config.rs          # ArniConfig, ConfigProfile (YAML/TOML loader)
│   │   │   ├── registry.rs        # ConnectionRegistry — shared adapter pool
│   │   │   ├── error.rs           # DataError enum
│   │   │   └── lib.rs             # Re-exports: DbAdapter, QueryValue, FilterExpr, …
│   │   ├── examples/              # Runnable examples
│   │   └── tests/                 # Integration tests (per adapter)
│   ├── arni-cli/                  # Command-line interface
│   │   └── src/
│   │       ├── main.rs            # clap commands: connect, query, export, metadata, mcp
│   │       ├── output_formatter.rs # OutputFormatter — human/JSON output switching
│   │       └── config.rs          # ~/.arni/connections.yml loader
│   ├── arni-mcp/                  # MCP server — exposes arni as AI tool calls
│   └── arni-logging/              # Structured tracing/logging infrastructure
├── docs/                          # This guide and other documentation
├── scripts/                       # Dev scripts, init SQL, coverage
└── compose.yml                    # Docker/Podman dev databases
```

---

## Trait Hierarchy

The library is built around a single async trait that all adapters implement:

```text
                          ┌──────────────────────────────┐
                          │           DbAdapter           │
                          │        (async_trait)          │
                          │                              │
                          │  Connection management        │
                          │    connect()                 │
                          │    disconnect()              │
                          │    is_connected()            │
                          │    test_connection()         │
                          │                              │
                          │  DataFrame operations         │
                          │    read_table()              │
                          │    query_df()                │
                          │    export_dataframe()        │
                          │                              │
                          │  Row-level operations         │
                          │    execute_query()           │
                          │    execute_query_stream()    │
                          │    execute_query_mapped()    │
                          │    execute_query_json()²     │
                          │    execute_query_csv()³      │
                          │                              │
                          │  Schema discovery             │
                          │    list_databases()          │
                          │    list_tables()             │
                          │    describe_table()          │
                          │    get_views()               │
                          │    get_indexes()             │
                          │    get_foreign_keys()        │
                          │    get_server_info()         │
                          │    list_stored_procedures()  │
                          │                              │
                          │  Bulk operations              │
                          │    bulk_insert()             │
                          │    bulk_update()             │
                          │    bulk_delete()             │
                          │                              │
                          │  Metadata accessor            │
                          │    metadata() → AdapterMetadata │
                          └──────────────┬───────────────┘
                                         │ implements
             ┌───────────────────────────┼───────────────────────────┐
             │           │           │           │           │         │
    PostgresAdapter  MySqlAdapter  DuckDbAdapter  SqliteAdapter  MssqlAdapter
                      MongoDbAdapter  OracleAdapter
```

The `Connection` trait (separate from `DbAdapter`) handles only the connection lifecycle and is used internally by some adapters.

---

## Core Types

All types are defined in `crates/arni/src/adapter.rs` and re-exported from the `arni` crate.

### `ConnectionConfig`

Describes how to reach a database. All adapters accept the same struct:

```rust
pub struct ConnectionConfig {
    pub id:          String,              // Unique identifier
    pub name:        String,              // Human-readable label
    pub db_type:     DatabaseType,        // Postgres | MySQL | SQLite | …
    pub host:        Option<String>,
    pub port:        Option<u16>,
    pub database:    String,              // DB name, or ":memory:", or file path
    pub username:    Option<String>,
    pub use_ssl:     bool,
    pub parameters:  HashMap<String, String>, // Driver-specific extras (incl. password)
    pub pool_config: Option<PoolConfig>,  // Connection pool settings
}
```

### `QueryValue`

A sum type for individual cell values used in bulk operations:

```rust
pub enum QueryValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}
```

### `FilterExpr`

An adapter-agnostic predicate for `bulk_update` and `bulk_delete`. Each adapter translates it to its native language (SQL `WHERE` or BSON filter):

```rust
pub enum FilterExpr {
    Eq(String, QueryValue),          // col = value
    Ne(String, QueryValue),          // col <> value
    Gt(String, QueryValue),          // col > value
    Gte(String, QueryValue),         // col >= value
    Lt(String, QueryValue),          // col < value
    Lte(String, QueryValue),         // col <= value
    In(String, Vec<QueryValue>),     // col IN (…)
    IsNull(String),                  // col IS NULL
    IsNotNull(String),               // col IS NOT NULL
    And(Vec<FilterExpr>),            // (a AND b AND …)
    Or(Vec<FilterExpr>),             // (a OR b OR …)
    Not(Box<FilterExpr>),            // NOT (a)
}
```

Two free functions in `adapter.rs` handle translation:

- `filter_to_sql(&FilterExpr) -> String` — used by all SQL adapters
- `mongo_filter_to_bson(&FilterExpr) -> Document` — private to the MongoDB adapter

### `TableInfo` / `ColumnInfo` / `ServerInfo`

Returned by schema introspection methods:

```rust
pub struct TableInfo {
    pub name:       String,
    pub schema:     Option<String>,
    pub columns:    Vec<ColumnInfo>,
    pub row_count:  Option<i64>,       // Approximate for Postgres/MySQL/Oracle
    pub size_bytes: Option<i64>,       // None for in-memory / MongoDB
    pub created_at: Option<String>,    // ISO-8601; None if DB doesn't track it
}

pub struct ColumnInfo {
    pub name:           String,
    pub data_type:      String,
    pub nullable:       bool,
    pub default_value:  Option<String>,
    pub is_primary_key: bool,
}

pub struct ServerInfo {
    pub version:     String,
    pub server_type: String,
    pub extra_info:  HashMap<String, String>,
}
```

---

## Data Flow

### Reading Data

```text
Database
  │
  │  (native query protocol: tokio-postgres / sqlx / mongodb driver / …)
  ▼
execute_query() → QueryResult { columns: Vec<String>, rows: Vec<Vec<QueryValue>> }
  │
  │  QueryResult::to_dataframe()
  ▼
Polars DataFrame
```

`query_df()` is a convenience wrapper that calls `execute_query()` and then `to_dataframe()`. The conversion infers each column's Polars type from the first non-null `QueryValue` in that column.

### Writing Data

```text
Polars DataFrame
  │
  │  export_dataframe() — each adapter serializes columns to its own INSERT/bulk format
  ▼
Database
```

`export_dataframe` with `replace = true` drops and recreates the table first; with `replace = false` it appends.

---

## Adapter Lifecycle

Every call sequence looks like this:

```text
1. DuckDbAdapter::new(config)       ← struct construction, no I/O
2. adapter.connect(&config, pw)     ← open connection / pool
3. adapter.query_df("SELECT …")     ← I/O
4. adapter.export_dataframe(…)      ← I/O
5. adapter.disconnect()             ← close connection / pool
```

Adapters use `tokio::sync::RwLock` (or `Arc<Mutex<_>>` for thread-bound drivers like DuckDB and Oracle) to hold the live connection, so they are `Send + Sync` and can be shared across async tasks.

---

## Feature Flag System

Each database driver is gated behind a Cargo feature to keep compile times and binary sizes small:

```toml
# Cargo.toml (user's project)
arni = { version = "0.4", features = ["duckdb", "postgres"] }
```

In `crates/arni/Cargo.toml`:

```toml
[features]
default = []
postgres    = ["dep:tokio-postgres", "dep:postgres-native-tls", "dep:native-tls"]
mysql       = ["dep:sqlx", "sqlx?/mysql"]
sqlite      = ["dep:sqlx", "sqlx?/sqlite"]
mongodb     = ["dep:mongodb"]
mssql       = ["dep:tiberius", "dep:tokio-util"]
oracle      = ["dep:oracle"]
duckdb      = ["dep:duckdb"]
all-databases = ["postgres", "mysql", "sqlite", "mongodb", "mssql", "oracle", "duckdb"]
```

All adapter modules are wrapped in `#[cfg(feature = "...")]` guards.

---

## Implementing a New Adapter

Adding support for a new database requires four steps.

### Step 1: Add the driver dependency

In `crates/arni/Cargo.toml`:

```toml
[dependencies]
mydb-driver = { version = "1", optional = true }

[features]
mydb = ["dep:mydb-driver"]
```

### Step 2: Create the adapter file

Create `crates/arni/src/adapters/mydb.rs`. Start with this skeleton — fill in each method one by one:

```rust
use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue,
    Result, ServerInfo, TableInfo, ViewInfo, filter_to_sql,
};
use crate::DataError;
use async_trait::async_trait;
use polars::prelude::*;
use std::collections::HashMap;

pub struct MyDbAdapter {
    config: ConnectionConfig,
    // connection: Arc<RwLock<Option<MyDbConnection>>>,
    // connected: Arc<RwLock<bool>>,
}

impl MyDbAdapter {
    pub fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ConnectionTrait for MyDbAdapter {
    async fn connect(&mut self) -> Result<()> { todo!() }
    async fn disconnect(&mut self) -> Result<()> { todo!() }
    fn is_connected(&self) -> bool { todo!() }
    async fn health_check(&self) -> Result<bool> { todo!() }
    fn config(&self) -> &ConnectionConfig { &self.config }
}

#[async_trait]
impl DbAdapter for MyDbAdapter {
    async fn connect(&mut self, config: &ConnectionConfig, password: Option<&str>) -> Result<()> {
        todo!("Open connection using mydb-driver")
    }

    async fn disconnect(&mut self) -> Result<()> { todo!() }

    fn is_connected(&self) -> bool { todo!() }

    async fn test_connection(&self, config: &ConnectionConfig, password: Option<&str>) -> Result<bool> {
        todo!()
    }

    fn database_type(&self) -> DatabaseType { DatabaseType::MyDb /* add variant */ }

    fn metadata(&self) -> AdapterMetadata<'_> { AdapterMetadata::new(self) }

    async fn export_dataframe(
        &self, df: &DataFrame, table_name: &str, schema: Option<&str>, replace: bool,
    ) -> Result<u64> {
        todo!("Serialize DataFrame rows and INSERT")
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        todo!("Run query, map rows to Vec<Vec<QueryValue>>")
    }

    async fn list_databases(&self) -> Result<Vec<String>> { todo!() }
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> { todo!() }
    async fn describe_table(&self, table_name: &str, schema: Option<&str>) -> Result<TableInfo> { todo!() }

    // --- Optional (default impls return empty / NotSupported) ---
    // Override get_server_info, get_views, get_indexes, bulk_insert, etc. as needed
}
```

### Step 3: Register the adapter

In `crates/arni/src/adapters/mod.rs`:

```rust
#[cfg(feature = "mydb")]
pub mod mydb;
```

In `crates/arni/src/lib.rs` (optional convenience re-export):

```rust
#[cfg(feature = "mydb")]
pub use adapters::mydb::MyDbAdapter;
```

### Step 4: Add tests

Create `crates/arni/tests/mydb.rs`. The pattern follows `tests/duckdb.rs`:

```rust
#[cfg(feature = "mydb")]
mod mydb_tests {
    use arni::adapter::{Connection as ConnectionTrait, DbAdapter, DatabaseType};
    use arni::adapters::mydb::MyDbAdapter;

    fn make_config() -> arni::ConnectionConfig {
        // ...
    }

    #[tokio::test]
    async fn test_mydb_connect() {
        // use TEST_MYDB_AVAILABLE env var to skip in CI without the database
        if std::env::var("TEST_MYDB_AVAILABLE").is_err() { return; }
        let mut adapter = MyDbAdapter::new(make_config());
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        assert!(adapter.is_connected());
    }
}
```

### Reference: Mapping QueryValue to native types

Every `execute_query` implementation needs to map database driver row values to `QueryValue`. Use this table as a guide:

| Rust / driver type | `QueryValue` variant |
| :--- | :--- |
| `NULL` | `QueryValue::Null` |
| `bool` | `QueryValue::Bool(b)` |
| `i8 / i16 / i32 / i64 / u32` | `QueryValue::Int(i as i64)` |
| `f32 / f64` | `QueryValue::Float(f as f64)` |
| `String / &str` | `QueryValue::Text(s)` |
| `Vec<u8>` | `QueryValue::Bytes(b)` |
| Date / Time / Timestamp | `QueryValue::Text(format!("{}", dt))` |
| Decimal / Numeric | `QueryValue::Text(format!("{}", d))` |
| JSON / JSONB | `QueryValue::Text(json_string)` |
| UUID | `QueryValue::Text(uuid.to_string())` |

---

## Testing Strategy

### Unit tests (no database)

Located in `src/adapters/<adapter>.rs` under `#[cfg(test)] mod tests`. Test pure functions like connection string building, SQL literal formatting, and configuration validation.

```bash
cargo test --lib --features duckdb sqlite
```

### Integration tests (real database)

Located in `tests/<adapter>.rs`. Tests are gated by environment variables:

```bash
TEST_POSTGRES_AVAILABLE=true \
cargo test --features postgres -p arni --test postgres
```

DuckDB and SQLite tests use in-memory databases and run unconditionally:

```bash
cargo test --features "duckdb sqlite"
```

### Coverage

```bash
make coverage   # generates HTML report in target/tarpaulin/html/
```

Target: ≥ 80% line coverage.

---

## Code Style

- **Async**: All I/O via Tokio. No blocking calls on the async runtime — use `tokio::task::spawn_blocking` for sync drivers (DuckDB, Oracle).
- **Error handling**: Return `Result<T>` (= `std::result::Result<T, DataError>`) everywhere. No `unwrap()` in library code.
- **Logging**: `tracing` crate, structured events with `instrument`, `info!`, `debug!`, `warn!`, `error!`.
- **Imports**: `use crate::adapter::{DbAdapter, FilterExpr, QueryValue, filter_to_sql, …}`.
- **Formatting**: `cargo fmt` (standard rustfmt settings).
- **Linting**: `cargo clippy -- -D warnings` (zero warnings in CI).
