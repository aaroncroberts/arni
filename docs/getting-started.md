# Getting Started with Arni

Arni gives you a single, consistent interface to query any supported database and receive every result as a [Polars](https://pola.rs) DataFrame. This guide gets you from zero to your first query in about five minutes.

---

## Prerequisites

| Requirement | Notes |
| :--- | :--- |
| **Rust** ≥ 1.75 (stable) | Install via [rustup](https://rustup.rs) |
| **Cargo** | Included with rustup |
| A database (optional) | DuckDB runs fully in-memory — no installation needed for the first examples |

> **Tip:** The fastest path is DuckDB in-memory. It requires no server, no Docker, and no credentials. If you want to connect to PostgreSQL, MySQL, or another server-based database, see [Local Development with Databases](../README.md#local-development-with-databases).

---

## Adding Arni to Your Project

```bash
cargo new my-arni-app
cd my-arni-app
```

Edit `Cargo.toml` and add `arni`. Enable only the features you need — each database driver is opt-in:

```toml
[dependencies]
arni   = { version = "0.4", features = ["duckdb", "polars"] }
tokio  = { version = "1",   features = ["full"] }
anyhow = "1"
```

### Available Feature Flags

| Feature | Database |
| :--- | :--- |
| `duckdb` | DuckDB (embedded, in-memory or file) |
| `sqlite` | SQLite (embedded, in-memory or file) |
| `postgres` | PostgreSQL |
| `mysql` | MySQL / MariaDB |
| `mssql` | Microsoft SQL Server / Azure SQL |
| `mongodb` | MongoDB |
| `oracle` | Oracle Database |
| `all-databases` | All of the above |

---

## Your First Query (DuckDB, Zero Setup)

```rust
use std::collections::HashMap;
use arni::{adapters::duckdb::DuckDbAdapter, ConnectionConfig, DatabaseType, DbAdapter};
use polars::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Configure ──────────────────────────────────────────────────────────
    let config = ConnectionConfig {
        id: "demo".to_string(),
        name: "Demo".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(), // in-memory, no file needed
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    // ── 2. Connect ────────────────────────────────────────────────────────────
    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;

    // ── 3. Write a Polars DataFrame ───────────────────────────────────────────
    let df = df![
        "name"  => ["Alice", "Bob", "Carol"],
        "score" => [92.5f64, 87.0, 95.1],
    ]?;
    adapter.export_dataframe(&df, "players", None, true).await?;

    // ── 4. Query it back ──────────────────────────────────────────────────────
    let result = adapter
        .query_df("SELECT * FROM players ORDER BY score DESC")
        .await?;
    println!("{result}");

    Ok(())
}
```

```text
shape: (3, 2)
┌───────┬───────┐
│ name  ┆ score │
│ ---   ┆ ---   │
│ str   ┆ f64   │
╞═══════╪═══════╡
│ Carol ┆ 95.1  │
│ Alice ┆ 92.5  │
│ Bob   ┆ 87.0  │
└───────┴───────┘
```

Run it with:

```bash
cargo run --features duckdb
```

---

## Switching Databases

Only the adapter type and its import change — the rest of the API is identical:

```rust
// DuckDB (zero setup)
use arni::adapters::duckdb::DuckDbAdapter;
let mut adapter = DuckDbAdapter::new(config.clone());

// PostgreSQL
use arni::adapters::postgres::PostgresAdapter;
let mut adapter = PostgresAdapter::new(config.clone());

// MySQL
use arni::adapters::mysql::MySqlAdapter;
let mut adapter = MySqlAdapter::new(config.clone());

// MongoDB
use arni::adapters::mongodb::MongoDbAdapter;
let mut adapter = MongoDbAdapter::new(config.clone());
```

The `connect`, `query_df`, `export_dataframe`, `describe_table`, `bulk_insert`, `bulk_update`, and `bulk_delete` calls are the same regardless of which adapter you use.

---

## Schema Introspection

After writing data, you can inspect the table structure:

```rust
let info = adapter.describe_table("players", None).await?;

println!("Table:    {}", info.name);
println!("Rows:     {}", info.row_count.unwrap_or(0));
println!("Size:     {} bytes", info.size_bytes.unwrap_or(0));
for col in &info.columns {
    println!(
        "  {:<15} {:<12} nullable={}",
        col.name, col.data_type, col.nullable
    );
}
```

```text
Table:    players
Rows:     3
Size:     0 bytes
  name            Utf8         nullable=true
  score           Float64      nullable=true
```

Other introspection methods available on every adapter:

```rust
let tables   = adapter.list_tables(None).await?;
let views    = adapter.get_views(None).await?;
let indexes  = adapter.get_indexes("players", None).await?;
let server   = adapter.get_server_info().await?;
```

---

## Bulk Operations with FilterExpr

Arni's bulk operations use the typed `FilterExpr` enum instead of raw SQL strings, so the same predicate works against any backend — SQL or MongoDB:

```rust
use arni::{FilterExpr, QueryValue};
use std::collections::HashMap;

// Bulk update: set score = 100 where name = 'Alice'
let mut updates = HashMap::new();
updates.insert("score".to_string(), QueryValue::Float(100.0));

adapter.bulk_update(
    "players",
    &[(updates, FilterExpr::Eq("name".to_string(), QueryValue::Text("Alice".to_string())))],
    None,
).await?;

// Bulk delete: remove rows where score < 90
adapter.bulk_delete(
    "players",
    &[FilterExpr::Lt("score".to_string(), QueryValue::Float(90.0))],
    None,
).await?;
```

Compound predicates compose naturally:

```rust
// score >= 80 AND score <= 95
let filter = FilterExpr::And(vec![
    FilterExpr::Gte("score".to_string(), QueryValue::Float(80.0)),
    FilterExpr::Lte("score".to_string(), QueryValue::Float(95.0)),
]);
```

---

## YAML Configuration (for the CLI)

Create `~/.arni/connections.yml` to define named connection profiles:

```yaml
# ~/.arni/connections.yml

# In-memory DuckDB — no server needed
demo-duckdb:
  type: duckdb
  database: ":memory:"

# Local SQLite file
local-sqlite:
  type: sqlite
  database: /tmp/mydata.db

# PostgreSQL (password prompted at runtime if null)
dev-postgres:
  type: postgres
  host: localhost
  port: 5432
  database: mydb
  username: myuser
  password: ~
  ssl: false

# MongoDB
local-mongo:
  type: mongodb
  host: localhost
  port: 27017
  database: mydb
```

The CLI reads `~/.arni/connections.yml` by default and also accepts a native library path via `~/.arni/config.yml` for Oracle.

---

## CLI Quick Tour

Install the CLI:

```bash
cargo install arni-cli
```

```bash
# List configured profiles
arni config list

# Connect and check server info
arni connect --profile dev-postgres

# Execute a query, display as table
arni query "SELECT * FROM users LIMIT 10" --profile dev-postgres

# Export query results to CSV
arni export "SELECT * FROM orders" \
  --profile dev-postgres \
  --format csv \
  --output orders.csv

# Show table metadata
arni metadata --profile dev-postgres --tables --columns

# Start local dev databases (requires podman-compose or docker-compose)
arni dev start
arni dev status
arni dev stop
```

---

## Common Errors

### `Connection refused` / `Not connected`

The adapter was not connected before calling a method. Always call `connect()` first:

```rust
adapter.connect(&config, Some("password")).await?;
```

For password-less connections (DuckDB, SQLite), pass `None`:

```rust
adapter.connect(&config, None).await?;
```

### Feature not enabled

```text
error[E0432]: unresolved import `arni::adapters::postgres`
```

Add the feature flag to `Cargo.toml`:

```toml
arni = { version = "0.4", features = ["postgres"] }
```

### `DataError::NotSupported`

Some operations are database-specific (e.g., stored procedures don't exist in SQLite/DuckDB). The adapter returns `Err(DataError::NotSupported(...))` rather than panicking. Handle it gracefully:

```rust
match adapter.list_stored_procedures(None).await {
    Ok(procs) => { /* use procs */ }
    Err(arni::DataError::NotSupported(_)) => { /* skip for this DB */ }
    Err(e) => return Err(e.into()),
}
```

### Oracle: library not found

Oracle requires the [Instant Client](https://www.oracle.com/database/technologies/instant-client.html). Set the library path in `~/.arni/config.yml`:

```yaml
oracle:
  lib_dir: /opt/oracle/instantclient_23_3
```

Or export the environment variable:

```bash
export DYLD_LIBRARY_PATH=~/Oracle/instantclient_23_3  # macOS
export LD_LIBRARY_PATH=~/Oracle/instantclient_23_3    # Linux
```

---

## Next Steps

- **[Architecture Guide](architecture.md)** — how arni works internally, and how to add a new adapter
- **[Examples](../crates/arni/examples/README.md)** — runnable programs for real-world patterns
- **[Local Databases](local-databases.md)** — spin up PostgreSQL, MySQL, MongoDB and more via Docker/Podman
- **[Configuration Reference](configuration.md)** — full YAML/TOML config schema
