# arni-data Examples

Runnable examples that demonstrate real-world usage patterns for `arni-data`.

All examples that use DuckDB or SQLite run **entirely in-memory** — no server or credentials required.

---

## Examples

### `quickstart`

The fastest path from zero to a working query. Connects to an in-memory DuckDB database, writes a Polars DataFrame, queries it back, and inspects the table schema.

```bash
cargo run --example quickstart -p arni-data --features duckdb
```

**Concepts:** `connect`, `export_dataframe`, `query_df`, `describe_table`

---

### `analytics`

A complete analytics workflow: seed data, run SQL aggregations and window functions, append rows with `bulk_insert`, apply updates via a typed `FilterExpr` predicate, remove rows with `bulk_delete`, and close with schema introspection.

```bash
cargo run --example analytics -p arni-data --features duckdb
```

**Concepts:** `export_dataframe`, `query_df`, `bulk_insert`, `bulk_update`, `bulk_delete`, `FilterExpr`, `describe_table`, `get_server_info`

---

### `multi_adapter`

Writes the same Polars DataFrame to both DuckDB and SQLite using identical API calls, runs the same SQL query against each backend, and asserts the results match. Demonstrates that adapter-agnostic code is a first-class reality, not just a promise.

```bash
cargo run --example multi_adapter -p arni-data --features "duckdb sqlite"
```

**Concepts:** `export_dataframe`, `query_df`, `list_tables`, `get_server_info`, adapter interchangeability

---

## Running Against a Real Database

The same code works with a server-based adapter — only the config and import change:

```rust
// swap DuckDbAdapter for PostgresAdapter
use arni_data::adapters::postgres::PostgresAdapter;

let config = ConnectionConfig {
    id: "prod".to_string(),
    name: "Production".to_string(),
    db_type: DatabaseType::Postgres,
    host: Some("localhost".to_string()),
    port: Some(5432),
    database: "mydb".to_string(),
    username: Some("myuser".to_string()),
    use_ssl: false,
    parameters: HashMap::new(),
};

let mut adapter = PostgresAdapter::new(config.clone());
adapter.connect(&config, Some("mypassword")).await?;
```

Enable the matching feature:

```bash
cargo run --example quickstart -p arni-data --features postgres
```

For local development databases, see [`docs/local-databases.md`](../../../../docs/local-databases.md).
