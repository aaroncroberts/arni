# arni Use Cases

arni surfaces in three ways — each suited to a different context:

| Surface | When to reach for it |
| :--- | :--- |
| **Library** (`arni` crate) | You are writing a Rust application and need database access |
| **MCP server** (`arni mcp`) | You want Claude or another AI agent to work with your databases |
| **CLI** (`arni` binary) | You need to inspect, query, or export data without writing code |

---

## Library Use Cases

### Analytics pipeline

Pull query results directly into Polars, transform in-process, write to Parquet — no CSV
round-trip, no ORM, no intermediate materialisation:

```rust
let adapter = PostgresAdapter::new(config.clone());
adapter.connect(&config, None).await?;

let mut df = adapter.query_df(
    "SELECT region, SUM(revenue) AS total FROM orders GROUP BY region"
).await?;

// Polars is re-exported — no direct dependency needed
use arni::polars::prelude::*;
let df = df.sort(["total"], SortMultipleOptions::default().with_order_descending(true))?;

arni::to_file(&mut df, arni::DataFormat::Parquet, Path::new("revenue.parquet"))?;
```

### Data migration across backends

Read from one database, write to another using the same trait — adapter swap is one line:

```rust
// Source: legacy MySQL
let src = MySqlAdapter::new(mysql_config.clone());
src.connect(&mysql_config, Some("password")).await?;

let df = src.query_df("SELECT id, email, created_at FROM users").await?;

// Target: new Postgres
let dst = PostgresAdapter::new(pg_config.clone());
dst.connect(&pg_config, None).await?;

dst.export_dataframe(&df, "users", None, true).await?;
```

The same pattern works for any of the seven supported backends — no rewriting
query logic or type-mapping code when switching databases.

### Web API backend

Use DuckDB in-memory for local development; point at Postgres in production.
The Axum handler code never changes — only the adapter construction does:

```rust
// Local dev — zero setup, zero Docker
fn make_adapter() -> impl DbAdapter {
    let config = ConnectionConfig {
        db_type: DatabaseType::DuckDB,
        database: ":memory:".into(),
        ..Default::default()
    };
    DuckDbAdapter::new(config)
}

// Production — one-line swap
fn make_adapter() -> impl DbAdapter {
    PostgresAdapter::new(load_config_from_env())
}
```

See [`examples/axum-api`](../examples/axum-api/README.md) for the full runnable server.

### Zero-dependency test fixtures

Give every unit test a real SQL engine without Docker, network, or cleanup:

```rust
#[tokio::test]
async fn test_order_summary() {
    let config = ConnectionConfig {
        db_type: DatabaseType::DuckDB,
        database: ":memory:".into(),
        ..Default::default()
    };
    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await.unwrap();

    // Seed test data using the same API you test against
    let df = df!["id" => [1i64, 2, 3], "amount" => [100.0f64, 200.0, 50.0]].unwrap();
    adapter.export_dataframe(&df, "orders", None, true).await.unwrap();

    let result = adapter.query_df("SELECT SUM(amount) AS total FROM orders").await.unwrap();
    assert_eq!(result.column("total").unwrap().sum::<f64>().unwrap(), 350.0);
}
```

No mocking, no test containers — the exact same `DbAdapter` interface your production code uses.

### Streaming large result sets

`execute_query_stream` yields rows as they arrive from the database, keeping memory
flat for large exports or real-time pipelines:

```rust
use futures::StreamExt;

let mut stream = adapter.execute_query_stream("SELECT * FROM events").await?;

let mut writer = csv::Writer::from_path("events.csv")?;
while let Some(row) = stream.next().await {
    let row = row?;
    writer.write_record(row.iter().map(|v| v.to_string()))?;
}
```

Or use the convenience method directly — it writes into any `impl Write`:

```rust
let file = std::fs::File::create("events.csv")?;
adapter.execute_query_csv("SELECT * FROM events", file).await?;
```

---

## MCP Use Cases

The MCP server (`arni mcp`) lets Claude call your database directly during a
conversation — no copy-pasting schemas, no throwaway SQL files.

### Setup (one time)

```bash
cargo install arni-cli
```

Register in Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "arni": {
      "command": "arni",
      "args": ["mcp"]
    }
  }
}
```

Or register in Claude Code:

```bash
claude mcp add arni -- arni mcp
```

Configure a connection profile in `~/.arni/config.yaml` and restart. The arni tools
appear automatically under the 🔌 icon.

---

### Schema-aware code generation

Instead of pasting your schema into the chat, let Claude read it directly:

> **You:** Generate a Rust struct and sqlx query to fetch all active users with their
> most recent order date.

Claude calls `describe_table("users")`, `describe_table("orders")`, and `get_foreign_keys("orders")`
before writing a single line — so the field names, types, and join condition match
your actual schema, not a guess.

### Natural-language data exploration

Useful for onboarding a new codebase or a new team member who doesn't know the schema:

> **You:** What tables are in this database, and what does the orders table look like?

> **You:** How many users signed up last week versus the week before?

> **You:** Which products have never been ordered?

Claude translates each question into SQL, runs it, and explains the result — no
SQL knowledge required from the person asking.

### Incident investigation

When something looks wrong in staging or production, skip the throwaway query
loop and work with Claude directly:

> **You:** We're seeing a spike in failed checkouts. Can you look at the orders
> table and find any patterns in rows where status = 'failed' in the last 6 hours?

Claude queries `orders`, groups by error code, checks for correlations with
`user_id` and `product_id`, and surfaces patterns — all without you writing SQL.

### Migration drafting

Claude has full schema context before drafting any DDL:

> **You:** I want to add soft-delete to the users table. What migration do I need,
> and what queries in the codebase should I update?

Claude calls `describe_table("users")`, `get_indexes("users")`, checks for FK
references from other tables, and produces a migration that accounts for your
existing indexes and constraints — not a generic template.

### Bulk data cleanup

> **You:** Remove all sessions that expired more than 30 days ago.

Claude constructs the appropriate `bulk_delete` filter, shows you the expression
before executing, and reports rows affected. The same workflow applies to
normalising data, backfilling columns, or deduplication.

### Cross-database comparison

Register two profiles (e.g. `dev` and `staging`) and ask Claude to compare them:

> **You:** Are there any tables in staging that don't exist in dev? And do the
> schemas match for the users and orders tables?

Claude calls `tables(profile="dev")`, `tables(profile="staging")`, then
`describe_table` for each divergent table — giving you a diff without a migration
tool or manual comparison.

---

## CLI Use Cases

The `arni` CLI is for interactive inspection, shell scripts, and one-off data
tasks — no Rust code required.

### Quick schema inspection

```bash
# What tables are in this database?
arni metadata --profile my-pg --tables

# What columns does the orders table have?
arni metadata --profile my-pg --columns --table orders

# What indexes exist on orders?
arni metadata --profile my-pg --indexes --table orders
```

No psql, no mongosh, no SQL Server Management Studio — one CLI for every backend.

### CI/CD data validation

Verify database state as part of a deploy pipeline:

```bash
# Assert all migrations have run
COUNT=$(arni query "SELECT count(*) FROM schema_migrations" \
    --profile prod --format json | jq '.[0][0]')
[ "$COUNT" -eq "$EXPECTED_MIGRATION_COUNT" ] || exit 1
```

```bash
# Smoke-test connectivity before cutting traffic
arni connect --profile prod --json | jq '.ok' | grep -q true || exit 1
```

### Data export in scripts

```bash
# Export to Parquet for an analytics job
arni export "SELECT * FROM events WHERE date > '2026-01-01'" \
    --profile prod \
    --format parquet \
    --output events_2026.parquet

# Export to CSV for a spreadsheet hand-off
arni export "SELECT id, email, plan FROM users WHERE churned_at IS NOT NULL" \
    --profile prod \
    --format csv \
    --output churned_users.csv
```

### Onboarding an unfamiliar database

```bash
# Map everything without installing a DB-specific client
arni metadata --profile legacy-oracle --tables
arni metadata --profile legacy-oracle --views --schemas
arni metadata --profile legacy-oracle --procs
arni metadata --profile legacy-oracle --server
```

Useful when inheriting a legacy system or doing a pre-migration audit.

### Bulk operations from the shell

```bash
# Clean up expired sessions
arni bulk-delete \
    --profile dev-pg \
    --table sessions \
    --filter '{"expires_at": {"lt": "2026-01-01T00:00:00Z"}}'

# Deactivate test accounts
arni bulk-update \
    --profile staging \
    --table users \
    --filter '{"email": {"in": ["test@example.com", "qa@example.com"]}}' \
    --values '{"active": false}'
```

### JSON output for scripting

Every command supports `--json` for machine-readable output:

```bash
# Table list as JSON
arni metadata --profile my-pg --tables --json | jq '.tables[].name'

# Query result as JSON array
arni query "SELECT id, email FROM users LIMIT 5" --profile my-pg --json
```

---

## Choosing the right surface

| Need | Recommended surface |
| :--- | :--- |
| Building a Rust application | **Library** |
| Need DataFrames, streaming, or bulk ops in code | **Library** |
| Want Claude to help write queries or migrations | **MCP** |
| Exploring an unfamiliar database with an AI assistant | **MCP** |
| Investigating a production issue interactively | **MCP** |
| Quick one-off query or schema check | **CLI** |
| Data export in a shell script or CI pipeline | **CLI** |
| Bulk insert/update/delete without writing code | **CLI** |
