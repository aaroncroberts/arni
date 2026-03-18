//! Quick-start: connect to DuckDB in-memory, write a DataFrame, query it back.
//!
//! Demonstrates the core arni-data API using a zero-setup in-memory DuckDB database.
//! The same `DbAdapter` trait works identically for PostgreSQL, MySQL, Oracle, and
//! every other supported backend — only the config and adapter type change.
//!
//! Run with:
//!   cargo run --example quickstart -p arni-data --features duckdb

use std::collections::HashMap;

use arni::{adapters::duckdb::DuckDbAdapter, ConnectionConfig, DatabaseType, DbAdapter};
use polars::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Connect ───────────────────────────────────────────────────────────
    let config = ConnectionConfig {
        id: "quickstart".to_string(),
        name: "Quick Start".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;
    println!("Connected to in-memory DuckDB.");

    // ── 2. Write a Polars DataFrame into the database ────────────────────────
    let df = df![
        "id"    => &[1i64, 2, 3],
        "name"  => &["Alice", "Bob", "Carol"],
        "score" => &[92.5f64, 87.0, 95.1],
    ]?;

    // replace=true creates the table on first run; replace=false appends.
    adapter.export_dataframe(&df, "players", None, true).await?;
    println!("Wrote {} rows to 'players'.", df.height());

    // ── 3. Query it back — result is a Polars DataFrame ──────────────────────
    let result = adapter
        .query_df("SELECT * FROM players ORDER BY score DESC")
        .await?;

    println!("\n{result}");
    // shape: (3, 3)
    // ┌─────┬───────┬───────┐
    // │ id  ┆ name  ┆ score │
    // │ --- ┆ ---   ┆ ---   │
    // │ i64 ┆ str   ┆ f64   │
    // ╞═════╪═══════╪═══════╡
    // │ 3   ┆ Carol ┆ 95.1  │
    // │ 1   ┆ Alice ┆ 92.5  │
    // │ 2   ┆ Bob   ┆ 87.0  │
    // └─────┴───────┴───────┘

    // ── 4. Introspect schema ─────────────────────────────────────────────────
    let info = adapter.describe_table("players", None).await?;
    println!(
        "Table '{}': {} columns, {} rows",
        info.name,
        info.columns.len(),
        info.row_count.unwrap_or(0),
    );
    for col in &info.columns {
        println!(
            "  {:<10} {:<12} nullable={}",
            col.name, col.data_type, col.nullable
        );
    }

    Ok(())
}
