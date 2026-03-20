//! Row mapping: `FromQueryRow`, streaming, and lightweight output (json / csv).
//!
//! Demonstrates the Polars-free API tier:
//!
//! 1. Implementing [`FromQueryRow`] for a concrete domain struct
//! 2. [`DbAdapterExt::execute_query_mapped`] — collect typed `Vec<T>` in one call
//! 3. [`DbAdapter::execute_query_stream`] — process rows one at a time (backpressure-friendly)
//! 4. [`DbAdapterOutputExt::execute_query_json`] — rows as `Vec<serde_json::Value>`
//! 5. [`DbAdapterOutputExt::execute_query_csv`] — stream directly into a `Write` impl
//!
//! Run with:
//!
//! ```bash
//! cargo run --example row_mapping -p arni --features duckdb,json,csv-output
//! ```
//!
//! No `polars` feature needed — this example has **zero Polars dependency**.

use std::collections::HashMap;

use arni::{
    adapter::DbAdapterExt,
    adapters::duckdb::DuckDbAdapter,
    output::DbAdapterOutputExt,
    ConnectionConfig, DataError, DatabaseType, DbAdapter, FromQueryRow, QueryValue,
};
use futures_util::StreamExt;

// ─── Domain type ─────────────────────────────────────────────────────────────

/// A simple user record — the type we want to bind database rows to.
#[derive(Debug)]
struct User {
    id: i64,
    name: String,
    score: f64,
}

/// Implement [`FromQueryRow`] to teach arni how to convert a raw row into `User`.
///
/// The library calls `from_row` once per row; the row is passed as an owned
/// `Vec<QueryValue>`. Return `Err(DataError::TypeConversion(…))` when a column
/// is missing or has an unexpected type.
impl FromQueryRow for User {
    fn from_row(row: Vec<QueryValue>) -> Result<Self, DataError> {
        let id = match row.get(0) {
            Some(QueryValue::Int(n)) => *n,
            _ => return Err(DataError::TypeConversion("expected Int at column 0 (id)".into())),
        };
        let name = match row.get(1) {
            Some(QueryValue::Text(s)) => s.clone(),
            _ => {
                return Err(DataError::TypeConversion(
                    "expected Text at column 1 (name)".into(),
                ))
            }
        };
        let score = match row.get(2) {
            Some(QueryValue::Float(f)) => *f,
            Some(QueryValue::Int(i)) => *i as f64, // DuckDB may return INTEGER for whole numbers
            _ => {
                return Err(DataError::TypeConversion(
                    "expected Float at column 2 (score)".into(),
                ))
            }
        };
        Ok(User { id, name, score })
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Connect to an in-memory DuckDB ────────────────────────────────────
    let config = ConnectionConfig {
        id: "row-mapping-example".into(),
        name: "Row Mapping Example".into(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".into(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;
    println!("Connected to in-memory DuckDB.\n");

    // ── 2. Seed some data (using the baseline QueryResult API) ───────────────
    adapter
        .execute_query("CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE)")
        .await?;
    adapter
        .execute_query(
            "INSERT INTO users VALUES \
             (1, 'Alice', 92.5), \
             (2, 'Bob',   87.0), \
             (3, 'Carol', 95.1), \
             (4, 'Dave',  78.3)",
        )
        .await?;

    // ── 3. execute_query_mapped — typed Vec<User> in one call ────────────────
    println!("=== execute_query_mapped ===");
    let users: Vec<User> = adapter
        .execute_query_mapped("SELECT id, name, score FROM users ORDER BY score DESC")
        .await?;

    for u in &users {
        println!("  {:2}. {:<8} score={:.1}", u.id, u.name, u.score);
    }
    println!();

    // ── 4. execute_query_stream — process rows one at a time ─────────────────
    println!("=== execute_query_stream (manual) ===");
    let mut stream = adapter
        .execute_query_stream("SELECT id, name, score FROM users WHERE score >= 90.0")
        .await?;

    while let Some(row_result) = stream.next().await {
        let row = row_result?;
        let user = User::from_row(row)?;
        println!("  High scorer: {} ({:.1})", user.name, user.score);
    }
    println!();

    // ── 5. execute_query_json — rows as serde_json::Value objects ─────────────
    #[cfg(feature = "json")]
    {
        println!("=== execute_query_json ===");
        let rows = adapter
            .execute_query_json("SELECT id, name, score FROM users ORDER BY id")
            .await?;

        for row in &rows {
            println!("  {}", serde_json::to_string(row)?);
        }
        println!();
    }

    // ── 6. execute_query_csv — stream into a Vec<u8> (or any Write) ──────────
    #[cfg(feature = "csv-output")]
    {
        println!("=== execute_query_csv ===");
        let mut csv_buf = Vec::<u8>::new();
        adapter
            .execute_query_csv(
                "SELECT id, name, score FROM users ORDER BY score DESC",
                &mut csv_buf,
            )
            .await?;

        print!("{}", String::from_utf8(csv_buf)?);
        println!();
    }

    println!("Done. No Polars required.");
    Ok(())
}
