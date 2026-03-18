//! Multi-adapter example: run the same query against DuckDB and SQLite.
//!
//! Demonstrates that `DbAdapter` is truly backend-agnostic — you can swap one
//! adapter for another without changing a single line of business logic.
//! Both databases are in-memory and require no external server.
//!
//! Concepts shown:
//!  - Identical API calls across two different adapters
//!  - `export_dataframe` — write the same DataFrame to both backends
//!  - `query_df`         — run the same SQL and compare results
//!  - `list_tables`      — inspect the schema of each backend
//!  - `get_server_info`  — show which engine you're talking to
//!
//! Run with:
//!   cargo run --example multi_adapter -p arni-data --features "duckdb sqlite"

use std::collections::HashMap;

use arni::{
    adapters::duckdb::DuckDbAdapter, adapters::sqlite::SqliteAdapter, ConnectionConfig,
    DatabaseType, DbAdapter,
};
use polars::prelude::*;

/// Return the same seed DataFrame every time.
fn seed_data() -> anyhow::Result<DataFrame> {
    let df = df![
        "id"         => [1i64, 2, 3, 4, 5],
        "city"       => ["Oslo", "Reykjavik", "Copenhagen", "Helsinki", "Stockholm"],
        "population" => [693_494i64, 131_136, 794_128, 658_864, 975_551],
        "area_km2"   => [480.8f64, 273.0, 86.4, 715.5, 188.0],
    ]?;
    Ok(df)
}

/// Simple SQL that runs identically on both DuckDB and SQLite.
const QUERY: &str = "SELECT city, population, area_km2 FROM capitals ORDER BY population DESC";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Multi-Adapter Example ===\n");

    // ── DuckDB ───────────────────────────────────────────────────────────────
    let duck_cfg = ConnectionConfig {
        id: "duck".to_string(),
        name: "DuckDB".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    let mut duck = DuckDbAdapter::new(duck_cfg.clone());
    duck.connect(&duck_cfg, None).await?;
    let duck_info = duck.get_server_info().await?;
    println!("Backend 1: {} {}", duck_info.server_type, duck_info.version);

    duck.export_dataframe(&seed_data()?, "capitals", None, true)
        .await?;
    let duck_result = duck.query_df(QUERY).await?;

    // ── SQLite ───────────────────────────────────────────────────────────────
    let sqlite_cfg = ConnectionConfig {
        id: "sqlite".to_string(),
        name: "SQLite".to_string(),
        db_type: DatabaseType::SQLite,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    let mut sqlite = SqliteAdapter::new(sqlite_cfg.clone());
    sqlite.connect(&sqlite_cfg, None).await?;
    let sqlite_info = sqlite.get_server_info().await?;
    println!(
        "Backend 2: {} {}",
        sqlite_info.server_type, sqlite_info.version
    );

    sqlite
        .export_dataframe(&seed_data()?, "capitals", None, true)
        .await?;
    let sqlite_result = sqlite.query_df(QUERY).await?;

    // ── Compare results ──────────────────────────────────────────────────────
    println!("\n── DuckDB raw result ──");
    println!("{duck_result}");
    println!("── SQLite raw result ──");
    println!("{sqlite_result}");

    // Verify both backends returned the same row count and column names.
    assert_eq!(
        duck_result.height(),
        sqlite_result.height(),
        "Row counts differ between DuckDB and SQLite"
    );
    assert_eq!(
        duck_result.get_column_names(),
        sqlite_result.get_column_names(),
        "Column names differ between DuckDB and SQLite"
    );
    println!(
        "\n✓ Both adapters returned {} rows with identical columns.",
        duck_result.height()
    );

    // ── Derive density in Polars (adapter-agnostic) ──────────────────────────
    // compute density from the DuckDB result using Polars lazy API
    let with_density = duck_result
        .lazy()
        .with_column((col("population").cast(DataType::Float64) / col("area_km2")).alias("density"))
        .sort(
            ["density"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()?;

    println!("\n── Population density (people / km²) from DuckDB result ──");
    println!("{with_density}");

    // ── Schema introspection ─────────────────────────────────────────────────
    println!("\n── Table lists ──");
    let duck_tables = duck.list_tables(None).await?;
    let sqlite_tables = sqlite.list_tables(None).await?;
    println!("DuckDB  tables: {duck_tables:?}");
    println!("SQLite  tables: {sqlite_tables:?}");

    Ok(())
}
