//! Analytics example: in-memory DuckDB for exploratory data analysis.
//!
//! Demonstrates the full read/write/introspect cycle that arni-data enables
//! for an analytics workflow — entirely in-process, no server required.
//!
//! Concepts shown:
//!  - `export_dataframe`  — write a Polars DataFrame into a table
//!  - `query_df`          — run arbitrary SQL and receive a DataFrame
//!  - `bulk_insert`       — append strongly-typed rows via `QueryValue`
//!  - `bulk_update`       — update rows using a `FilterExpr` predicate
//!  - `bulk_delete`       — remove rows using a `FilterExpr` predicate
//!  - `describe_table`    — inspect schema and row count
//!  - `get_server_info`   — read engine version metadata
//!
//! Run with:
//!   cargo run --example analytics -p arni-data --features duckdb

use std::collections::HashMap;

use arni_data::{
    adapters::duckdb::DuckDbAdapter, ConnectionConfig, DatabaseType, DbAdapter, FilterExpr,
    QueryValue,
};
use polars::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Connect ───────────────────────────────────────────────────────────
    let config = ConnectionConfig {
        id: "analytics".to_string(),
        name: "Analytics Example".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
    };

    let mut adapter = DuckDbAdapter::new(config.clone());
    adapter.connect(&config, None).await?;

    let server = adapter.get_server_info().await?;
    println!("Engine: {} {}", server.server_type, server.version);

    // ── 2. Seed data via export_dataframe ────────────────────────────────────
    let sales = df![
        "region"   => ["North", "South", "East", "West", "North", "South"],
        "product"  => ["Widget", "Gadget", "Widget", "Gadget", "Gadget", "Widget"],
        "qty"      => [120i64, 85, 200, 150, 95, 60],
        "revenue"  => [1200.0f64, 1275.0, 2000.0, 3000.0, 1425.0, 900.0],
    ]?;

    adapter
        .export_dataframe(&sales, "sales", None, true)
        .await?;
    println!("Seeded {} rows into 'sales'.", sales.height());

    // ── 3. SQL analytics — group by ──────────────────────────────────────────
    let by_region = adapter
        .query_df(
            "SELECT region,
                    SUM(qty)     AS total_qty,
                    SUM(revenue) AS total_revenue,
                    ROUND(SUM(revenue) / SUM(qty), 2) AS avg_price
             FROM sales
             GROUP BY region
             ORDER BY total_revenue DESC",
        )
        .await?;

    println!("\nRevenue by region:");
    println!("{by_region}");

    // ── 4. SQL analytics — window function ───────────────────────────────────
    let ranked = adapter
        .query_df(
            "SELECT region, product, revenue,
                    RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rank
             FROM sales",
        )
        .await?;

    println!("\nRank within region:");
    println!("{ranked}");

    // ── 5. bulk_insert — append new rows ─────────────────────────────────────
    let new_columns = vec![
        "region".to_string(),
        "product".to_string(),
        "qty".to_string(),
        "revenue".to_string(),
    ];
    let new_rows = vec![
        vec![
            QueryValue::Text("Central".to_string()),
            QueryValue::Text("Widget".to_string()),
            QueryValue::Int(300),
            QueryValue::Float(3000.0),
        ],
        vec![
            QueryValue::Text("Central".to_string()),
            QueryValue::Text("Gadget".to_string()),
            QueryValue::Int(250),
            QueryValue::Float(5000.0),
        ],
    ];

    let inserted = adapter
        .bulk_insert("sales", &new_columns, &new_rows, None)
        .await?;
    println!("\nInserted {inserted} new rows via bulk_insert.");

    // ── 6. bulk_update — apply a discount ────────────────────────────────────
    let discount_filter = FilterExpr::And(vec![
        FilterExpr::Eq(
            "region".to_string(),
            QueryValue::Text("Central".to_string()),
        ),
        FilterExpr::Eq(
            "product".to_string(),
            QueryValue::Text("Gadget".to_string()),
        ),
    ]);
    let mut discount = HashMap::new();
    discount.insert("revenue".to_string(), QueryValue::Float(4500.0)); // 10 % off

    let updated = adapter
        .bulk_update("sales", &[(discount, discount_filter)], None)
        .await?;
    println!("Updated {updated} rows (applied discount).");

    // ── 7. bulk_delete — remove zero-revenue rows ────────────────────────────
    let zero_revenue = FilterExpr::Lte("revenue".to_string(), QueryValue::Float(0.0));
    let deleted = adapter.bulk_delete("sales", &[zero_revenue], None).await?;
    println!("Deleted {deleted} rows with non-positive revenue.");

    // ── 8. Final schema introspection ────────────────────────────────────────
    let info = adapter.describe_table("sales", None).await?;
    println!(
        "\nTable '{}': {} columns, {} rows",
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

    // ── 9. Final aggregation ─────────────────────────────────────────────────
    let totals = adapter
        .query_df("SELECT COUNT(*) AS rows, SUM(revenue) AS total FROM sales")
        .await?;
    println!("\nFinal totals:\n{totals}");

    Ok(())
}
