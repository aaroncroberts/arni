//! Axum HTTP API backed by arni
//!
//! Demonstrates using arni as a library inside an Axum web server.
//! Uses SQLite in-memory by default — no server or credentials required.
//!
//! Routes:
//!   GET  /tables              — list all tables
//!   GET  /query?sql=<SQL>     — execute a SELECT, returns JSON rows
//!   POST /bulk-insert         — insert rows via DbAdapter::bulk_insert
//!
//! Run with:
//!   cargo run -p axum-api
//!
//! To point at a real database, change `make_adapter()` — the routes are
//! completely adapter-agnostic.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::info;

use arni::adapters::sqlite::SqliteAdapter;
use arni::{ConnectionConfig, DatabaseType, DbAdapter, QueryValue, SharedAdapter};

// ── Application state ────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    db: SharedAdapter,
}

// ── Startup ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "axum_api=debug,arni=info,tower_http=info".into()),
        )
        .init();

    let db = make_adapter()
        .await
        .context("Failed to connect to database")?;
    seed_data(&db)
        .await
        .context("Failed to seed initial data")?;

    let state = AppState { db };

    let app = Router::new()
        .route("/tables", get(list_tables))
        .route("/query", get(run_query))
        .route("/bulk-insert", post(bulk_insert))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("arni axum-api listening on http://{addr}");
    info!("  GET  /tables");
    info!("  GET  /query?sql=SELECT+*+FROM+users");
    info!("  POST /bulk-insert   body: {{\"table\":\"users\",\"rows\":[...]}}");
    axum::serve(listener, app).await?;

    Ok(())
}

/// Build and connect the database adapter.
///
/// Default: SQLite in-memory — zero config, works immediately.
///
/// To switch to another database, replace this function:
///
/// ```rust
/// use arni::{adapters::postgres::PostgresAdapter, DatabaseType};
/// let mut adapter = PostgresAdapter::new(ConnectionConfig {
///     id: "api".into(), name: "API DB".into(),
///     db_type: DatabaseType::Postgres,
///     host: Some("localhost".into()), port: Some(5432),
///     database: "myapp".into(), username: Some("myuser".into()),
///     use_ssl: false,
///     parameters: HashMap::from([("password".into(), "mypass".into())]),
///     pool_config: None,
/// });
/// adapter.connect(&adapter.config().clone(), None).await?;
/// ```
async fn make_adapter() -> anyhow::Result<SharedAdapter> {
    let config = ConnectionConfig {
        id: "api-db".into(),
        name: "API SQLite".into(),
        db_type: DatabaseType::SQLite,
        host: None,
        port: None,
        database: ":memory:".into(),
        username: None,
        use_ssl: false,
        parameters: HashMap::new(),
        pool_config: None,
    };

    let mut adapter = SqliteAdapter::new(config.clone());
    adapter.connect(&config, None).await?;
    info!("Connected to in-memory SQLite");
    Ok(Arc::new(adapter))
}

/// Seed the database with a sample table so the API is useful out of the box.
async fn seed_data(db: &SharedAdapter) -> anyhow::Result<()> {
    db.execute_query(
        "CREATE TABLE IF NOT EXISTS users (
            id    INTEGER PRIMARY KEY,
            name  TEXT    NOT NULL,
            email TEXT    NOT NULL,
            score REAL    NOT NULL DEFAULT 0
        )",
    )
    .await?;

    let columns = vec![
        "id".to_string(),
        "name".to_string(),
        "email".to_string(),
        "score".to_string(),
    ];
    let rows = vec![
        vec![
            QueryValue::Int(1),
            QueryValue::Text("Alice".into()),
            QueryValue::Text("alice@example.com".into()),
            QueryValue::Float(92.5),
        ],
        vec![
            QueryValue::Int(2),
            QueryValue::Text("Bob".into()),
            QueryValue::Text("bob@example.com".into()),
            QueryValue::Float(87.0),
        ],
        vec![
            QueryValue::Int(3),
            QueryValue::Text("Carol".into()),
            QueryValue::Text("carol@example.com".into()),
            QueryValue::Float(95.1),
        ],
    ];
    db.bulk_insert("users", &columns, &rows, None).await?;

    info!("Seeded 3 rows into 'users'");
    Ok(())
}

// ── Handlers ─────────────────────────────────────────────────────────────────

/// GET /tables — list all tables in the database
async fn list_tables(State(state): State<AppState>) -> impl IntoResponse {
    info!(endpoint = "/tables", "list_tables");
    match state.db.list_tables(None).await {
        Ok(tables) => Json(json!({ "tables": tables })).into_response(),
        Err(e) => api_error(e.to_string()),
    }
}

/// Query parameters for GET /query
#[derive(Deserialize)]
struct SqlParams {
    sql: String,
}

/// GET /query?sql=<SQL> — execute a SELECT, return rows as JSON
async fn run_query(
    State(state): State<AppState>,
    Query(params): Query<SqlParams>,
) -> impl IntoResponse {
    info!(endpoint = "/query", sql = %params.sql, "run_query");
    match state.db.execute_query(&params.sql).await {
        Ok(result) => {
            let rows: Vec<Value> = result
                .rows
                .iter()
                .map(|row| {
                    let obj: serde_json::Map<String, Value> = result
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.clone(), query_value_to_json(val)))
                        .collect();
                    Value::Object(obj)
                })
                .collect();
            Json(json!({ "columns": result.columns, "rows": rows })).into_response()
        }
        Err(e) => api_error(e.to_string()),
    }
}

/// Request body for POST /bulk-insert
#[derive(Deserialize)]
struct BulkInsertRequest {
    table: String,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

/// Response for POST /bulk-insert
#[derive(Serialize)]
struct BulkInsertResponse {
    inserted: usize,
}

/// POST /bulk-insert — insert rows via DbAdapter::bulk_insert
async fn bulk_insert(
    State(state): State<AppState>,
    Json(body): Json<BulkInsertRequest>,
) -> impl IntoResponse {
    info!(endpoint = "/bulk-insert", table = %body.table, rows = body.rows.len(), "bulk_insert");

    let typed_rows: Vec<Vec<QueryValue>> = body
        .rows
        .iter()
        .map(|row| row.iter().map(json_to_query_value).collect())
        .collect();

    match state
        .db
        .bulk_insert(&body.table, &body.columns, &typed_rows, None)
        .await
    {
        Ok(_) => Json(BulkInsertResponse {
            inserted: typed_rows.len(),
        })
        .into_response(),
        Err(e) => api_error(e.to_string()),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn api_error(message: String) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": message })),
    )
        .into_response()
}

fn query_value_to_json(v: &QueryValue) -> Value {
    match v {
        QueryValue::Null => Value::Null,
        QueryValue::Bool(b) => json!(b),
        QueryValue::Int(i) => json!(i),
        QueryValue::Float(f) => json!(f),
        QueryValue::Text(s) => json!(s),
        QueryValue::Bytes(b) => json!(b),
    }
}

fn json_to_query_value(v: &Value) -> QueryValue {
    match v {
        Value::Null => QueryValue::Null,
        Value::Bool(b) => QueryValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QueryValue::Int(i)
            } else {
                QueryValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => QueryValue::Text(s.clone()),
        _ => QueryValue::Text(v.to_string()),
    }
}
