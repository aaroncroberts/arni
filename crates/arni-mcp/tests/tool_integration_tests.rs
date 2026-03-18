//! Integration tests for all 14 arni-mcp tool handlers.
//!
//! These tests exercise the real `ArniMcpServer` tool methods end-to-end
//! against an in-memory DuckDB database — no external server required.
//!
//! Strategy:
//! 1. Build an `ArniConfig` that points a named profile at DuckDB `:memory:`.
//! 2. Construct `ArniMcpServer` with that config and an empty `ConnectionRegistry`.
//! 3. Call each `pub async fn` tool method directly, bypassing the MCP transport.
//!    (`Parameters<T>` is a plain newtype — `Parameters(params_struct)` is all it
//!    takes to construct one.)
//! 4. Extract the JSON payload from the returned `Content` and assert structure.

use std::collections::HashMap;
use std::sync::Arc;

use arni::{ArniConfig, ConfigProfile, ConnectionConfig, ConnectionRegistry, DatabaseType};
use arni_mcp::ArniMcpServer;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::Content;
use serde_json::Value;

use arni_mcp::types::{
    BulkDeleteParams, BulkInsertParams, BulkUpdateParams, ExecuteParams, FindTablesParams,
    ProfileParams, QueryParams, SchemaParams, TableParams,
};

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Profile name used across all tests.
const PROFILE: &str = "test-duckdb";

/// Build a server backed by an in-memory DuckDB database.
///
/// The DuckDB adapter is lazy-connected on first tool call via `get_or_connect`,
/// so no explicit connection step is needed here.
fn make_server() -> ArniMcpServer {
    let mut params = HashMap::new();
    // DuckDB has no password, but some adapters read from parameters["password"].
    // Leave it empty — the adapter ignores it for DuckDB.
    params.insert("password".to_string(), String::new());

    let conn = ConnectionConfig {
        id: "duckdb-mem".to_string(),
        name: "In-memory DuckDB".to_string(),
        db_type: DatabaseType::DuckDB,
        host: None,
        port: None,
        database: ":memory:".to_string(),
        username: None,
        use_ssl: false,
        parameters: params,
        pool_config: None,
    };

    let mut profile = ConfigProfile::new();
    profile.add_connection(conn);

    let mut config = ArniConfig::new();
    config.default_profile = PROFILE.to_string();
    config.profiles.insert(PROFILE.to_string(), profile);

    let registry = Arc::new(ConnectionRegistry::new());
    ArniMcpServer::new(registry, Arc::new(config))
}

/// Extract the JSON value from a successful tool `Content` response.
///
/// All arni-mcp tools return `Content::json(...)` which serialises to a
/// `Content { raw: RawContent::Text { text: "<json>" } }`.
fn content_to_json(content: Content) -> Value {
    let text = &content.raw.as_text().expect("Content is not text").text;
    serde_json::from_str(text).expect("Content text is not valid JSON")
}

/// Create the `events` table used by multiple tests.
async fn create_events_table(server: &ArniMcpServer) {
    let ddl = "CREATE TABLE IF NOT EXISTS events (\
        id INTEGER PRIMARY KEY, \
        name VARCHAR NOT NULL, \
        active BOOLEAN DEFAULT TRUE, \
        score DOUBLE\
    )";
    server
        .execute(Parameters(ExecuteParams {
            profile: PROFILE.to_string(),
            sql: ddl.to_string(),
        }))
        .await
        .expect("CREATE TABLE events failed");
}

// ── Core tools ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_returns_columns_and_rows() {
    let server = make_server();
    let result = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT 42 AS n, 'hello' AS s".to_string(),
        }))
        .await
        .expect("query failed");

    let json = content_to_json(result);
    let columns = json["columns"].as_array().unwrap();
    let rows = json["rows"].as_array().unwrap();

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0], "n");
    assert_eq!(columns[1], "s");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], 42);
    assert_eq!(rows[0][1], "hello");
}

#[tokio::test]
async fn query_empty_result_has_columns_but_no_rows() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT id, name FROM events WHERE 1=0".to_string(),
        }))
        .await
        .expect("query failed");

    let json = content_to_json(result);
    assert!(!json["columns"].as_array().unwrap().is_empty());
    assert!(json["rows"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn execute_create_and_insert_returns_rows_affected() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .execute(Parameters(ExecuteParams {
            profile: PROFILE.to_string(),
            sql: "INSERT INTO events (id, name, score) VALUES (1, 'alpha', 1.5), (2, 'beta', 2.5)"
                .to_string(),
        }))
        .await
        .expect("execute failed");

    let json = content_to_json(result);
    // DuckDB's execute_query path returns rows_affected=null for INSERT/DDL;
    // the key must be present in the response (null is a valid value here).
    assert!(json.get("rows_affected").is_some());
}

// ── Schema introspection ──────────────────────────────────────────────────────

#[tokio::test]
async fn tables_lists_created_table() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .tables(Parameters(ProfileParams {
            profile: PROFILE.to_string(),
        }))
        .await
        .expect("tables failed");

    let json = content_to_json(result);
    let tables: Vec<String> = json["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_lowercase())
        .collect();

    assert!(
        tables.contains(&"events".to_string()),
        "expected 'events' in {tables:?}"
    );
}

#[tokio::test]
async fn describe_table_returns_correct_columns() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .describe_table(Parameters(TableParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            schema: None,
        }))
        .await
        .expect("describe_table failed");

    let json = content_to_json(result);
    let cols = json["columns"].as_array().expect("no columns array");
    let col_names: Vec<&str> = cols
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();

    assert!(col_names.contains(&"id"), "missing 'id' column");
    assert!(col_names.contains(&"name"), "missing 'name' column");
    assert!(col_names.contains(&"score"), "missing 'score' column");

    // Each column entry must have the expected shape.
    let id_col = cols.iter().find(|c| c["name"] == "id").unwrap();
    assert!(id_col.get("data_type").is_some(), "missing data_type");
    assert!(id_col.get("nullable").is_some(), "missing nullable");
    // Note: DuckDB doesn't surface PK metadata via PRAGMA/information_schema
    // in the same way SQL databases do, so is_primary_key may be false here.
}

#[tokio::test]
async fn list_databases_returns_nonempty_list() {
    let server = make_server();

    let result = server
        .list_databases(Parameters(ProfileParams {
            profile: PROFILE.to_string(),
        }))
        .await
        .expect("list_databases failed");

    let json = content_to_json(result);
    let dbs = json["databases"].as_array().unwrap();
    assert!(!dbs.is_empty(), "expected at least one database");
}

#[tokio::test]
async fn get_server_info_contains_duckdb() {
    let server = make_server();

    let result = server
        .get_server_info(Parameters(ProfileParams {
            profile: PROFILE.to_string(),
        }))
        .await
        .expect("get_server_info failed");

    let json = content_to_json(result);
    let server_type = json["server_type"].as_str().unwrap_or("");
    let version = json["version"].as_str().unwrap_or("");

    assert!(
        server_type.to_lowercase().contains("duckdb")
            || version.to_lowercase().contains("duckdb"),
        "expected DuckDB in server info, got: {json}"
    );
}

#[tokio::test]
async fn get_views_returns_list() {
    let server = make_server();

    // DuckDB supports views; an empty list is still a valid response.
    let result = server
        .get_views(Parameters(SchemaParams {
            profile: PROFILE.to_string(),
            schema: None,
        }))
        .await
        .expect("get_views failed");

    let json = content_to_json(result);
    assert!(json.is_array() || json.is_object(), "expected JSON response");
}

#[tokio::test]
async fn get_indexes_returns_list() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .get_indexes(Parameters(TableParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            schema: None,
        }))
        .await
        .expect("get_indexes failed");

    let json = content_to_json(result);
    assert!(json.is_array() || json.is_object(), "expected JSON response");
}

#[tokio::test]
async fn get_foreign_keys_returns_list() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .get_foreign_keys(Parameters(TableParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            schema: None,
        }))
        .await
        .expect("get_foreign_keys failed");

    let json = content_to_json(result);
    assert!(json.is_array() || json.is_object(), "expected JSON response");
}

#[tokio::test]
async fn list_stored_procedures_returns_list() {
    let server = make_server();

    // DuckDB has no stored procedures; empty list is valid.
    let result = server
        .list_stored_procedures(Parameters(SchemaParams {
            profile: PROFILE.to_string(),
            schema: None,
        }))
        .await
        .expect("list_stored_procedures failed");

    let json = content_to_json(result);
    assert!(json.is_array() || json.is_object(), "expected JSON response");
}

#[tokio::test]
async fn find_tables_contains_mode() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .find_tables(Parameters(FindTablesParams {
            profile: PROFILE.to_string(),
            pattern: "event".to_string(),
            mode: Some("contains".to_string()),
            schema: None,
        }))
        .await
        .expect("find_tables failed");

    let json = content_to_json(result);
    let tables: Vec<String> = json["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_lowercase())
        .collect();

    assert!(
        tables.contains(&"events".to_string()),
        "'events' not found with contains='event': {tables:?}"
    );
}

#[tokio::test]
async fn find_tables_starts_mode() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .find_tables(Parameters(FindTablesParams {
            profile: PROFILE.to_string(),
            pattern: "eve".to_string(),
            mode: Some("starts".to_string()),
            schema: None,
        }))
        .await
        .expect("find_tables starts failed");

    let json = content_to_json(result);
    let tables: Vec<String> = json["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_lowercase())
        .collect();

    assert!(tables.contains(&"events".to_string()));
}

// ── Bulk operations ───────────────────────────────────────────────────────────

#[tokio::test]
async fn bulk_insert_inserts_rows_and_query_finds_them() {
    let server = make_server();
    create_events_table(&server).await;

    let result = server
        .bulk_insert(Parameters(BulkInsertParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "score".to_string()],
            rows: vec![
                vec![
                    serde_json::json!(10),
                    serde_json::json!("gamma"),
                    serde_json::json!(9.9),
                ],
                vec![
                    serde_json::json!(11),
                    serde_json::json!("delta"),
                    serde_json::json!(8.8),
                ],
            ],
            schema: None,
        }))
        .await
        .expect("bulk_insert failed");

    // bulk_insert goes through execute_statement_blocking which does count rows.
    let insert_json = content_to_json(result);
    assert_eq!(insert_json["rows_affected"], 2);


    // Verify rows are actually in the database.
    let query_result = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT name FROM events WHERE id IN (10, 11) ORDER BY id".to_string(),
        }))
        .await
        .expect("verify query failed");

    let qjson = content_to_json(query_result);
    let rows = qjson["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "gamma");
    assert_eq!(rows[1][0], "delta");
}

#[tokio::test]
async fn bulk_update_changes_matching_rows() {
    let server = make_server();
    create_events_table(&server).await;

    // Seed rows
    server
        .execute(Parameters(ExecuteParams {
            profile: PROFILE.to_string(),
            sql: "INSERT INTO events (id, name, score) VALUES (20, 'old-name', 1.0)".to_string(),
        }))
        .await
        .unwrap();

    // Update using eq filter
    let result = server
        .bulk_update(Parameters(BulkUpdateParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            filter: serde_json::json!({"id": {"eq": 20}}),
            values: serde_json::json!({"name": "new-name", "score": 99.0}),
            schema: None,
        }))
        .await
        .expect("bulk_update failed");

    let json = content_to_json(result);
    assert_eq!(json["rows_affected"], 1);

    // Verify the update
    let verify = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT name, score FROM events WHERE id = 20".to_string(),
        }))
        .await
        .unwrap();

    let vjson = content_to_json(verify);
    let row = &vjson["rows"].as_array().unwrap()[0];
    assert_eq!(row[0], "new-name");
    assert_eq!(row[1], 99.0);
}

#[tokio::test]
async fn bulk_delete_removes_matching_rows() {
    let server = make_server();
    create_events_table(&server).await;

    // Seed rows
    server
        .execute(Parameters(ExecuteParams {
            profile: PROFILE.to_string(),
            sql: "INSERT INTO events (id, name) VALUES (30, 'to-delete'), (31, 'keep-me')"
                .to_string(),
        }))
        .await
        .unwrap();

    // Delete with eq filter
    let result = server
        .bulk_delete(Parameters(BulkDeleteParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            filter: serde_json::json!({"id": {"eq": 30}}),
            schema: None,
        }))
        .await
        .expect("bulk_delete failed");

    let json = content_to_json(result);
    assert_eq!(json["rows_affected"], 1);

    // Verify id=30 is gone, id=31 remains
    let verify = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT id FROM events WHERE id IN (30, 31) ORDER BY id".to_string(),
        }))
        .await
        .unwrap();

    let vjson = content_to_json(verify);
    let rows = vjson["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], 31);
}

#[tokio::test]
async fn bulk_delete_with_in_filter_removes_multiple_rows() {
    let server = make_server();
    create_events_table(&server).await;

    server
        .execute(Parameters(ExecuteParams {
            profile: PROFILE.to_string(),
            sql: "INSERT INTO events (id, name) VALUES (40, 'a'), (41, 'b'), (42, 'c')".to_string(),
        }))
        .await
        .unwrap();

    let result = server
        .bulk_delete(Parameters(BulkDeleteParams {
            profile: PROFILE.to_string(),
            table: "events".to_string(),
            filter: serde_json::json!({"id": {"in": [40, 41]}}),
            schema: None,
        }))
        .await
        .expect("bulk_delete with IN failed");

    let json = content_to_json(result);
    assert_eq!(json["rows_affected"], 2);
}

// ── Error cases ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn query_bad_sql_returns_err() {
    let server = make_server();

    let result = server
        .query(Parameters(QueryParams {
            profile: PROFILE.to_string(),
            sql: "SELECT * FROM nonexistent_table_xyz".to_string(),
        }))
        .await;

    assert!(result.is_err(), "expected Err for bad SQL");
}

#[tokio::test]
async fn tool_call_unknown_profile_returns_err() {
    let server = make_server();

    let result = server
        .tables(Parameters(ProfileParams {
            profile: "no-such-profile".to_string(),
        }))
        .await;

    assert!(result.is_err(), "expected Err for unknown profile");
    assert!(
        result.unwrap_err().contains("not found"),
        "error should mention 'not found'"
    );
}
