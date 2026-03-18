//! Live-database integration tests for arni-mcp tool handlers.
//!
//! Each test calls the real `ArniMcpServer` tool methods against a running
//! database container, exercising adapter-specific features that the in-memory
//! DuckDB tests (tool_integration_tests.rs) cannot cover — primary-key
//! detection, real indexes and foreign keys, stored procedures, and the MongoDB
//! JSON query format.
//!
//! # Running locally
//!
//! Start the dev containers first, then export the availability flag for each
//! database you want to test:
//!
//! ```bash
//! podman-compose up -d
//!
//! # Pick any combination:
//! export TEST_POSTGRES_AVAILABLE=true
//! export TEST_MYSQL_AVAILABLE=true
//! export TEST_MSSQL_AVAILABLE=true
//! export TEST_MONGODB_AVAILABLE=true
//!
//! cargo test -p arni-mcp --test live_db_tests
//! ```
//!
//! # CI behaviour
//!
//! The CI workflow runs `cargo test --workspace --lib`, which only executes
//! in-module unit tests.  Files under `tests/` are never compiled in that
//! mode, so these tests are completely invisible to CI regardless of the
//! availability flags.  No `#[ignore]` attributes are needed.
//!
//! # Test isolation
//!
//! Each test creates tables prefixed with `mcp_` (e.g. `mcp_pg_items`) via
//! `CREATE TABLE IF NOT EXISTS`, making individual tests idempotent across
//! repeated runs.  Tests do not depend on execution order and do not clean up
//! after themselves — the containers are ephemeral dev environments.

use std::collections::HashMap;
use std::sync::Arc;

use arni::{ArniConfig, ConfigProfile, ConnectionConfig, ConnectionRegistry, DatabaseType};
use arni_mcp::types::{
    BulkDeleteParams, BulkInsertParams, ExecuteParams, FindTablesParams, ProfileParams,
    QueryParams, SchemaParams, TableParams,
};
use arni_mcp::ArniMcpServer;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::Content;
use serde_json::{json, Value};

// ── Shared helpers ────────────────────────────────────────────────────────────

mod helpers {
    use super::*;

    // ── Container constants (must match compose.yml) ──────────────────────────

    pub mod pg {
        pub const HOST: &str = "localhost";
        pub const PORT: u16 = 5432;
        pub const DATABASE: &str = "test_db";
        pub const USER: &str = "test_user";
        pub const PASSWORD: &str = "test_password";
        pub const AVAILABLE_KEY: &str = "postgres";
    }

    pub mod mysql {
        pub const HOST: &str = "localhost";
        pub const PORT: u16 = 3306;
        pub const DATABASE: &str = "test_db";
        pub const USER: &str = "test_user";
        pub const PASSWORD: &str = "test_password";
        pub const AVAILABLE_KEY: &str = "mysql";
    }

    pub mod mssql {
        pub const HOST: &str = "localhost";
        // Host-side port mapped in compose.yml (container listens on 1433)
        pub const PORT: u16 = 1434;
        // Connect to test_db where the init script seeded the users table
        pub const DATABASE: &str = "test_db";
        pub const USER: &str = "sa";
        pub const PASSWORD: &str = "Test_Password123!";
        pub const AVAILABLE_KEY: &str = "mssql";
    }

    pub mod mongodb {
        pub const HOST: &str = "localhost";
        // Host-side port mapped in compose.yml (container listens on 27017)
        pub const PORT: u16 = 27018;
        pub const DATABASE: &str = "test_db";
        pub const USER: &str = "test_user";
        pub const PASSWORD: &str = "test_password";
        pub const AVAILABLE_KEY: &str = "mongodb";
    }

    // ── Availability guard ────────────────────────────────────────────────────

    /// Returns `true` when `TEST_<DB>_AVAILABLE=true` (or `1`) is set.
    pub fn is_available(db: &str) -> bool {
        let key = format!("TEST_{}_AVAILABLE", db.to_uppercase().replace('-', "_"));
        matches!(
            std::env::var(&key).as_deref(),
            Ok("true") | Ok("1") | Ok("yes")
        )
    }

    // ── Config factory ────────────────────────────────────────────────────────

    fn build_config(
        db_type: DatabaseType,
        id: &str,
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> ConnectionConfig {
        let mut parameters = HashMap::new();
        parameters.insert("password".to_string(), password.to_string());
        ConnectionConfig {
            id: id.to_string(),
            name: id.to_string(),
            db_type,
            host: Some(host.to_string()),
            port: Some(port),
            database: database.to_string(),
            username: Some(username.to_string()),
            use_ssl: false,
            parameters,
            pool_config: None,
        }
    }

    pub fn postgres_config() -> ConnectionConfig {
        build_config(
            DatabaseType::Postgres,
            "pg-live",
            pg::HOST,
            pg::PORT,
            pg::DATABASE,
            pg::USER,
            pg::PASSWORD,
        )
    }

    pub fn mysql_config() -> ConnectionConfig {
        build_config(
            DatabaseType::MySQL,
            "mysql-live",
            mysql::HOST,
            mysql::PORT,
            mysql::DATABASE,
            mysql::USER,
            mysql::PASSWORD,
        )
    }

    pub fn mssql_config() -> ConnectionConfig {
        build_config(
            DatabaseType::SQLServer,
            "mssql-live",
            mssql::HOST,
            mssql::PORT,
            mssql::DATABASE,
            mssql::USER,
            mssql::PASSWORD,
        )
    }

    pub fn mongodb_config() -> ConnectionConfig {
        build_config(
            DatabaseType::MongoDB,
            "mongo-live",
            mongodb::HOST,
            mongodb::PORT,
            mongodb::DATABASE,
            mongodb::USER,
            mongodb::PASSWORD,
        )
    }

    // ── Server factory ────────────────────────────────────────────────────────

    /// Build an `ArniMcpServer` with a single profile backed by `cfg`.
    ///
    /// The adapter is lazy-connected on the first tool call via
    /// `get_or_connect`, so no explicit connect step is required.
    pub fn make_server(cfg: ConnectionConfig, profile: &str) -> ArniMcpServer {
        let mut config_profile = ConfigProfile::new();
        config_profile.add_connection(cfg);

        let mut arni_config = ArniConfig::new();
        arni_config.default_profile = profile.to_string();
        arni_config
            .profiles
            .insert(profile.to_string(), config_profile);

        ArniMcpServer::new(Arc::new(ConnectionRegistry::new()), Arc::new(arni_config))
    }

    // ── Response helper ───────────────────────────────────────────────────────

    /// Extract the JSON payload from a successful tool `Content` response.
    pub fn content_to_json(content: Content) -> Value {
        let text = &content.raw.as_text().expect("Content is not text").text;
        serde_json::from_str(text).expect("Content text is not valid JSON")
    }
}

// ── PostgreSQL ────────────────────────────────────────────────────────────────

mod postgres {
    use super::helpers::{self, pg};
    use super::*;

    const PROFILE: &str = "pg-live-test";

    fn server() -> ArniMcpServer {
        helpers::make_server(helpers::postgres_config(), PROFILE)
    }

    /// Basic smoke test: SELECT from the pre-seeded users table.
    #[tokio::test]
    async fn query_returns_seeded_users() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: "SELECT id, name, email FROM users ORDER BY id".to_string(),
            }))
            .await
            .expect("query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 5, "init script seeds exactly 5 users");
        let cols = json["columns"].as_array().expect("columns must be array");
        assert!(
            cols.iter().any(|c| c.as_str() == Some("name")),
            "columns must include 'name'"
        );
    }

    /// PostgreSQL surfaces primary-key information in `describe_table`.
    /// DuckDB always returns `is_primary_key: false`, so this test is
    /// PostgreSQL-specific.
    #[tokio::test]
    async fn describe_table_id_column_is_primary_key() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .describe_table(Parameters(TableParams {
                profile: PROFILE.to_string(),
                table: "users".to_string(),
                schema: None,
            }))
            .await
            .expect("describe_table should succeed");

        let json = helpers::content_to_json(content);
        let columns = json["columns"].as_array().expect("columns must be array");
        let id_col = columns
            .iter()
            .find(|c| c["name"].as_str() == Some("id"))
            .expect("users must have an id column");
        assert_eq!(
            id_col["is_primary_key"], true,
            "users.id must be reported as primary key on PostgreSQL"
        );
    }

    /// Indexes created by the init script must be visible via the MCP tool.
    ///
    /// `get_indexes` takes a `TableParams` (specific table) and returns a
    /// flat JSON array of `IndexInfo` objects — not a wrapped object.
    #[tokio::test]
    async fn get_indexes_includes_init_script_indexes() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_indexes(Parameters(TableParams {
                profile: PROFILE.to_string(),
                table: "users".to_string(),
                schema: None,
            }))
            .await
            .expect("get_indexes should succeed");

        let json = helpers::content_to_json(content);
        // Response is a direct JSON array of IndexInfo objects
        let indexes = json.as_array().expect("get_indexes returns a JSON array");
        let names: Vec<&str> = indexes.iter().filter_map(|i| i["name"].as_str()).collect();
        assert!(
            names.contains(&"idx_users_email"),
            "idx_users_email must be listed; got: {names:?}"
        );
        assert!(
            names.contains(&"idx_users_active"),
            "idx_users_active must be listed; got: {names:?}"
        );
    }

    /// The server-info tool must identify the database engine as PostgreSQL.
    #[tokio::test]
    async fn get_server_info_identifies_postgres() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_server_info(Parameters(ProfileParams {
                profile: PROFILE.to_string(),
            }))
            .await
            .expect("get_server_info should succeed");

        let json = helpers::content_to_json(content);
        let server_type = json["server_type"].as_str().unwrap_or("").to_lowercase();
        assert!(
            server_type.contains("postgres"),
            "server_type must mention postgres; got: {server_type}"
        );
    }

    /// `get_foreign_keys` must find a FK created within this test.
    ///
    /// Creates a parent and child table, adds a foreign key constraint, then
    /// calls the MCP tool and verifies the FK appears in the response.
    #[tokio::test]
    async fn get_foreign_keys_finds_created_fk() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();

        // Create parent table
        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "CREATE TABLE IF NOT EXISTS mcp_pg_parent \
                      (id SERIAL PRIMARY KEY, label TEXT NOT NULL)"
                    .to_string(),
            }))
            .await
            .expect("CREATE mcp_pg_parent should succeed");

        // Create child table with FK referencing parent
        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "CREATE TABLE IF NOT EXISTS mcp_pg_child \
                      (id SERIAL PRIMARY KEY, parent_id INT \
                       REFERENCES mcp_pg_parent(id) ON DELETE CASCADE)"
                    .to_string(),
            }))
            .await
            .expect("CREATE mcp_pg_child should succeed");

        // `get_foreign_keys` takes a TableParams and returns a flat JSON array
        let content = server
            .get_foreign_keys(Parameters(TableParams {
                profile: PROFILE.to_string(),
                table: "mcp_pg_child".to_string(),
                schema: None,
            }))
            .await
            .expect("get_foreign_keys should succeed");

        let json = helpers::content_to_json(content);
        // Response is a direct JSON array of ForeignKeyInfo objects
        let fks = json
            .as_array()
            .expect("get_foreign_keys returns a JSON array");
        // ForeignKeyInfo uses `table_name` (not `table`) and `referenced_table`
        let found = fks.iter().any(|fk| {
            fk["table_name"].as_str() == Some("mcp_pg_child")
                || fk["referenced_table"].as_str() == Some("mcp_pg_parent")
        });
        assert!(
            found,
            "mcp_pg_child FK to mcp_pg_parent must appear in get_foreign_keys; got: {fks:?}"
        );
    }

    /// `bulk_insert` inserts rows, then `query` reads them back.
    ///
    /// Exercises the complete write-then-read path through MCP tools.
    #[tokio::test]
    async fn bulk_insert_and_query_round_trip() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();

        // Ensure the test table exists
        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "CREATE TABLE IF NOT EXISTS mcp_pg_items \
                      (id INTEGER, label TEXT)"
                    .to_string(),
            }))
            .await
            .expect("CREATE TABLE mcp_pg_items should succeed");

        // Clear any rows from previous runs so the count assertion is reliable
        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "DELETE FROM mcp_pg_items WHERE label = 'mcp-round-trip'".to_string(),
            }))
            .await
            .expect("DELETE should succeed");

        // Insert via MCP bulk_insert
        server
            .bulk_insert(Parameters(BulkInsertParams {
                profile: PROFILE.to_string(),
                table: "mcp_pg_items".to_string(),
                columns: vec!["id".to_string(), "label".to_string()],
                rows: vec![
                    vec![json!(1), json!("mcp-round-trip")],
                    vec![json!(2), json!("mcp-round-trip")],
                ],
                schema: None,
            }))
            .await
            .expect("bulk_insert should succeed");

        // Query back via MCP query
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: "SELECT id, label FROM mcp_pg_items \
                      WHERE label = 'mcp-round-trip' ORDER BY id"
                    .to_string(),
            }))
            .await
            .expect("query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 2, "both inserted rows must be queryable");
    }

    /// `bulk_delete` with a filter removes only matching rows.
    #[tokio::test]
    async fn bulk_delete_removes_matching_rows() {
        if !helpers::is_available(pg::AVAILABLE_KEY) {
            return;
        }
        let server = server();

        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "CREATE TABLE IF NOT EXISTS mcp_pg_delete_test \
                      (id INTEGER, label TEXT)"
                    .to_string(),
            }))
            .await
            .expect("CREATE TABLE should succeed");

        // Seed known rows
        server
            .execute(Parameters(ExecuteParams {
                profile: PROFILE.to_string(),
                sql: "DELETE FROM mcp_pg_delete_test WHERE label IN \
                      ('keep', 'remove')"
                    .to_string(),
            }))
            .await
            .ok(); // ignore if empty

        server
            .bulk_insert(Parameters(BulkInsertParams {
                profile: PROFILE.to_string(),
                table: "mcp_pg_delete_test".to_string(),
                columns: vec!["id".to_string(), "label".to_string()],
                rows: vec![
                    vec![json!(10), json!("keep")],
                    vec![json!(20), json!("remove")],
                    vec![json!(30), json!("remove")],
                ],
                schema: None,
            }))
            .await
            .expect("bulk_insert should succeed");

        // Delete only the 'remove' rows
        server
            .bulk_delete(Parameters(BulkDeleteParams {
                profile: PROFILE.to_string(),
                table: "mcp_pg_delete_test".to_string(),
                filter: json!({"label": {"eq": "remove"}}),
                schema: None,
            }))
            .await
            .expect("bulk_delete should succeed");

        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: "SELECT id FROM mcp_pg_delete_test \
                      WHERE label IN ('keep', 'remove') ORDER BY id"
                    .to_string(),
            }))
            .await
            .expect("query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 1, "only the 'keep' row must remain");
        assert_eq!(rows[0][0], json!(10), "remaining row must have id=10");
    }
}

// ── MySQL ─────────────────────────────────────────────────────────────────────

mod mysql {
    use super::helpers::{self, mysql};
    use super::*;

    const PROFILE: &str = "mysql-live-test";

    fn server() -> ArniMcpServer {
        helpers::make_server(helpers::mysql_config(), PROFILE)
    }

    /// Smoke test: query the pre-seeded users table.
    #[tokio::test]
    async fn query_returns_seeded_users() {
        if !helpers::is_available(mysql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: "SELECT id, name, email FROM users ORDER BY id".to_string(),
            }))
            .await
            .expect("query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 5, "init script seeds exactly 5 users");
    }

    /// Indexes created by the init script should be visible.
    #[tokio::test]
    async fn get_indexes_includes_init_script_indexes() {
        if !helpers::is_available(mysql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_indexes(Parameters(TableParams {
                profile: PROFILE.to_string(),
                table: "users".to_string(),
                schema: None,
            }))
            .await
            .expect("get_indexes should succeed");

        let json = helpers::content_to_json(content);
        let indexes = json.as_array().expect("get_indexes returns a JSON array");
        let names: Vec<&str> = indexes.iter().filter_map(|i| i["name"].as_str()).collect();
        assert!(
            names.contains(&"idx_users_email"),
            "idx_users_email must be listed; got: {names:?}"
        );
    }

    /// Server info should identify MySQL.
    #[tokio::test]
    async fn get_server_info_identifies_mysql() {
        if !helpers::is_available(mysql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_server_info(Parameters(ProfileParams {
                profile: PROFILE.to_string(),
            }))
            .await
            .expect("get_server_info should succeed");

        let json = helpers::content_to_json(content);
        let server_type = json["server_type"].as_str().unwrap_or("").to_lowercase();
        assert!(
            server_type.contains("mysql"),
            "server_type must mention mysql; got: {server_type}"
        );
    }

    /// `find_tables` with mode=contains should locate the users table.
    #[tokio::test]
    async fn find_tables_locates_users() {
        if !helpers::is_available(mysql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .find_tables(Parameters(FindTablesParams {
                profile: PROFILE.to_string(),
                pattern: "users".to_string(),
                mode: Some("contains".to_string()),
                schema: None,
            }))
            .await
            .expect("find_tables should succeed");

        let json = helpers::content_to_json(content);
        let tables = json["tables"].as_array().expect("tables must be array");
        assert!(
            tables.iter().any(|t| t.as_str() == Some("users")),
            "find_tables must find 'users'; got: {tables:?}"
        );
    }
}

// ── SQL Server ────────────────────────────────────────────────────────────────

mod mssql {
    use super::helpers::{self, mssql};
    use super::*;

    const PROFILE: &str = "mssql-live-test";

    fn server() -> ArniMcpServer {
        helpers::make_server(helpers::mssql_config(), PROFILE)
    }

    /// Smoke test: query the pre-seeded users table in test_db.
    #[tokio::test]
    async fn query_returns_seeded_users() {
        if !helpers::is_available(mssql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: "SELECT TOP 10 id, name, email FROM users ORDER BY id".to_string(),
            }))
            .await
            .expect("query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 5, "init script seeds exactly 5 users");
    }

    /// Indexes created by the init script should be visible.
    #[tokio::test]
    async fn get_indexes_includes_init_script_indexes() {
        if !helpers::is_available(mssql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_indexes(Parameters(TableParams {
                profile: PROFILE.to_string(),
                table: "users".to_string(),
                schema: Some("dbo".to_string()),
            }))
            .await
            .expect("get_indexes should succeed");

        let json = helpers::content_to_json(content);
        let indexes = json.as_array().expect("get_indexes returns a JSON array");
        let names: Vec<&str> = indexes.iter().filter_map(|i| i["name"].as_str()).collect();
        assert!(
            names.contains(&"idx_users_email"),
            "idx_users_email must be listed; got: {names:?}"
        );
    }

    /// Server info should identify SQL Server.
    #[tokio::test]
    async fn get_server_info_identifies_sql_server() {
        if !helpers::is_available(mssql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_server_info(Parameters(ProfileParams {
                profile: PROFILE.to_string(),
            }))
            .await
            .expect("get_server_info should succeed");

        let json = helpers::content_to_json(content);
        let server_type = json["server_type"].as_str().unwrap_or("").to_lowercase();
        assert!(
            server_type.contains("sql"),
            "server_type must mention sql; got: {server_type}"
        );
    }

    /// `list_stored_procedures` returns a JSON array (may be empty when no
    /// user-defined procedures exist), but must not error.
    #[tokio::test]
    async fn list_stored_procedures_succeeds() {
        if !helpers::is_available(mssql::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let result = server
            .list_stored_procedures(Parameters(SchemaParams {
                profile: PROFILE.to_string(),
                schema: Some("dbo".to_string()),
            }))
            .await;
        assert!(
            result.is_ok(),
            "list_stored_procedures must not return an error: {:?}",
            result.err()
        );
        let json = helpers::content_to_json(result.unwrap());
        // Response is a direct JSON array of ProcedureInfo objects
        assert!(
            json.is_array(),
            "list_stored_procedures must return a JSON array; got: {json:?}"
        );
    }
}

// ── MongoDB ───────────────────────────────────────────────────────────────────

mod mongodb {
    use super::helpers::{self, mongodb};
    use super::*;

    const PROFILE: &str = "mongo-live-test";

    fn server() -> ArniMcpServer {
        helpers::make_server(helpers::mongodb_config(), PROFILE)
    }

    /// MongoDB `query` uses a JSON command format rather than SQL.
    ///
    /// Format: `{"collection": "<name>", "filter": {…}}`
    #[tokio::test]
    async fn query_users_collection_returns_documents() {
        if !helpers::is_available(mongodb::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: r#"{"collection": "users", "filter": {}}"#.to_string(),
            }))
            .await
            .expect("MongoDB query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(rows.len(), 5, "init script seeds exactly 5 users");
        // MongoDB results have a 'name' field
        let cols = json["columns"].as_array().expect("columns must be array");
        assert!(
            cols.iter().any(|c| c.as_str() == Some("name")),
            "columns must include 'name'; got: {cols:?}"
        );
    }

    /// `tables` should list the users collection from the init script.
    #[tokio::test]
    async fn tables_includes_users_collection() {
        if !helpers::is_available(mongodb::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .tables(Parameters(ProfileParams {
                profile: PROFILE.to_string(),
            }))
            .await
            .expect("tables should succeed");

        let json = helpers::content_to_json(content);
        let tables = json["tables"].as_array().expect("tables must be array");
        assert!(
            tables.iter().any(|t| t.as_str() == Some("users")),
            "users collection must appear in tables list; got: {tables:?}"
        );
    }

    /// `get_server_info` must return a non-empty response for MongoDB.
    #[tokio::test]
    async fn get_server_info_returns_version() {
        if !helpers::is_available(mongodb::AVAILABLE_KEY) {
            return;
        }
        let server = server();
        let content = server
            .get_server_info(Parameters(ProfileParams {
                profile: PROFILE.to_string(),
            }))
            .await
            .expect("get_server_info should succeed");

        let json = helpers::content_to_json(content);
        // MongoDB returns a version string and server type
        assert!(
            !json["version"].as_str().unwrap_or("").is_empty(),
            "version must be non-empty"
        );
    }

    /// Query with a filter returns only matching documents.
    ///
    /// MongoDB JSON query format supports BSON filter documents.
    #[tokio::test]
    async fn query_with_filter_returns_subset() {
        if !helpers::is_available(mongodb::AVAILABLE_KEY) {
            return;
        }
        let server = server();

        // Filter for active=false — init script seeds exactly 1 inactive user
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: r#"{"collection": "users", "filter": {"active": false}}"#.to_string(),
            }))
            .await
            .expect("filtered query should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert_eq!(
            rows.len(),
            1,
            "exactly one inactive user was seeded; got: {rows:?}"
        );
    }

    /// `bulk_insert` into a MongoDB collection works via the MCP tool.
    #[tokio::test]
    async fn bulk_insert_into_collection() {
        if !helpers::is_available(mongodb::AVAILABLE_KEY) {
            return;
        }
        let server = server();

        // Insert two documents into a test collection
        let result = server
            .bulk_insert(Parameters(BulkInsertParams {
                profile: PROFILE.to_string(),
                table: "mcp_mongo_items".to_string(),
                columns: vec!["item_id".to_string(), "value".to_string()],
                rows: vec![
                    vec![json!(1), json!("alpha")],
                    vec![json!(2), json!("beta")],
                ],
                schema: None,
            }))
            .await;

        assert!(
            result.is_ok(),
            "bulk_insert into MongoDB collection must succeed: {:?}",
            result.err()
        );

        // Verify inserted docs are queryable
        let content = server
            .query(Parameters(QueryParams {
                profile: PROFILE.to_string(),
                sql: r#"{"collection": "mcp_mongo_items", "filter": {}}"#.to_string(),
            }))
            .await
            .expect("query after bulk_insert should succeed");

        let json = helpers::content_to_json(content);
        let rows = json["rows"].as_array().expect("rows must be array");
        assert!(
            rows.len() >= 2,
            "at least 2 inserted docs must be queryable"
        );
    }
}
