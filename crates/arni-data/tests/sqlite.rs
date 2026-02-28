//! SQLite adapter integration tests.
//!
//! These tests use an in-memory database (`:memory:`) and require no external
//! services. They run unconditionally in CI when the `sqlite` feature is enabled.
//!
//! Run with:
//! ```bash
//! cargo test -p arni-data --features sqlite --test sqlite
//! ```

mod common;

#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use arni_data::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType};
    use arni_data::adapters::sqlite::SqliteAdapter;
    use arni_data::FilterExpr;
    use std::collections::HashMap;

    fn memory_config() -> ConnectionConfig {
        ConnectionConfig {
            id: "test-sqlite".to_string(),
            name: "test-sqlite".to_string(),
            db_type: DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        }
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_connect_memory() {
        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("connect to :memory: should succeed");
    }

    #[tokio::test]
    async fn test_sqlite_disconnect() {
        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        ConnectionTrait::disconnect(&mut adapter)
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn test_sqlite_health_check_before_connect_returns_false() {
        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        let healthy = ConnectionTrait::health_check(&adapter).await.unwrap();
        assert!(!healthy, "health_check before connect should return false");
    }

    #[tokio::test]
    async fn test_sqlite_health_check_after_connect_returns_true() {
        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        let healthy = ConnectionTrait::health_check(&adapter).await.unwrap();
        assert!(healthy, "health_check after connect should return true");
    }

    // ── Query execution ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_execute_query_before_connect_returns_error() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        let result = DbAdapter::execute_query(&adapter, "SELECT 1").await;
        assert!(
            result.is_err(),
            "execute_query before connect should return error"
        );
    }

    #[tokio::test]
    async fn test_sqlite_execute_select_1() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT 1 as value")
            .await
            .expect("SELECT 1 should succeed");
        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_sqlite_create_table_and_insert() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob')",
        )
        .await
        .expect("INSERT should succeed");

        let result =
            DbAdapter::execute_query(&adapter, "SELECT id, name FROM test_users ORDER BY id")
                .await
                .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    // ── Schema introspection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_list_tables_empty_db() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables on empty DB should succeed");
        assert!(
            tables.is_empty(),
            "fresh :memory: DB should have no user tables"
        );
    }

    #[tokio::test]
    async fn test_sqlite_list_tables_after_create() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE foo (id INTEGER)")
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, "CREATE TABLE bar (name TEXT)")
            .await
            .unwrap();

        let mut tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        tables.sort();
        assert_eq!(tables, vec!["bar", "foo"]);
    }

    #[tokio::test]
    async fn test_sqlite_describe_table() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE sample (id INTEGER PRIMARY KEY, label TEXT)",
        )
        .await
        .unwrap();

        let info = DbAdapter::describe_table(&adapter, "sample", None)
            .await
            .expect("describe_table should succeed");

        assert_eq!(info.name, "sample");
        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"), "should include 'id' column");
        assert!(
            col_names.contains(&"label"),
            "should include 'label' column"
        );
        // Empty table — row_count should be Some(0)
        assert_eq!(
            info.row_count,
            Some(0),
            "empty table should report row_count = 0"
        );
        assert!(
            info.size_bytes.is_none(),
            "in-memory SQLite has no disk size"
        );
        assert!(
            info.created_at.is_none(),
            "SQLite does not track creation time"
        );
    }

    // ── DataFrame queries ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_read_table_returns_dataframe() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE rt_tbl (id INTEGER, label TEXT)")
            .await
            .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO rt_tbl VALUES (1, 'alpha'), (2, 'beta')",
        )
        .await
        .unwrap();

        let df = DbAdapter::read_table(&adapter, "rt_tbl", None)
            .await
            .expect("read_table should return a DataFrame");
        assert_eq!(df.height(), 2, "should have 2 rows");
        assert!(df.column("id").is_ok(), "id column should exist");
        assert!(df.column("label").is_ok(), "label column should exist");
    }

    #[tokio::test]
    async fn test_sqlite_query_df_returns_dataframe() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = DbAdapter::query_df(&adapter, "SELECT 7 AS n, 'hi' AS msg")
            .await
            .expect("query_df should return a DataFrame");
        assert_eq!(df.height(), 1);
        assert!(df.column("n").is_ok());
        assert!(df.column("msg").is_ok());
    }

    #[tokio::test]
    async fn test_sqlite_read_table_not_connected_returns_error() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        let result = DbAdapter::read_table(&adapter, "anything", None).await;
        assert!(result.is_err(), "read_table before connect should fail");
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_list_databases() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let dbs = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");
        // SQLite returns the path/database name as the single entry
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0], ":memory:");
    }

    #[tokio::test]
    async fn test_sqlite_invalid_sql_returns_error() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_sqlite_database_type() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::SQLite);
    }

    // ── export_dataframe ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_export_dataframe_not_connected_returns_error() {
        use arni_data::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);

        let df = df! { "id" => [1i32, 2] }.unwrap();
        let result = DbAdapter::export_dataframe(&adapter, &df, "t", None, true).await;
        assert!(result.is_err(), "should fail before connect");
    }

    #[tokio::test]
    async fn test_sqlite_export_dataframe_creates_and_inserts() {
        use arni_data::adapter::{DbAdapter, QueryValue};
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! {
            "id" => [1i32, 2, 3],
            "label" => ["alpha", "beta", "gamma"],
        }
        .unwrap();

        let rows = DbAdapter::export_dataframe(&adapter, &df, "exp_basic", None, true)
            .await
            .expect("export_dataframe should succeed");
        assert_eq!(rows, 3);

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM exp_basic")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(3));
    }

    #[tokio::test]
    async fn test_sqlite_export_dataframe_replace() {
        use arni_data::adapter::{DbAdapter, QueryValue};
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df1 = df! { "x" => [1i64, 2, 3, 4, 5] }.unwrap();
        DbAdapter::export_dataframe(&adapter, &df1, "repl", None, true)
            .await
            .unwrap();

        let df2 = df! { "x" => [10i64, 20] }.unwrap();
        let rows = DbAdapter::export_dataframe(&adapter, &df2, "repl", None, true)
            .await
            .expect("replace should succeed");
        assert_eq!(rows, 2);

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM repl")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(2));
    }

    #[tokio::test]
    async fn test_sqlite_export_dataframe_empty_returns_zero() {
        use arni_data::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE emp (id INTEGER)")
            .await
            .unwrap();

        let df = DataFrame::new(0, vec![Column::new("id".into(), &[] as &[i32])]).unwrap();
        let rows = DbAdapter::export_dataframe(&adapter, &df, "emp", None, false)
            .await
            .expect("empty export should succeed");
        assert_eq!(rows, 0);
    }

    // ── bulk_insert ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_bulk_insert_basic() {
        use arni_data::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi (id INTEGER, name TEXT)")
            .await
            .unwrap();

        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![QueryValue::Int(1), QueryValue::Text("alice".to_string())],
            vec![QueryValue::Int(2), QueryValue::Text("bob".to_string())],
        ];

        let inserted = DbAdapter::bulk_insert(&adapter, "bi", &cols, &rows, None)
            .await
            .expect("bulk_insert should succeed");
        assert_eq!(inserted, 2);
    }

    #[tokio::test]
    async fn test_sqlite_bulk_insert_empty_returns_zero() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi_empty (id INTEGER)")
            .await
            .unwrap();
        let cols = vec!["id".to_string()];
        let n = DbAdapter::bulk_insert(&adapter, "bi_empty", &cols, &[], None)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_sqlite_bulk_insert_mismatch_returns_error() {
        use arni_data::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let cols = vec!["a".to_string(), "b".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]]; // 1 value, 2 cols
        let result = DbAdapter::bulk_insert(&adapter, "any", &cols, &rows, None).await;
        assert!(result.is_err());
    }

    // ── bulk_update ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_bulk_update_basic() {
        use arni_data::adapter::{DbAdapter, QueryValue};
        use std::collections::HashMap;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bu (id INTEGER, score INTEGER)")
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, "INSERT INTO bu VALUES (1, 10), (2, 20)")
            .await
            .unwrap();

        let mut set_vals = HashMap::new();
        set_vals.insert("score".to_string(), QueryValue::Int(99));
        let updates = vec![(
            set_vals,
            FilterExpr::Eq("id".to_string(), QueryValue::Int(1)),
        )];

        let affected = DbAdapter::bulk_update(&adapter, "bu", &updates, None)
            .await
            .expect("bulk_update should succeed");
        assert_eq!(affected, 1);

        let r = DbAdapter::execute_query(&adapter, "SELECT score FROM bu WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], QueryValue::Int(99));
    }

    #[tokio::test]
    async fn test_sqlite_bulk_update_empty_returns_zero() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let n = DbAdapter::bulk_update(&adapter, "t", &[], None)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    // ── bulk_delete ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_bulk_delete_basic() {
        use arni_data::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bd (id INTEGER)")
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, "INSERT INTO bd VALUES (1), (2), (3)")
            .await
            .unwrap();

        let deleted = DbAdapter::bulk_delete(
            &adapter,
            "bd",
            &[FilterExpr::Eq("id".to_string(), QueryValue::Int(2))],
            None,
        )
        .await
        .expect("bulk_delete should succeed");
        assert_eq!(deleted, 1);

        let r = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM bd")
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], QueryValue::Int(2));
    }

    #[tokio::test]
    async fn test_sqlite_bulk_delete_empty_returns_zero() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let n = DbAdapter::bulk_delete(&adapter, "t", &[], None)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    // ── Metadata: get_view_definition ────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_get_view_definition() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE arni_vdef_base (id INTEGER, val TEXT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "CREATE VIEW arni_vdef_view AS SELECT id, val FROM arni_vdef_base",
        )
        .await
        .expect("CREATE VIEW should succeed");

        let def = DbAdapter::get_view_definition(&adapter, "arni_vdef_view", None)
            .await
            .expect("get_view_definition should succeed");

        let def_str = def.expect("view definition should be Some");
        assert!(
            def_str.to_lowercase().contains("select"),
            "definition should contain SELECT; got: {}",
            def_str
        );
    }

    #[tokio::test]
    async fn test_sqlite_get_view_definition_nonexistent() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::get_view_definition(&adapter, "arni_no_such_view_xyzzy", None)
            .await
            .expect("get_view_definition for nonexistent view should return Ok");

        assert!(
            result.is_none(),
            "nonexistent view should return None; got: {:?}",
            result
        );
    }
}
