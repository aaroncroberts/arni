//! SQLite adapter integration tests.
//!
//! These tests use an in-memory database (`:memory:`) and require no external
//! services. They run unconditionally in CI when the `sqlite` feature is enabled.
//!
//! Run with:
//! ```bash
//! cargo test -p arni --features sqlite --test sqlite
//! ```

mod common;

#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use arni::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType};
    use arni::adapters::sqlite::SqliteAdapter;
    use arni::FilterExpr;
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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        let result = DbAdapter::read_table(&adapter, "anything", None).await;
        assert!(result.is_err(), "read_table before connect should fail");
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_list_databases() {
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_sqlite_database_type() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::SQLite);
    }

    // ── export_dataframe ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_export_dataframe_not_connected_returns_error() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let adapter = SqliteAdapter::new(cfg);

        let df = df! { "id" => [1i32, 2] }.unwrap();
        let result = DbAdapter::export_dataframe(&adapter, &df, "t", None, true).await;
        assert!(result.is_err(), "should fail before connect");
    }

    #[tokio::test]
    async fn test_sqlite_export_dataframe_creates_and_inserts() {
        use arni::adapter::{DbAdapter, QueryValue};
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
        use arni::adapter::{DbAdapter, QueryValue};
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
        use arni::adapter::DbAdapter;
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
        use arni::adapter::{DbAdapter, QueryValue};

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::{DbAdapter, QueryValue};

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
        use arni::adapter::{DbAdapter, QueryValue};
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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::{DbAdapter, QueryValue};

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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
        use arni::adapter::DbAdapter;

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

    // ── DataFrame round-trip tests ────────────────────────────────────────────
    //
    // These tests write a DataFrame via export_dataframe(), read it back via
    // read_table(), and assert that the schema and values are preserved.
    // SQLite stores booleans as 0/1 integers, so the column type changes;
    // we compare values after casting.

    #[tokio::test]
    async fn test_sqlite_round_trip_schema_matches() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let original = df! {
            "id"    => [1i64, 2, 3],
            "name"  => ["alice", "bob", "carol"],
            "score" => [9.5f64, 8.0, 7.25],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, "rt_schema", None, true)
            .await
            .expect("export should succeed");

        let read_back = DbAdapter::read_table(&adapter, "rt_schema", None)
            .await
            .expect("read_table should succeed");

        // Column names must match (order may differ for some adapters; sort both)
        let mut original_cols: Vec<&str> = original
            .get_column_names()
            .into_iter()
            .map(|s| s.as_str())
            .collect();
        let mut read_back_cols: Vec<&str> = read_back
            .get_column_names()
            .into_iter()
            .map(|s| s.as_str())
            .collect();
        original_cols.sort_unstable();
        read_back_cols.sort_unstable();
        assert_eq!(
            original_cols, read_back_cols,
            "column names should match after round-trip"
        );

        assert_eq!(
            read_back.height(),
            original.height(),
            "row count should match after round-trip"
        );
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_values_preserved() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let original = df! {
            "id"    => [10i64, 20, 30],
            "label" => ["x", "y", "z"],
            "val"   => [1.1f64, 2.2, 3.3],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, "rt_values", None, true)
            .await
            .unwrap();

        let read_back = DbAdapter::read_table(&adapter, "rt_values", None)
            .await
            .unwrap();

        // Verify integer column values
        let orig_ids = original
            .column("id")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap();
        let read_ids = read_back
            .column("id")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap();
        assert_eq!(orig_ids, read_ids, "id column values should round-trip");

        // Verify string column values
        let orig_labels = original.column("label").unwrap();
        let read_labels = read_back
            .column("label")
            .unwrap()
            .cast(&DataType::String)
            .unwrap();
        assert_eq!(
            orig_labels.cast(&DataType::String).unwrap(),
            read_labels,
            "label column values should round-trip"
        );
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_replace_true_no_duplicates() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! { "n" => [1i64, 2, 3] }.unwrap();

        // Write once
        DbAdapter::export_dataframe(&adapter, &df, "rt_replace", None, true)
            .await
            .unwrap();

        // Write again with replace=true — should replace, not append
        let rows = DbAdapter::export_dataframe(&adapter, &df, "rt_replace", None, true)
            .await
            .expect("second export with replace=true should succeed");
        assert_eq!(rows, 3, "replace=true should write exactly 3 rows");

        let read_back = DbAdapter::read_table(&adapter, "rt_replace", None)
            .await
            .unwrap();
        assert_eq!(
            read_back.height(),
            3,
            "table should have 3 rows after replace"
        );
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_replace_false_appends() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! { "n" => [1i64, 2] }.unwrap();

        // Create table first
        DbAdapter::export_dataframe(&adapter, &df, "rt_append", None, true)
            .await
            .unwrap();

        // Append with replace=false
        let rows = DbAdapter::export_dataframe(&adapter, &df, "rt_append", None, false)
            .await
            .expect("append should succeed");
        assert_eq!(rows, 2, "append should insert 2 more rows");

        let read_back = DbAdapter::read_table(&adapter, "rt_append", None)
            .await
            .unwrap();
        assert_eq!(
            read_back.height(),
            4,
            "table should have 4 rows after append"
        );
    }

    #[tokio::test]
    async fn test_sqlite_round_trip_empty_dataframe() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        // Create table schema first, then export empty DataFrame into it
        DbAdapter::execute_query(&adapter, "CREATE TABLE rt_empty (id INTEGER, name TEXT)")
            .await
            .unwrap();

        let empty_df = DataFrame::new(
            0,
            vec![
                Column::new("id".into(), &[] as &[i32]),
                Column::new("name".into(), &[] as &[&str]),
            ],
        )
        .unwrap();

        let rows = DbAdapter::export_dataframe(&adapter, &empty_df, "rt_empty", None, false)
            .await
            .expect("empty export should succeed");
        assert_eq!(rows, 0, "exporting empty DataFrame should insert 0 rows");

        let read_back = DbAdapter::read_table(&adapter, "rt_empty", None)
            .await
            .unwrap();
        assert_eq!(
            read_back.height(),
            0,
            "reading back empty table should yield 0 rows"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BULK OPERATIONS (in-memory, no external database required)
    // ═══════════════════════════════════════════════════════════════════════

    async fn connected_memory() -> SqliteAdapter {
        let cfg = memory_config();
        let mut adapter = SqliteAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("in-memory connect should succeed");
        adapter
    }

    #[tokio::test]
    async fn test_sqlite_bulk_insert_multi_row_returns_count() {
        use arni::adapter::{DbAdapter, QueryValue};

        let adapter = connected_memory().await;

        let _ = DbAdapter::execute_query(&adapter, "DROP TABLE IF EXISTS bk_ins").await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bk_ins (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, score INTEGER)",
        )
        .await
        .unwrap();

        let columns = vec!["name".to_string(), "score".to_string()];
        let rows = vec![
            vec![QueryValue::Text("Alice".to_string()), QueryValue::Int(90)],
            vec![QueryValue::Text("Bob".to_string()), QueryValue::Int(85)],
            vec![QueryValue::Text("Carol".to_string()), QueryValue::Int(92)],
        ];

        let n = DbAdapter::bulk_insert(&adapter, "bk_ins", &columns, &rows, None)
            .await
            .expect("bulk_insert should succeed");
        assert_eq!(n, 3);

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) FROM bk_ins")
            .await
            .unwrap();
        assert!(matches!(result.rows[0][0], QueryValue::Int(3)));
    }

    #[tokio::test]
    async fn test_sqlite_bulk_insert_empty_rows_returns_zero() {
        use arni::adapter::DbAdapter;

        let adapter = connected_memory().await;
        let _ = DbAdapter::execute_query(&adapter, "DROP TABLE IF EXISTS bk_empty").await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bk_empty (id INTEGER PRIMARY KEY, val INTEGER)",
        )
        .await
        .unwrap();

        let n = DbAdapter::bulk_insert(&adapter, "bk_empty", &["val".to_string()], &[], None)
            .await
            .expect("empty bulk_insert should succeed");
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_sqlite_bulk_insert_null_value_round_trips() {
        use arni::adapter::{DbAdapter, QueryValue};

        let adapter = connected_memory().await;
        let _ = DbAdapter::execute_query(&adapter, "DROP TABLE IF EXISTS bk_null").await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bk_null (id INTEGER PRIMARY KEY, note TEXT)",
        )
        .await
        .unwrap();

        DbAdapter::bulk_insert(
            &adapter,
            "bk_null",
            &["note".to_string()],
            &[vec![QueryValue::Null]],
            None,
        )
        .await
        .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT note FROM bk_null")
            .await
            .unwrap();
        assert!(matches!(result.rows[0][0], QueryValue::Null));
    }

    #[tokio::test]
    async fn test_sqlite_bulk_update_matching_rows_only() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};
        use std::collections::HashMap;

        let adapter = connected_memory().await;
        let _ = DbAdapter::execute_query(&adapter, "DROP TABLE IF EXISTS bk_upd").await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bk_upd (id INTEGER PRIMARY KEY, status TEXT)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO bk_upd VALUES (1,'pending'), (2,'pending'), (3,'done')",
        )
        .await
        .unwrap();

        let mut set_clauses = HashMap::new();
        set_clauses.insert("status".to_string(), QueryValue::Text("active".to_string()));
        let filter = FilterExpr::Eq("id".to_string(), QueryValue::Int(1));

        let n = DbAdapter::bulk_update(&adapter, "bk_upd", &[(set_clauses, filter)], None)
            .await
            .expect("bulk_update should succeed");
        assert_eq!(n, 1);

        let result = DbAdapter::execute_query(&adapter, "SELECT status FROM bk_upd WHERE id = 2")
            .await
            .unwrap();
        assert!(
            matches!(&result.rows[0][0], QueryValue::Text(s) if s == "pending"),
            "id=2 should remain pending"
        );
    }

    #[tokio::test]
    async fn test_sqlite_bulk_delete_matching_rows_only() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};

        let adapter = connected_memory().await;
        let _ = DbAdapter::execute_query(&adapter, "DROP TABLE IF EXISTS bk_del").await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bk_del (id INTEGER PRIMARY KEY, tag TEXT)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO bk_del VALUES (1,'a'), (2,'b'), (3,'a')",
        )
        .await
        .unwrap();

        let filter = FilterExpr::Eq("tag".to_string(), QueryValue::Text("a".to_string()));
        let n = DbAdapter::bulk_delete(&adapter, "bk_del", &[filter], None)
            .await
            .expect("bulk_delete should succeed");
        assert_eq!(n, 2);

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) FROM bk_del")
            .await
            .unwrap();
        assert!(matches!(result.rows[0][0], QueryValue::Int(1)));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // FILTEREXPR INTEGRATION TESTS
    //
    // Verify complex FilterExpr trees (And, Or, Not, In, IsNull, IsNotNull)
    // work correctly end-to-end through bulk_update and bulk_delete.
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_sqlite_bulk_update_with_and_filter_matches_only_correct_rows() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};
        use std::collections::HashMap;

        let adapter = connected_memory().await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE flt_and (id INTEGER PRIMARY KEY, score INTEGER, active INTEGER)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO flt_and VALUES (1, 90, 1), (2, 70, 1), (3, 90, 0), (4, 80, 1)",
        )
        .await
        .unwrap();

        // Update where score >= 90 AND active = 1 → should match only id=1
        let filter = FilterExpr::And(vec![
            FilterExpr::Gte("score".to_string(), QueryValue::Int(90)),
            FilterExpr::Eq("active".to_string(), QueryValue::Int(1)),
        ]);
        let mut set_clauses = HashMap::new();
        set_clauses.insert("score".to_string(), QueryValue::Int(100));

        let n = DbAdapter::bulk_update(&adapter, "flt_and", &[(set_clauses, filter)], None)
            .await
            .expect("bulk_update with And filter should succeed");

        assert_eq!(n, 1, "And filter should match exactly 1 row (id=1)");

        let result =
            DbAdapter::execute_query(&adapter, "SELECT id, score FROM flt_and ORDER BY id")
                .await
                .unwrap();
        assert!(
            matches!(result.rows[0][1], QueryValue::Int(100)),
            "id=1 score should be 100"
        );
        assert!(
            matches!(result.rows[1][1], QueryValue::Int(70)),
            "id=2 score should be unchanged"
        );
        assert!(
            matches!(result.rows[2][1], QueryValue::Int(90)),
            "id=3 score should be unchanged"
        );
        assert!(
            matches!(result.rows[3][1], QueryValue::Int(80)),
            "id=4 score should be unchanged"
        );
    }

    #[tokio::test]
    async fn test_sqlite_bulk_delete_with_or_filter_removes_correct_rows() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};

        let adapter = connected_memory().await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE flt_or (id INTEGER PRIMARY KEY, tag TEXT)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO flt_or VALUES (1,'a'), (2,'b'), (3,'c'), (4,'a')",
        )
        .await
        .unwrap();

        // Delete where tag = 'a' OR tag = 'c' → rows 1, 3, 4
        let filter = FilterExpr::Or(vec![
            FilterExpr::Eq("tag".to_string(), QueryValue::Text("a".to_string())),
            FilterExpr::Eq("tag".to_string(), QueryValue::Text("c".to_string())),
        ]);

        let n = DbAdapter::bulk_delete(&adapter, "flt_or", &[filter], None)
            .await
            .expect("bulk_delete with Or filter should succeed");

        assert_eq!(n, 3, "Or filter should delete 3 rows (id=1,3,4)");

        let result = DbAdapter::execute_query(&adapter, "SELECT id FROM flt_or")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1, "1 row should remain");
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(2)),
            "id=2 should remain"
        );
    }

    #[tokio::test]
    async fn test_sqlite_bulk_delete_in_empty_list_deletes_zero_rows() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};

        let adapter = connected_memory().await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE flt_in_empty (id INTEGER PRIMARY KEY)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(&adapter, "INSERT INTO flt_in_empty VALUES (1), (2), (3)")
            .await
            .unwrap();

        // In(...) with empty list — should not produce SQL "IN ()" syntax error
        let filter = FilterExpr::In("id".to_string(), vec![]);

        let n = DbAdapter::bulk_delete(&adapter, "flt_in_empty", &[filter], None)
            .await
            .expect("In() with empty list should not error");

        assert_eq!(n, 0, "empty In() list should match 0 rows");

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) FROM flt_in_empty")
            .await
            .unwrap();
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(3)),
            "all 3 rows should remain"
        );
    }

    #[tokio::test]
    async fn test_sqlite_bulk_update_is_null_and_is_not_null_filters() {
        use arni::adapter::{DbAdapter, FilterExpr, QueryValue};
        use std::collections::HashMap;

        let adapter = connected_memory().await;
        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE flt_null (id INTEGER PRIMARY KEY, note TEXT)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO flt_null VALUES (1, NULL), (2, 'hi'), (3, NULL)",
        )
        .await
        .unwrap();

        // Update rows where note IS NULL (id=1 and id=3)
        let null_filter = FilterExpr::IsNull("note".to_string());
        let mut set_clauses = HashMap::new();
        set_clauses.insert("note".to_string(), QueryValue::Text("filled".to_string()));

        let n = DbAdapter::bulk_update(&adapter, "flt_null", &[(set_clauses, null_filter)], None)
            .await
            .expect("bulk_update with IsNull filter should succeed");

        assert_eq!(n, 2, "IsNull should match 2 rows (id=1 and id=3)");

        // Delete rows where note IS NOT NULL (now all 3 rows have a non-null note)
        let not_null_filter = FilterExpr::IsNotNull("note".to_string());
        let del_n = DbAdapter::bulk_delete(&adapter, "flt_null", &[not_null_filter], None)
            .await
            .expect("bulk_delete with IsNotNull filter should succeed");

        assert_eq!(del_n, 3, "IsNotNull should now match all 3 rows");
    }

    // ── adapter-specific feature tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_sqlite_get_server_info_version_non_empty() {
        use arni::adapter::DbAdapter;

        let adapter = connected_memory().await;
        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        assert!(
            !info.version.is_empty(),
            "server version field must not be empty"
        );
    }
}
