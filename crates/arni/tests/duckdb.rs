//! DuckDB adapter integration tests.
//!
//! These tests use an in-memory database (`:memory:`) and require no external
//! services. They run unconditionally in CI when the `duckdb` feature is enabled.
//!
//! Run with:
//! ```bash
//! cargo test -p arni-data --features duckdb --test duckdb
//! ```

mod common;

#[cfg(feature = "duckdb")]
mod duckdb_tests {
    use arni::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType};
    use arni::adapters::duckdb::DuckDbAdapter;
    use arni::FilterExpr;
    use std::collections::HashMap;

    fn memory_config() -> ConnectionConfig {
        ConnectionConfig {
            id: "test-duckdb".to_string(),
            name: "test-duckdb".to_string(),
            db_type: DatabaseType::DuckDB,
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
    async fn test_duckdb_connect_memory() {
        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("connect to :memory: should succeed");
    }

    #[tokio::test]
    async fn test_duckdb_is_connected_after_connect() {
        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "should not be connected before connect()"
        );
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        assert!(
            ConnectionTrait::is_connected(&adapter),
            "should be connected after connect()"
        );
    }

    #[tokio::test]
    async fn test_duckdb_is_connected_false_after_disconnect() {
        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        ConnectionTrait::disconnect(&mut adapter).await.unwrap();
        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "should not be connected after disconnect()"
        );
    }

    #[tokio::test]
    async fn test_duckdb_health_check_before_connect_returns_false() {
        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);
        let healthy = ConnectionTrait::health_check(&adapter).await.unwrap();
        assert!(!healthy, "health_check before connect should return false");
    }

    #[tokio::test]
    async fn test_duckdb_health_check_after_connect_returns_true() {
        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();
        let healthy = ConnectionTrait::health_check(&adapter).await.unwrap();
        assert!(healthy, "health_check after connect should return true");
    }

    // ── Query execution ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_execute_query_before_connect_returns_error() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);
        let result = DbAdapter::execute_query(&adapter, "SELECT 1").await;
        assert!(
            result.is_err(),
            "execute_query before connect should return error"
        );
    }

    #[tokio::test]
    async fn test_duckdb_execute_select_1() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS value")
            .await
            .expect("SELECT 1 should succeed");
        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_duckdb_create_table_and_insert() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE test_items (id INTEGER, label VARCHAR)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO test_items VALUES (1, 'alpha'), (2, 'beta')",
        )
        .await
        .expect("INSERT should succeed");

        let result =
            DbAdapter::execute_query(&adapter, "SELECT id, label FROM test_items ORDER BY id")
                .await
                .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 2);
        assert!(result.columns.contains(&"id".to_string()));
        assert!(result.columns.contains(&"label".to_string()));
    }

    // ── Schema introspection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_list_tables_empty_db() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
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
    async fn test_duckdb_list_tables_after_create() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE alpha (id INTEGER)")
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, "CREATE TABLE beta (name VARCHAR)")
            .await
            .unwrap();

        let mut tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        tables.sort();
        assert_eq!(tables, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_duckdb_describe_table() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE sample (id INTEGER, name VARCHAR, score DOUBLE)",
        )
        .await
        .unwrap();

        let info = DbAdapter::describe_table(&adapter, "sample", None)
            .await
            .expect("describe_table should succeed");

        assert_eq!(info.name, "sample");
        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"), "should include 'id'");
        assert!(col_names.contains(&"name"), "should include 'name'");
        assert!(col_names.contains(&"score"), "should include 'score'");
        // Empty table — row_count should be Some(0)
        assert_eq!(
            info.row_count,
            Some(0),
            "empty table should report row_count = 0"
        );
        assert!(
            info.size_bytes.is_none(),
            "in-memory DuckDB has no disk size"
        );
        assert!(
            info.created_at.is_none(),
            "DuckDB does not track creation time"
        );
    }

    // ── DataFrame queries ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_read_table_returns_dataframe() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE rt_tbl (id INTEGER, label VARCHAR)")
            .await
            .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO rt_tbl VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
        )
        .await
        .unwrap();

        let df = DbAdapter::read_table(&adapter, "rt_tbl", None)
            .await
            .expect("read_table should return a DataFrame");
        assert_eq!(df.height(), 3, "should have 3 rows");
        assert!(df.column("id").is_ok(), "id column should exist");
        assert!(df.column("label").is_ok(), "label column should exist");
    }

    #[tokio::test]
    async fn test_duckdb_query_df_returns_dataframe() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = DbAdapter::query_df(&adapter, "SELECT 42 AS answer, 'world' AS greet")
            .await
            .expect("query_df should return a DataFrame");
        assert_eq!(df.height(), 1);
        assert!(df.column("answer").is_ok());
        assert!(df.column("greet").is_ok());
    }

    #[tokio::test]
    async fn test_duckdb_read_table_not_connected_returns_error() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);
        let result = DbAdapter::read_table(&adapter, "anything", None).await;
        assert!(result.is_err(), "read_table before connect should fail");
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_invalid_sql_returns_error() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_duckdb_database_type() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::DuckDB);
    }

    #[tokio::test]
    async fn test_duckdb_arithmetic_query() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT 6 * 7 AS answer")
            .await
            .expect("arithmetic query should succeed");

        assert_eq!(result.rows.len(), 1);
        let answer = &result.rows[0][0];
        // DuckDB returns integers as QueryValue::Int
        match answer {
            QueryValue::Int(v) => assert_eq!(*v, 42),
            other => panic!("expected QueryValue::Int(42), got {:?}", other),
        }
    }

    // ── export_dataframe ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_export_dataframe_not_connected_returns_error() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);

        let df = df! {
            "id" => [1i32, 2, 3],
            "name" => ["a", "b", "c"],
        }
        .unwrap();

        let result = DbAdapter::export_dataframe(&adapter, &df, "test_table", None, true).await;
        assert!(
            result.is_err(),
            "export_dataframe before connect should fail"
        );
    }

    #[tokio::test]
    async fn test_duckdb_export_dataframe_creates_table_and_inserts() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! {
            "id" => [1i32, 2, 3],
            "label" => ["alpha", "beta", "gamma"],
        }
        .unwrap();

        let rows = DbAdapter::export_dataframe(&adapter, &df, "export_basic", None, true)
            .await
            .expect("export_dataframe should succeed");

        assert_eq!(rows, 3, "should have inserted 3 rows");

        // Verify data is queryable
        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM export_basic")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], arni::adapter::QueryValue::Int(3));
    }

    #[tokio::test]
    async fn test_duckdb_export_dataframe_replace() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df1 = df! { "x" => [1i64, 2, 3, 4, 5] }.unwrap();
        DbAdapter::export_dataframe(&adapter, &df1, "replace_test", None, true)
            .await
            .unwrap();

        // Replace with smaller dataset
        let df2 = df! { "x" => [10i64, 20] }.unwrap();
        let rows = DbAdapter::export_dataframe(&adapter, &df2, "replace_test", None, true)
            .await
            .expect("replace should succeed");

        assert_eq!(rows, 2, "replaced table should have 2 rows");

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM replace_test")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], arni::adapter::QueryValue::Int(2));
    }

    #[tokio::test]
    async fn test_duckdb_export_dataframe_empty_df_returns_zero() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        // Create empty DataFrame with schema
        let df = DataFrame::new(0, vec![Column::new("id".into(), &[] as &[i32])]).unwrap();

        // Create table first so export has somewhere to write
        DbAdapter::execute_query(&adapter, "CREATE TABLE empty_test (id INTEGER)")
            .await
            .unwrap();

        let rows = DbAdapter::export_dataframe(&adapter, &df, "empty_test", None, false)
            .await
            .expect("exporting empty df should succeed");

        assert_eq!(rows, 0, "empty DataFrame should insert 0 rows");
    }

    #[tokio::test]
    async fn test_duckdb_export_dataframe_with_nulls() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! {
            "id" => [Some(1i32), None, Some(3)],
            "val" => [Some(1.5f64), Some(2.5), None],
        }
        .unwrap();

        let rows = DbAdapter::export_dataframe(&adapter, &df, "null_test", None, true)
            .await
            .expect("export with nulls should succeed");

        assert_eq!(rows, 3);
    }

    // ── bulk_insert ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_bulk_insert_not_connected_returns_error() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);

        let cols = vec!["id".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]];
        let result = DbAdapter::bulk_insert(&adapter, "t", &cols, &rows, None).await;
        assert!(
            result.is_err(),
            "bulk_insert before connect should return error"
        );
    }

    #[tokio::test]
    async fn test_duckdb_bulk_insert_empty_rows_returns_zero() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi_empty (id INTEGER)")
            .await
            .unwrap();

        let cols = vec!["id".to_string()];
        let result = DbAdapter::bulk_insert(&adapter, "bi_empty", &cols, &[], None)
            .await
            .expect("empty bulk_insert should succeed");
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_duckdb_bulk_insert_empty_columns_returns_error() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let rows = vec![vec![QueryValue::Int(1)]];
        let result = DbAdapter::bulk_insert(&adapter, "any_table", &[], &rows, None).await;
        assert!(
            result.is_err(),
            "bulk_insert with no columns should return error"
        );
    }

    #[tokio::test]
    async fn test_duckdb_bulk_insert_column_count_mismatch_returns_error() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi_mismatch (a INTEGER, b INTEGER)")
            .await
            .unwrap();

        let cols = vec!["a".to_string(), "b".to_string()]; // 2 columns
        let rows = vec![
            vec![QueryValue::Int(1)], // only 1 value
        ];
        let result = DbAdapter::bulk_insert(&adapter, "bi_mismatch", &cols, &rows, None).await;
        assert!(
            result.is_err(),
            "mismatched column count should return error"
        );
    }

    #[tokio::test]
    async fn test_duckdb_bulk_insert_basic() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi_basic (id INTEGER, name VARCHAR)")
            .await
            .unwrap();

        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![QueryValue::Int(1), QueryValue::Text("alice".to_string())],
            vec![QueryValue::Int(2), QueryValue::Text("bob".to_string())],
            vec![QueryValue::Int(3), QueryValue::Text("charlie".to_string())],
        ];

        let inserted = DbAdapter::bulk_insert(&adapter, "bi_basic", &cols, &rows, None)
            .await
            .expect("bulk_insert should succeed");

        assert_eq!(inserted, 3, "should have inserted 3 rows");

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM bi_basic")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(3));
    }

    #[tokio::test]
    async fn test_duckdb_bulk_insert_with_nulls() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bi_nulls (id INTEGER, note VARCHAR)")
            .await
            .unwrap();

        let cols = vec!["id".to_string(), "note".to_string()];
        let rows = vec![
            vec![QueryValue::Int(1), QueryValue::Null],
            vec![QueryValue::Null, QueryValue::Text("no id".to_string())],
        ];

        let inserted = DbAdapter::bulk_insert(&adapter, "bi_nulls", &cols, &rows, None)
            .await
            .expect("bulk_insert with nulls should succeed");

        assert_eq!(inserted, 2);
    }

    // ── bulk_update ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_bulk_update_empty_returns_zero() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::bulk_update(&adapter, "any_table", &[], None)
            .await
            .expect("bulk_update with empty list should return 0");
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_duckdb_bulk_update_basic() {
        use arni::adapter::{DbAdapter, QueryValue};
        use std::collections::HashMap;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bu_basic (id INTEGER, score INTEGER)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO bu_basic VALUES (1, 10), (2, 20), (3, 30)",
        )
        .await
        .unwrap();

        let mut set_vals = HashMap::new();
        set_vals.insert("score".to_string(), QueryValue::Int(99));

        let updates = vec![(
            set_vals,
            FilterExpr::Eq("id".to_string(), QueryValue::Int(2)),
        )];
        let affected = DbAdapter::bulk_update(&adapter, "bu_basic", &updates, None)
            .await
            .expect("bulk_update should succeed");

        assert_eq!(affected, 1, "one row should have been updated");

        let result = DbAdapter::execute_query(&adapter, "SELECT score FROM bu_basic WHERE id = 2")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(99));
    }

    // ── bulk_delete ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_bulk_delete_empty_returns_zero() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::bulk_delete(&adapter, "any_table", &[], None)
            .await
            .expect("bulk_delete with empty list should return 0");
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_duckdb_bulk_delete_basic() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE bd_basic (id INTEGER, active BOOLEAN)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO bd_basic VALUES (1, true), (2, false), (3, true), (4, false)",
        )
        .await
        .unwrap();

        let where_clauses = vec![FilterExpr::Eq(
            "active".to_string(),
            QueryValue::Bool(false),
        )];
        let deleted = DbAdapter::bulk_delete(&adapter, "bd_basic", &where_clauses, None)
            .await
            .expect("bulk_delete should succeed");

        assert_eq!(deleted, 2, "two inactive rows should have been deleted");

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM bd_basic")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(2));
    }

    #[tokio::test]
    async fn test_duckdb_bulk_delete_multiple_clauses() {
        use arni::adapter::{DbAdapter, QueryValue};

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(&adapter, "CREATE TABLE bd_multi (id INTEGER)")
            .await
            .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO bd_multi VALUES (1), (2), (3), (4), (5)",
        )
        .await
        .unwrap();

        // Delete rows 1 and 5 separately
        let where_clauses = vec![
            FilterExpr::Eq("id".to_string(), QueryValue::Int(1)),
            FilterExpr::Eq("id".to_string(), QueryValue::Int(5)),
        ];
        let deleted = DbAdapter::bulk_delete(&adapter, "bd_multi", &where_clauses, None)
            .await
            .expect("bulk_delete should succeed");

        assert_eq!(deleted, 2);

        let result = DbAdapter::execute_query(&adapter, "SELECT COUNT(*) AS n FROM bd_multi")
            .await
            .unwrap();
        assert_eq!(result.rows[0][0], QueryValue::Int(3));
    }

    // ── Metadata: get_view_definition ────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_get_view_definition() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE duckdb_vdef_base (id BIGINT, val TEXT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "CREATE VIEW duckdb_vdef_view AS SELECT id, val FROM duckdb_vdef_base",
        )
        .await
        .expect("CREATE VIEW should succeed");

        let def = DbAdapter::get_view_definition(&adapter, "duckdb_vdef_view", Some("main"))
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
    async fn test_duckdb_get_view_definition_nonexistent() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result =
            DbAdapter::get_view_definition(&adapter, "duckdb_no_such_view_xyzzy", Some("main"))
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
    // DuckDB is an in-process engine, so round-trip tests run without any
    // external services. DuckDB preserves types faithfully (no bool→int coercion),
    // making it ideal for verifying column schema preservation.

    #[tokio::test]
    async fn test_duckdb_round_trip_schema_matches() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
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
    async fn test_duckdb_round_trip_values_preserved() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
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

        let orig_labels = original
            .column("label")
            .unwrap()
            .cast(&DataType::String)
            .unwrap();
        let read_labels = read_back
            .column("label")
            .unwrap()
            .cast(&DataType::String)
            .unwrap();
        assert_eq!(
            orig_labels, read_labels,
            "label column values should round-trip"
        );
    }

    #[tokio::test]
    async fn test_duckdb_round_trip_replace_true_no_duplicates() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! { "n" => [1i64, 2, 3] }.unwrap();

        DbAdapter::export_dataframe(&adapter, &df, "rt_replace", None, true)
            .await
            .unwrap();

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
    async fn test_duckdb_round_trip_replace_false_appends() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let df = df! { "n" => [1i64, 2] }.unwrap();

        DbAdapter::export_dataframe(&adapter, &df, "rt_append", None, true)
            .await
            .unwrap();

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
    async fn test_duckdb_round_trip_empty_dataframe() {
        use arni::adapter::DbAdapter;
        use polars::prelude::*;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        // DuckDB requires the table to exist before we can export 0 rows into it.
        DbAdapter::execute_query(&adapter, "CREATE TABLE rt_empty (id BIGINT, name VARCHAR)")
            .await
            .unwrap();

        let empty_df = DataFrame::new(
            0,
            vec![
                Column::new("id".into(), &[] as &[i64]),
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

    // ── adapter-specific feature tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_analytic_row_number_query() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE analytic_tbl (grp TEXT, val INTEGER)",
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO analytic_tbl VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)",
        )
        .await
        .unwrap();

        let df = DbAdapter::query_df(
            &adapter,
            "SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) AS rn \
             FROM analytic_tbl ORDER BY grp, val",
        )
        .await
        .expect("analytic query with ROW_NUMBER OVER should succeed");

        assert_eq!(df.height(), 4, "analytic query should return 4 rows");
        assert!(
            df.get_column_names().iter().any(|c| c.as_str() == "rn"),
            "result must include the 'rn' column from ROW_NUMBER()"
        );
    }

    #[tokio::test]
    async fn test_duckdb_get_server_info_version_non_empty() {
        use arni::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        assert!(
            !info.version.is_empty(),
            "server version field must not be empty"
        );
    }
}
