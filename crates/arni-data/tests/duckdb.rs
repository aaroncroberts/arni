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
    use arni_data::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType};
    use arni_data::adapters::duckdb::DuckDbAdapter;
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
        use arni_data::adapter::DbAdapter;

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
        use arni_data::adapter::DbAdapter;

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
        use arni_data::adapter::DbAdapter;

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
        use arni_data::adapter::DbAdapter;

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
        use arni_data::adapter::DbAdapter;

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
        use arni_data::adapter::DbAdapter;

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
    }

    // ── Edge cases ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_duckdb_invalid_sql_returns_error() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let mut adapter = DuckDbAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter).await.unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_duckdb_database_type() {
        use arni_data::adapter::DbAdapter;

        let cfg = memory_config();
        let adapter = DuckDbAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::DuckDB);
    }

    #[tokio::test]
    async fn test_duckdb_arithmetic_query() {
        use arni_data::adapter::{DbAdapter, QueryValue};

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
}
