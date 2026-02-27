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

        let result = DbAdapter::execute_query(&adapter, "SELECT id, name FROM test_users ORDER BY id")
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
        assert!(tables.is_empty(), "fresh :memory: DB should have no user tables");
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
        assert!(col_names.contains(&"label"), "should include 'label' column");
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
}
