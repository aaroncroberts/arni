//! PostgreSQL adapter integration tests.
//!
//! These tests require a running PostgreSQL instance. Locally, start containers
//! with `arni dev start`. In CI, the `integration-tests` job handles this.
//!
//! Set TEST_POSTGRES_AVAILABLE=true to enable:
//! ```bash
//! export TEST_POSTGRES_AVAILABLE=true
//! cargo test -p arni-data --features postgres --test postgres
//! ```

mod common;

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::common;
    use arni_data::adapter::{Connection as ConnectionTrait, DatabaseType, DbAdapter};

    /// Load the test config for PostgreSQL, or skip the test if unavailable.
    macro_rules! pg_config {
        () => {{
            if common::skip_if_unavailable("postgres") {
                return;
            }
            match common::load_test_config("pg-dev") {
                Some(cfg) => cfg,
                None => {
                    println!("[SKIP] pg-dev profile not found in ~/.arni/connections.yml or env");
                    return;
                }
            }
        }};
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Build a connected adapter from the given config.
    async fn connected_adapter(
        cfg: &arni_data::adapter::ConnectionConfig,
    ) -> arni_data::adapters::postgres::PostgresAdapter {
        use arni_data::adapters::postgres::PostgresAdapter;
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, cfg, password.as_deref())
            .await
            .expect("postgres connect should succeed");
        adapter
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1. CONNECTION LIFECYCLE
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_connect_and_disconnect() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("postgres connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("postgres disconnect should succeed");
    }

    #[tokio::test]
    async fn test_postgres_double_connect_is_idempotent() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("first connect should succeed");

        // A second connect should not panic or return a hard error; the adapter
        // may reconnect or be a no-op, but it must not leave the adapter broken.
        let _ = DbAdapter::connect(&mut adapter, &cfg, password.as_deref()).await;

        // Must still be functional after the second connect attempt.
        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS ping")
            .await
            .expect("query should succeed after double-connect");
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_postgres_disconnect_when_not_connected_is_no_op() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        // Disconnect without ever connecting should be a no-op (or at worst a
        // benign error). We just verify it does not panic.
        let _ = DbAdapter::disconnect(&mut adapter).await;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. HEALTH CHECK
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_health_check_after_connect() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "postgres should be healthy after connect");
    }

    #[tokio::test]
    async fn test_postgres_health_check_before_connect_is_false_or_error() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let adapter = PostgresAdapter::new(cfg.clone());

        // Before connecting, health_check should either return Ok(false) or Err.
        match ConnectionTrait::health_check(&adapter).await {
            Ok(healthy) => assert!(!healthy, "health_check before connect should return false"),
            Err(_) => {
                // An error is also acceptable when not connected.
            }
        }
    }

    #[tokio::test]
    async fn test_postgres_health_check_after_disconnect_is_false_or_error() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();

        match ConnectionTrait::health_check(&adapter).await {
            Ok(healthy) => assert!(
                !healthy,
                "health_check after disconnect should return false"
            ),
            Err(_) => {
                // An error is also acceptable after disconnect.
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 3. IS_CONNECTED STATE
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_is_connected_lifecycle() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());

        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "should not be connected before connect()"
        );

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        assert!(
            ConnectionTrait::is_connected(&adapter),
            "should be connected after connect()"
        );

        DbAdapter::disconnect(&mut adapter).await.unwrap();
        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "should not be connected after disconnect()"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 4. DATABASE TYPE
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_database_type() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let adapter = PostgresAdapter::new(cfg.clone());

        assert_eq!(
            DbAdapter::database_type(&adapter),
            DatabaseType::Postgres,
            "database_type() must return DatabaseType::Postgres"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. QUERY EXECUTION
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_execute_select_1() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS value")
            .await
            .expect("SELECT 1 should succeed");
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_postgres_execute_select_multiple_columns() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT 42 AS num, 'hello' AS greeting, TRUE AS flag",
        )
        .await
        .expect("multi-column SELECT should succeed");

        assert_eq!(result.columns.len(), 3, "expected 3 columns");
        assert_eq!(result.rows.len(), 1, "expected 1 row");

        let row = &result.rows[0];
        assert_eq!(row.len(), 3, "row should have 3 values");
    }

    #[tokio::test]
    async fn test_postgres_execute_select_empty_result_set() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        // pg_class is always present; filter guarantees zero rows.
        let result = DbAdapter::execute_query(&adapter, "SELECT relname FROM pg_class WHERE FALSE")
            .await
            .expect("empty-result SELECT should succeed");

        assert_eq!(result.rows.len(), 0, "result set should be empty");
    }

    #[tokio::test]
    async fn test_postgres_execute_select_null_values() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "SELECT NULL::TEXT AS nullable_col")
            .await
            .expect("NULL SELECT should succeed");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);

        // The single value should be represented as QueryValue::Null.
        use arni_data::adapter::QueryValue;
        let val = &result.rows[0][0];
        assert_eq!(
            val,
            &QueryValue::Null,
            "NULL column value must be QueryValue::Null"
        );
    }

    #[tokio::test]
    async fn test_postgres_execute_select_multi_row() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT generate_series AS n FROM generate_series(1, 5)",
        )
        .await
        .expect("generate_series SELECT should succeed");

        assert_eq!(result.rows.len(), 5, "expected 5 rows from generate_series");
        assert_eq!(result.columns.len(), 1);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 6. CRUD LIFECYCLE
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_create_table_insert_select_drop() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_pg_test_basic";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, label TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {table} (label) VALUES ('hello'), ('world')"),
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, label FROM {table} ORDER BY id"),
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(result.rows.len(), 2, "expected 2 rows after insert");
        assert_eq!(result.columns.len(), 2);

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_update_and_verify() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_update";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, val INTEGER)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(&adapter, &format!("INSERT INTO {table} (val) VALUES (10)"))
            .await
            .expect("INSERT should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("UPDATE {table} SET val = 99 WHERE val = 10"),
        )
        .await
        .expect("UPDATE should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT val FROM {table}"))
            .await
            .expect("SELECT after UPDATE should succeed");

        assert_eq!(result.rows.len(), 1);
        use arni_data::adapter::QueryValue;
        assert_eq!(
            result.rows[0][0],
            QueryValue::Int(99),
            "updated value should be 99"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_delete_and_verify() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_delete";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, val TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {table} (val) VALUES ('keep'), ('remove')"),
        )
        .await
        .expect("INSERT should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("DELETE FROM {table} WHERE val = 'remove'"),
        )
        .await
        .expect("DELETE should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT val FROM {table}"))
            .await
            .expect("SELECT after DELETE should succeed");

        assert_eq!(result.rows.len(), 1, "one row should remain after DELETE");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 7. ERROR HANDLING
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_invalid_sql_returns_error() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_postgres_query_nonexistent_table_returns_error() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT * FROM arni_pg_table_that_does_not_exist_xyz",
        )
        .await;

        assert!(
            result.is_err(),
            "querying a non-existent table should return an error"
        );
    }

    #[tokio::test]
    async fn test_postgres_syntax_error_in_dml_returns_error() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "INSERT INTO (col1) VALUES ()").await;
        assert!(
            result.is_err(),
            "syntactically invalid DML should return an error"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 8. METADATA - LIST TABLES
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_list_tables() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        // The test DB may have tables from init SQL; just verify it doesn't error.
        let _ = tables;
    }

    #[tokio::test]
    async fn test_postgres_list_tables_contains_created_table() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_listtables";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");

        assert!(
            tables.iter().any(|t| t.eq_ignore_ascii_case(table)),
            "newly created table '{table}' must appear in list_tables result; got: {tables:?}"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_list_tables_with_public_schema() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_schema_filter";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, Some("public"))
            .await
            .expect("list_tables with schema should succeed");

        assert!(
            tables.iter().any(|t| t.eq_ignore_ascii_case(table)),
            "table '{table}' must appear when filtering by 'public' schema; got: {tables:?}"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_list_tables_dropped_table_absent() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_dropped";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");

        assert!(
            !tables.iter().any(|t| t.eq_ignore_ascii_case(table)),
            "dropped table '{table}' must not appear in list_tables; got: {tables:?}"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 9. METADATA - DESCRIBE TABLE
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_describe_table_column_names() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_describe";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT NOT NULL, score FLOAT8)"
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let info = DbAdapter::describe_table(&adapter, table, None)
            .await
            .expect("describe_table should succeed");

        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            col_names.contains(&"id"),
            "column 'id' must be present; got: {col_names:?}"
        );
        assert!(
            col_names.contains(&"name"),
            "column 'name' must be present; got: {col_names:?}"
        );
        assert!(
            col_names.contains(&"score"),
            "column 'score' must be present; got: {col_names:?}"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_describe_table_nullable_flag() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_nullable";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (required TEXT NOT NULL, optional TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let info = DbAdapter::describe_table(&adapter, table, None)
            .await
            .expect("describe_table should succeed");

        let required_col = info
            .columns
            .iter()
            .find(|c| c.name == "required")
            .expect("'required' column must exist");
        let optional_col = info
            .columns
            .iter()
            .find(|c| c.name == "optional")
            .expect("'optional' column must exist");

        assert!(
            !required_col.nullable,
            "'required' column should not be nullable"
        );
        assert!(
            optional_col.nullable,
            "'optional' column should be nullable"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_describe_table_with_schema() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_describe_schema";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let info = DbAdapter::describe_table(&adapter, table, Some("public"))
            .await
            .expect("describe_table with explicit schema should succeed");

        assert!(
            !info.columns.is_empty(),
            "table must have at least one column"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 10. METADATA - VIEWS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_get_views_contains_created_view() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let base_table = "arni_pg_test_view_base";
        let view_name = "arni_pg_test_view";

        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP VIEW IF EXISTS {view_name}")).await;
        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {base_table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {base_table} (id SERIAL PRIMARY KEY, label TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE VIEW {view_name} AS SELECT id, label FROM {base_table}"),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let views = DbAdapter::get_views(&adapter, None)
            .await
            .expect("get_views should succeed");

        assert!(
            views.iter().any(|v| v.name.eq_ignore_ascii_case(view_name)),
            "created view '{view_name}' must appear in get_views; got: {:?}",
            views.iter().map(|v| &v.name).collect::<Vec<_>>()
        );

        DbAdapter::execute_query(&adapter, &format!("DROP VIEW {view_name}"))
            .await
            .expect("DROP VIEW should succeed");
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {base_table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_get_views_with_public_schema() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        // list_views for 'public' schema must succeed even when empty.
        let views = DbAdapter::get_views(&adapter, Some("public"))
            .await
            .expect("get_views with explicit schema should succeed");
        let _ = views;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 11. METADATA - INDEXES
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_get_indexes_contains_created_index() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_indexes";
        let index = "arni_pg_test_idx_label";

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, label TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE INDEX {index} ON {table} (label)"),
        )
        .await
        .expect("CREATE INDEX should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, table, None)
            .await
            .expect("get_indexes should succeed");

        assert!(
            indexes.iter().any(|i| i.name.eq_ignore_ascii_case(index)),
            "created index '{index}' must appear in get_indexes; got: {:?}",
            indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_get_indexes_primary_key_present() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_pk_index";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, val TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, table, None)
            .await
            .expect("get_indexes should succeed");

        // There must be at least the implicit primary-key index.
        assert!(
            !indexes.is_empty(),
            "a table with PRIMARY KEY must have at least one index"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 12. METADATA - FOREIGN KEYS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_get_foreign_keys_contains_created_fk() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let parent = "arni_pg_test_fk_parent";
        let child = "arni_pg_test_fk_child";

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {child}")).await;
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {parent}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {parent} (id SERIAL PRIMARY KEY, name TEXT)"),
        )
        .await
        .expect("CREATE parent TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {child} (
                    id SERIAL PRIMARY KEY,
                    parent_id INTEGER NOT NULL,
                    CONSTRAINT arni_pg_fk_parent FOREIGN KEY (parent_id) REFERENCES {parent}(id)
                )"
            ),
        )
        .await
        .expect("CREATE child TABLE with FK should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, child, None)
            .await
            .expect("get_foreign_keys should succeed");

        assert!(
            !fks.is_empty(),
            "child table must have at least one foreign key"
        );
        assert!(
            fks.iter()
                .any(|fk| fk.referenced_table.eq_ignore_ascii_case(parent)),
            "FK must reference the parent table '{parent}'; got: {:?}",
            fks.iter()
                .map(|fk| &fk.referenced_table)
                .collect::<Vec<_>>()
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {child}"))
            .await
            .expect("DROP child TABLE should succeed");
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {parent}"))
            .await
            .expect("DROP parent TABLE should succeed");
    }

    #[tokio::test]
    async fn test_postgres_get_foreign_keys_empty_for_standalone_table() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_test_no_fk";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, val TEXT)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, table, None)
            .await
            .expect("get_foreign_keys should succeed for table without FKs");

        assert!(
            fks.is_empty(),
            "table with no foreign keys must return an empty vec"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 13. METADATA - LIST DATABASES
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_list_databases_returns_non_empty_vec() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let dbs = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        assert!(
            !dbs.is_empty(),
            "list_databases must return at least one database"
        );
    }

    #[tokio::test]
    async fn test_postgres_list_databases_contains_current_db() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let current_db = cfg.database.clone();
        let dbs = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        assert!(
            dbs.iter().any(|d| d.eq_ignore_ascii_case(&current_db)),
            "list_databases must include the current database '{current_db}'; got: {dbs:?}"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 14. METADATA - LIST STORED PROCEDURES
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_list_stored_procedures_succeeds() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let procs = DbAdapter::list_stored_procedures(&adapter, None)
            .await
            .expect("list_stored_procedures should succeed");
        // May return empty — that is fine. We only assert success.
        let _ = procs;
    }

    #[tokio::test]
    async fn test_postgres_list_stored_procedures_with_schema_succeeds() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let procs = DbAdapter::list_stored_procedures(&adapter, Some("public"))
            .await
            .expect("list_stored_procedures with explicit schema should succeed");
        let _ = procs;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 15. DATAFRAME OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_query_df_returns_valid_dataframe() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let df = DbAdapter::query_df(
            &adapter,
            "SELECT generate_series AS n FROM generate_series(1, 3)",
        )
        .await
        .expect("query_df should succeed");

        assert_eq!(df.height(), 3, "DataFrame should have 3 rows");
        assert_eq!(df.width(), 1, "DataFrame should have 1 column");
    }

    #[tokio::test]
    async fn test_postgres_query_df_correct_column_names() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let df = DbAdapter::query_df(&adapter, "SELECT 1 AS alpha, 2 AS beta, 3 AS gamma")
            .await
            .expect("query_df should succeed");

        let col_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        assert!(
            col_names.iter().any(|n| n == "alpha"),
            "column 'alpha' must be present; got: {col_names:?}"
        );
        assert!(
            col_names.iter().any(|n| n == "beta"),
            "column 'beta' must be present; got: {col_names:?}"
        );
        assert!(
            col_names.iter().any(|n| n == "gamma"),
            "column 'gamma' must be present; got: {col_names:?}"
        );
    }

    #[tokio::test]
    async fn test_postgres_query_df_empty_result() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let df = DbAdapter::query_df(&adapter, "SELECT relname FROM pg_class WHERE FALSE")
            .await
            .expect("query_df with empty result should succeed");

        assert_eq!(
            df.height(),
            0,
            "DataFrame from empty query must have 0 rows"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 16. NUMERIC / TYPE HANDLING
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_integer_values_in_result() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "SELECT 42::BIGINT AS n")
            .await
            .expect("integer SELECT should succeed");

        use arni_data::adapter::QueryValue;
        assert_eq!(result.rows[0][0], QueryValue::Int(42));
    }

    #[tokio::test]
    async fn test_postgres_float_values_in_result() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "SELECT 1.5::FLOAT8 AS f")
            .await
            .expect("float SELECT should succeed");

        use arni_data::adapter::QueryValue;
        match &result.rows[0][0] {
            QueryValue::Float(f) => {
                assert!(
                    (f - 1.5_f64).abs() < 1e-9,
                    "float value should be 1.5, got {f}"
                );
            }
            other => panic!("expected QueryValue::Float, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_postgres_text_values_in_result() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "SELECT 'hello world'::TEXT AS txt")
            .await
            .expect("text SELECT should succeed");

        use arni_data::adapter::QueryValue;
        assert_eq!(
            result.rows[0][0],
            QueryValue::Text("hello world".to_string())
        );
    }

    #[tokio::test]
    async fn test_postgres_boolean_values_in_result() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(&adapter, "SELECT TRUE AS t, FALSE AS f")
            .await
            .expect("boolean SELECT should succeed");

        use arni_data::adapter::QueryValue;
        assert_eq!(
            result.rows[0][0],
            QueryValue::Bool(true),
            "TRUE should map to QueryValue::Bool(true)"
        );
        assert_eq!(
            result.rows[0][1],
            QueryValue::Bool(false),
            "FALSE should map to QueryValue::Bool(false)"
        );
    }

    #[tokio::test]
    async fn test_postgres_mixed_types_in_single_row() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT 1::BIGINT AS i, 2.5::FLOAT8 AS f, 'text'::TEXT AS t, TRUE AS b, NULL::TEXT AS n",
        )
        .await
        .expect("mixed-type SELECT should succeed");

        use arni_data::adapter::QueryValue;
        assert_eq!(result.columns.len(), 5);
        assert_eq!(result.rows.len(), 1);

        let row = &result.rows[0];
        assert_eq!(row[0], QueryValue::Int(1));
        assert!(matches!(row[1], QueryValue::Float(_)));
        assert_eq!(row[2], QueryValue::Text("text".to_string()));
        assert_eq!(row[3], QueryValue::Bool(true));
        assert_eq!(row[4], QueryValue::Null);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 17. CONFIG ACCESSOR
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_config_returns_original_config() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let adapter = PostgresAdapter::new(cfg.clone());

        let returned = ConnectionTrait::config(&adapter);
        assert_eq!(returned.id, cfg.id, "config() must return the original id");
        assert_eq!(
            returned.database, cfg.database,
            "config() must return the original database"
        );
        assert_eq!(
            returned.db_type,
            arni_data::adapter::DatabaseType::Postgres,
            "config() db_type must be Postgres"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 18. get_server_info
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_pg_get_server_info() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        assert!(
            !info.version.is_empty(),
            "server version should not be empty; got: {:?}",
            info.version
        );
        assert_eq!(
            info.server_type, "PostgreSQL",
            "server_type should be 'PostgreSQL'; got: {:?}",
            info.server_type
        );
    }

    #[tokio::test]
    async fn test_pg_server_info_version_contains_postgres() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        let version_lower = info.version.to_lowercase();
        assert!(
            version_lower.contains("postgresql"),
            "version string should contain 'PostgreSQL'; got: {}",
            info.version
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 19. get_view_definition
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_pg_get_view_definition() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_vdef_base";
        let view = "arni_pg_vdef_view";

        // Cleanup from any previous run
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP VIEW IF EXISTS {}", view)).await;
        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {}", table)).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {} (id BIGINT, val TEXT)", table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE VIEW {} AS SELECT id, val FROM {}", view, table),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let def = DbAdapter::get_view_definition(&adapter, view, Some("public"))
            .await
            .expect("get_view_definition should succeed");

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP VIEW IF EXISTS {}", view)).await;
        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {}", table)).await;

        let def_str = def.expect("view definition should be Some");
        assert!(
            def_str.to_lowercase().contains("select"),
            "definition should contain SELECT; got: {}",
            def_str
        );
    }

    #[tokio::test]
    async fn test_pg_get_view_definition_nonexistent() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let result =
            DbAdapter::get_view_definition(&adapter, "arni_pg_no_such_view_xyzzy", Some("public"))
                .await
                .expect("get_view_definition for nonexistent view should return Ok");

        assert!(
            result.is_none(),
            "nonexistent view should return None; got: {:?}",
            result
        );
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BULK OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_bulk_insert_multi_row_returns_count() {
        use arni_data::adapter::QueryValue;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_bulk_insert_count";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, name TEXT, score INT)"),
        )
        .await
        .unwrap();

        let columns = vec!["name".to_string(), "score".to_string()];
        let rows = vec![
            vec![QueryValue::Text("Alice".to_string()), QueryValue::Int(90)],
            vec![QueryValue::Text("Bob".to_string()), QueryValue::Int(85)],
            vec![QueryValue::Text("Carol".to_string()), QueryValue::Int(92)],
        ];

        let n = DbAdapter::bulk_insert(&adapter, table, &columns, &rows, None)
            .await
            .expect("bulk_insert should succeed");

        assert_eq!(n, 3, "bulk_insert should return 3 rows affected");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        let count_val = &result.rows[0][0];
        assert!(
            matches!(count_val, QueryValue::Int(3)),
            "expected 3 rows in table, got {:?}",
            count_val
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_bulk_insert_empty_rows_returns_zero() {
        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_bulk_insert_empty";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, val INT)"),
        )
        .await
        .unwrap();

        let n = DbAdapter::bulk_insert(
            &adapter,
            table,
            &["val".to_string()],
            &[], // empty rows
            None,
        )
        .await
        .expect("bulk_insert with empty rows should succeed");

        assert_eq!(n, 0, "empty rows should return 0");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_bulk_insert_column_count_mismatch_returns_err() {
        use arni_data::adapter::QueryValue;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let columns = vec!["a".to_string(), "b".to_string()]; // 2 columns
        let rows = vec![
            vec![QueryValue::Int(1)], // only 1 value — mismatch
        ];

        let result = DbAdapter::bulk_insert(&adapter, "any_table", &columns, &rows, None).await;
        assert!(result.is_err(), "column count mismatch should return Err");
    }

    #[tokio::test]
    async fn test_bulk_insert_null_values_stored_as_null() {
        use arni_data::adapter::QueryValue;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_bulk_insert_null";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id SERIAL PRIMARY KEY, note TEXT)"),
        )
        .await
        .unwrap();

        let columns = vec!["note".to_string()];
        let rows = vec![vec![QueryValue::Null]];

        DbAdapter::bulk_insert(&adapter, table, &columns, &rows, None)
            .await
            .expect("inserting NULL should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT note FROM {table}"))
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(
            matches!(result.rows[0][0], QueryValue::Null),
            "NULL value should round-trip as NULL, got {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_bulk_update_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};
        use std::collections::HashMap;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_bulk_update";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {table} VALUES (1, 'pending'), (2, 'pending'), (3, 'done')"),
        )
        .await
        .unwrap();

        // Update only id = 1
        let mut set_clauses = HashMap::new();
        set_clauses.insert("status".to_string(), QueryValue::Text("active".to_string()));
        let filter = FilterExpr::Eq("id".to_string(), QueryValue::Int(1));

        let n = DbAdapter::bulk_update(&adapter, table, &[(set_clauses, filter)], Some("public"))
            .await
            .expect("bulk_update should succeed");

        assert_eq!(n, 1, "should update exactly 1 row");

        // Verify only id=1 changed
        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, status FROM {table} ORDER BY id"),
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(
            matches!(&result.rows[1][1], QueryValue::Text(s) if s == "pending"),
            "id=2 should remain 'pending'"
        );
        assert!(
            matches!(&result.rows[2][1], QueryValue::Text(s) if s == "done"),
            "id=3 should remain 'done'"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_bulk_delete_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_bulk_delete";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, tag TEXT)"),
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {table} VALUES (1,'a'), (2,'b'), (3,'a')"),
        )
        .await
        .unwrap();

        // Delete where tag = 'a' (2 rows)
        let filter = FilterExpr::Eq("tag".to_string(), QueryValue::Text("a".to_string()));
        let n = DbAdapter::bulk_delete(&adapter, table, &[filter], Some("public"))
            .await
            .expect("bulk_delete should succeed");

        assert_eq!(n, 2, "should delete 2 rows where tag='a'");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(1)),
            "1 row should remain"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DATAFRAME ROUND-TRIP TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_round_trip_schema_matches() {
        use polars::prelude::*;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_rt_schema";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        let original = df! {
            "id"    => [1i64, 2, 3],
            "name"  => ["alice", "bob", "carol"],
            "score" => [9.5f64, 8.0, 7.25],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, table, None, true)
            .await
            .expect("export should succeed");

        let read_back = DbAdapter::read_table(&adapter, table, None)
            .await
            .expect("read_table should succeed");

        let mut orig_cols: Vec<String> = original
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut back_cols: Vec<String> = read_back
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        orig_cols.sort_unstable();
        back_cols.sort_unstable();
        assert_eq!(
            orig_cols, back_cols,
            "column names must match after round-trip"
        );
        assert_eq!(read_back.height(), 3, "row count must be preserved");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_postgres_round_trip_values_preserved() {
        use polars::prelude::*;

        let cfg = pg_config!();
        let adapter = connected_adapter(&cfg).await;

        let table = "arni_pg_rt_values";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        let original = df! {
            "id"    => [10i64, 20, 30],
            "label" => ["x", "y", "z"],
            "val"   => [1.1f64, 2.2, 3.3],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, table, None, true)
            .await
            .unwrap();

        let read_back = DbAdapter::read_table(&adapter, table, None).await.unwrap();

        let orig_ids: Vec<i64> = original
            .column("id")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let back_ids: Vec<i64> = read_back
            .column("id")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(orig_ids, back_ids, "id column values must round-trip");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // test_connection() INTEGRATION TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_postgres_test_connection_valid_credentials_returns_true() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let adapter = PostgresAdapter::new(cfg.clone());

        let result = DbAdapter::test_connection(&adapter, &cfg, password.as_deref())
            .await
            .expect("test_connection should not error with valid credentials");

        assert!(
            result,
            "test_connection should return true with valid credentials"
        );
    }

    #[tokio::test]
    async fn test_postgres_test_connection_wrong_password_returns_false() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let adapter = PostgresAdapter::new(cfg.clone());

        let result = DbAdapter::test_connection(&adapter, &cfg, Some("totally_wrong_password_xyz"))
            .await
            .expect("test_connection with wrong password should return Ok(false), not Err");

        assert!(
            !result,
            "test_connection should return false with wrong password"
        );
    }

    #[tokio::test]
    async fn test_postgres_test_connection_unreachable_host_returns_false() {
        use arni_data::adapter::ConnectionConfig;
        use arni_data::adapters::postgres::PostgresAdapter;
        use std::collections::HashMap;

        // Port 1 on localhost is refused instantly
        let cfg = ConnectionConfig {
            id: "unreachable".to_string(),
            name: "unreachable".to_string(),
            db_type: arni_data::adapter::DatabaseType::Postgres,
            host: Some("127.0.0.1".to_string()),
            port: Some(1),
            database: "test_db".to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let adapter = PostgresAdapter::new(cfg.clone());

        let result = DbAdapter::test_connection(&adapter, &cfg, Some("password"))
            .await
            .expect("test_connection with unreachable host should return Ok(false), not Err");

        assert!(
            !result,
            "test_connection should return false for unreachable host"
        );
    }
}
