//! SQL Server (MSSQL) adapter integration tests.
//!
//! These tests require a running SQL Server instance. Locally, start containers
//! with `arni dev start`. In CI, the `integration-tests` job handles this.
//!
//! Set TEST_MSSQL_AVAILABLE=true to enable:
//! ```bash
//! export TEST_MSSQL_AVAILABLE=true
//! cargo test -p arni-data --features mssql --test mssql
//! ```

mod common;

#[cfg(feature = "mssql")]
mod mssql_tests {
    use super::common;
    use arni_data::adapter::{Connection as ConnectionTrait, DatabaseType, DbAdapter};

    macro_rules! mssql_config {
        () => {{
            if common::skip_if_unavailable("mssql") {
                return;
            }
            match common::load_test_config("mssql-dev") {
                Some(cfg) => cfg,
                None => {
                    println!(
                        "[SKIP] mssql-dev profile not found in ~/.arni/connections.yml or env"
                    );
                    return;
                }
            }
        }};
    }

    /// Helper: drop a table using MSSQL's IF OBJECT_ID guard.
    macro_rules! drop_table_if_exists {
        ($adapter:expr, $table:expr) => {
            let _ = DbAdapter::execute_query(
                $adapter,
                &format!(
                    "IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}",
                    $table, $table
                ),
            )
            .await;
        };
    }

    /// Helper: drop a view using MSSQL's IF OBJECT_ID guard.
    macro_rules! drop_view_if_exists {
        ($adapter:expr, $view:expr) => {
            let _ = DbAdapter::execute_query(
                $adapter,
                &format!(
                    "IF OBJECT_ID('{}', 'V') IS NOT NULL DROP VIEW {}",
                    $view, $view
                ),
            )
            .await;
        };
    }

    // ── Connection lifecycle ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_connect_and_disconnect() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("mssql connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("mssql disconnect should succeed");
    }

    #[tokio::test]
    async fn test_mssql_reconnect_after_disconnect() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("first connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("disconnect should succeed");

        // Re-connect after disconnect.
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("second connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("second disconnect should succeed");
    }

    // ── Health check ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_health_check_before_connect_returns_false() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let adapter = SqlServerAdapter::new(cfg.clone());

        // No connection established — health_check should return Ok(false) or an
        // error; it must NOT panic.
        let result = ConnectionTrait::health_check(&adapter).await;
        if let Ok(healthy) = result {
            assert!(!healthy, "should not be healthy before connect");
        } // Err(_) is also acceptable
    }

    #[tokio::test]
    async fn test_mssql_health_check_after_connect() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "mssql should be healthy after connect");
    }

    // ── is_connected state ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_is_connected_before_connect() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let adapter = SqlServerAdapter::new(cfg.clone());
        // The MSSQL adapter reports false before any connection is made.
        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "is_connected should be false before connect"
        );
    }

    // ── database_type ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_database_type() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let adapter = SqlServerAdapter::new(cfg.clone());
        assert_eq!(
            DbAdapter::database_type(&adapter),
            DatabaseType::SQLServer,
            "database_type() must return DatabaseType::SQLServer"
        );
    }

    // ── Query execution ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_execute_select_1() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
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
    async fn test_mssql_execute_multi_column_select() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result =
            DbAdapter::execute_query(&adapter, "SELECT 1 AS num, N'hello' AS str, 3.14 AS flt")
                .await
                .expect("multi-column SELECT should succeed");

        assert_eq!(result.columns.len(), 3, "expected 3 columns");
        assert_eq!(result.rows.len(), 1, "expected 1 row");
        assert_eq!(result.columns[0], "num");
        assert_eq!(result.columns[1], "str");
        assert_eq!(result.columns[2], "flt");
    }

    #[tokio::test]
    async fn test_mssql_execute_select_with_null_values() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // T-SQL NULL literal with explicit CAST.
        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT CAST(NULL AS NVARCHAR(100)) AS nullable_col",
        )
        .await
        .expect("SELECT with NULL should succeed");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_mssql_execute_empty_result_set() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // A WHERE clause that can never be true returns zero rows.
        let table = "arni_mssql_empty_result";
        drop_table_if_exists!(&adapter, table);
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let result =
            DbAdapter::execute_query(&adapter, &format!("SELECT id FROM {} WHERE 1 = 0", table))
                .await
                .expect("SELECT with impossible WHERE should succeed");

        drop_table_if_exists!(&adapter, table);

        // An empty result set may return zero columns and zero rows, or the
        // column list but zero rows — both are valid.
        assert_eq!(
            result.rows.len(),
            0,
            "expected zero rows from impossible WHERE"
        );
    }

    // ── CRUD lifecycle ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_create_table_insert_select_drop() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_test_mssql_basic";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, label NVARCHAR(100))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {} (label) VALUES ('hello'), ('world')", table),
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, label FROM {} ORDER BY id", table),
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(result.rows.len(), 2);

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {}", table))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mssql_full_crud_lifecycle() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_crud";
        drop_table_if_exists!(&adapter, table);

        // CREATE
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100), score FLOAT)",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // INSERT
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {} (name, score) VALUES ('Alice', 9.5), ('Bob', 7.2), ('Carol', 8.8)",
                table
            ),
        )
        .await
        .expect("INSERT should succeed");

        // SELECT after insert
        let select_result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, name, score FROM {} ORDER BY id", table),
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(select_result.rows.len(), 3, "expected 3 rows after INSERT");

        // UPDATE
        DbAdapter::execute_query(
            &adapter,
            &format!("UPDATE {} SET score = 10.0 WHERE name = N'Alice'", table),
        )
        .await
        .expect("UPDATE should succeed");

        let updated = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT score FROM {} WHERE name = N'Alice'", table),
        )
        .await
        .expect("SELECT after UPDATE should succeed");
        assert_eq!(updated.rows.len(), 1);

        // DELETE
        DbAdapter::execute_query(
            &adapter,
            &format!("DELETE FROM {} WHERE name = N'Bob'", table),
        )
        .await
        .expect("DELETE should succeed");

        let after_delete =
            DbAdapter::execute_query(&adapter, &format!("SELECT id FROM {} ORDER BY id", table))
                .await
                .expect("SELECT after DELETE should succeed");
        assert_eq!(after_delete.rows.len(), 2, "expected 2 rows after DELETE");

        // DROP
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}",
                table, table
            ),
        )
        .await
        .expect("DROP TABLE should succeed");
    }

    // ── Error handling ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_invalid_sql_returns_error() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_mssql_select_from_nonexistent_table_returns_error() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT * FROM arni_mssql_definitely_does_not_exist_xyzzy",
        )
        .await;
        assert!(
            result.is_err(),
            "querying a nonexistent table should return an error"
        );
    }

    #[tokio::test]
    async fn test_mssql_query_before_connect_returns_error() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        // Intentionally do NOT call connect.
        let adapter = SqlServerAdapter::new(cfg.clone());

        let result = DbAdapter::execute_query(&adapter, "SELECT 1").await;
        assert!(
            result.is_err(),
            "execute_query before connect should return an error"
        );
    }

    // ── Metadata: get_server_info ────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_get_server_info() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        assert!(
            !info.version.is_empty(),
            "server version string should not be empty; got: {:?}",
            info.version
        );
        // MSSQL adapter sets server_type = "SQL Server"
        assert_eq!(info.server_type, "SQL Server");
    }

    #[tokio::test]
    async fn test_mssql_server_info_version_contains_sql_server() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        // @@VERSION always contains "Microsoft SQL Server" or "SQL Server".
        let version_lower = info.version.to_lowercase();
        assert!(
            version_lower.contains("sql server") || version_lower.contains("microsoft"),
            "version string should mention SQL Server; got: {}",
            info.version
        );
    }

    // ── Metadata: list_tables ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_list_tables() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        // list_tables returns a Vec; the test DB may or may not have tables
        let _ = tables;
    }

    #[tokio::test]
    async fn test_mssql_list_tables_includes_created_table() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_list_tables_check";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY)", table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, Some("dbo"))
            .await
            .expect("list_tables should succeed");

        drop_table_if_exists!(&adapter, table);

        assert!(
            tables.iter().any(|t| t.eq_ignore_ascii_case(table)),
            "list_tables should include '{}'; got: {:?}",
            table,
            tables
        );
    }

    #[tokio::test]
    async fn test_mssql_list_tables_dbo_schema_explicit() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Passing Some("dbo") explicitly should behave the same as None.
        let tables_none = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables(None) should succeed");
        let tables_dbo = DbAdapter::list_tables(&adapter, Some("dbo"))
            .await
            .expect("list_tables(Some(\"dbo\")) should succeed");

        // Both calls target the "dbo" schema (None defaults to "dbo" in the
        // adapter).  We cannot assert exact count equality because concurrent
        // tests may create/drop tables between the two calls, but each result
        // must be a valid list of non-empty strings.
        assert!(
            tables_none.iter().all(|t| !t.is_empty()),
            "list_tables(None) should return non-empty table names"
        );
        assert!(
            tables_dbo.iter().all(|t| !t.is_empty()),
            "list_tables(Some(\"dbo\")) should return non-empty table names"
        );
    }

    // ── Metadata: describe_table ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_describe_table() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_describe";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} \
                 (id INT IDENTITY(1,1) PRIMARY KEY, \
                  name NVARCHAR(200) NOT NULL, \
                  score FLOAT, \
                  active BIT)",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let info = DbAdapter::describe_table(&adapter, table, Some("dbo"))
            .await
            .expect("describe_table should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(
            info.name, table,
            "TableInfo.name should match the table name"
        );
        assert_eq!(
            info.columns.len(),
            4,
            "expected 4 columns; got: {:?}",
            info.columns
        );

        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            col_names.contains(&"id"),
            "columns should include 'id'; got: {:?}",
            col_names
        );
        assert!(
            col_names.contains(&"name"),
            "columns should include 'name'; got: {:?}",
            col_names
        );
        assert!(
            col_names.contains(&"score"),
            "columns should include 'score'; got: {:?}",
            col_names
        );
        assert!(
            col_names.contains(&"active"),
            "columns should include 'active'; got: {:?}",
            col_names
        );
    }

    #[tokio::test]
    async fn test_mssql_describe_nonexistent_table_returns_error() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result =
            DbAdapter::describe_table(&adapter, "arni_mssql_no_such_table_xyzzy", Some("dbo"))
                .await;
        assert!(
            result.is_err(),
            "describe_table on nonexistent table should return an error"
        );
    }

    // ── Metadata: get_views ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_get_views() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let views = DbAdapter::get_views(&adapter, Some("dbo"))
            .await
            .expect("get_views should succeed");
        // An empty list is valid in a fresh test database.
        let _ = views;
    }

    #[tokio::test]
    async fn test_mssql_get_views_includes_created_view() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_view_base";
        let view = "arni_mssql_view_check";

        drop_view_if_exists!(&adapter, view);
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE VIEW {} AS SELECT id, val FROM {}", view, table),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let views = DbAdapter::get_views(&adapter, Some("dbo"))
            .await
            .expect("get_views should succeed");

        drop_view_if_exists!(&adapter, view);
        drop_table_if_exists!(&adapter, table);

        assert!(
            views.iter().any(|v| v.name.eq_ignore_ascii_case(view)),
            "get_views should include '{}'; got: {:?}",
            view,
            views.iter().map(|v| &v.name).collect::<Vec<_>>()
        );
    }

    // ── Metadata: get_indexes ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_get_indexes() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_indexes";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, email NVARCHAR(200))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // Create an explicit non-clustered index.
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE NONCLUSTERED INDEX idx_arni_mssql_email ON {} (email)",
                table
            ),
        )
        .await
        .expect("CREATE INDEX should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, table, Some("dbo"))
            .await
            .expect("get_indexes should succeed");

        drop_table_if_exists!(&adapter, table);

        // The clustered primary-key index plus our explicit index should be present.
        assert!(
            !indexes.is_empty(),
            "get_indexes should return at least one index for a table with a PK"
        );

        let index_names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
        assert!(
            index_names
                .iter()
                .any(|n| n.contains("idx_arni_mssql_email")),
            "indexes should include 'idx_arni_mssql_email'; got: {:?}",
            index_names
        );
    }

    #[tokio::test]
    async fn test_mssql_get_indexes_primary_key_present() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_pk_index";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, table, Some("dbo"))
            .await
            .expect("get_indexes should succeed");

        drop_table_if_exists!(&adapter, table);

        let has_primary = indexes.iter().any(|i| i.is_primary);
        assert!(
            has_primary,
            "get_indexes should include the primary key index; got: {:?}",
            indexes
        );
    }

    // ── Metadata: get_foreign_keys ───────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_get_foreign_keys() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let parent = "arni_mssql_fk_parent";
        let child = "arni_mssql_fk_child";

        // Clean up leftovers from previous runs — child first, then parent.
        drop_table_if_exists!(&adapter, child);
        drop_table_if_exists!(&adapter, parent);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, label NVARCHAR(100))",
                parent
            ),
        )
        .await
        .expect("CREATE parent TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} \
                 (id INT IDENTITY(1,1) PRIMARY KEY, \
                  parent_id INT, \
                  CONSTRAINT fk_arni_mssql_parent \
                    FOREIGN KEY (parent_id) REFERENCES {} (id))",
                child, parent
            ),
        )
        .await
        .expect("CREATE child TABLE with FK should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, child, Some("dbo"))
            .await
            .expect("get_foreign_keys should succeed");

        // Cleanup
        drop_table_if_exists!(&adapter, child);
        drop_table_if_exists!(&adapter, parent);

        assert!(
            !fks.is_empty(),
            "get_foreign_keys should return at least one FK for the child table"
        );

        let fk = &fks[0];
        assert!(
            fk.referenced_table.eq_ignore_ascii_case(parent),
            "FK should reference '{}'; got: '{}'",
            parent,
            fk.referenced_table
        );
        assert!(
            fk.columns.contains(&"parent_id".to_string()),
            "FK columns should include 'parent_id'; got: {:?}",
            fk.columns
        );
    }

    #[tokio::test]
    async fn test_mssql_get_foreign_keys_no_fks_returns_empty() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_no_fk_table";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, table, Some("dbo"))
            .await
            .expect("get_foreign_keys should succeed on table without FKs");

        drop_table_if_exists!(&adapter, table);

        assert!(
            fks.is_empty(),
            "expected no FKs for a table with no foreign keys; got: {:?}",
            fks
        );
    }

    // ── Metadata: list_databases ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_list_databases() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let databases = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        // The MSSQL adapter filters database_id > 4 (system databases are 1-4)
        // so the result may be empty if only system databases are present,
        // OR it may include user databases.  We only assert the call succeeds.
        let _ = databases;
    }

    #[tokio::test]
    async fn test_mssql_list_databases_is_vec_of_strings() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Verify the return type contains valid non-empty strings (if any).
        let databases = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        for db in &databases {
            assert!(!db.is_empty(), "database name should not be empty");
        }
    }

    // ── Metadata: list_stored_procedures ────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_list_stored_procedures() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let procedures = DbAdapter::list_stored_procedures(&adapter, Some("dbo"))
            .await
            .expect("list_stored_procedures should succeed");

        // A fresh test database may have zero stored procedures — that is fine.
        let _ = procedures;
    }

    #[tokio::test]
    async fn test_mssql_list_stored_procedures_none_schema() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Passing None should default to 'dbo' and not panic.
        let procedures = DbAdapter::list_stored_procedures(&adapter, None)
            .await
            .expect("list_stored_procedures(None) should succeed");

        let _ = procedures;
    }

    // ── DataFrame operations ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_query_df_returns_correct_shape() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_query_df";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {} (val) VALUES ('a'), ('b'), ('c')", table),
        )
        .await
        .expect("INSERT should succeed");

        let df = DbAdapter::query_df(
            &adapter,
            &format!("SELECT id, val FROM {} ORDER BY id", table),
        )
        .await
        .expect("query_df should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(df.height(), 3, "DataFrame should have 3 rows");
        assert_eq!(df.width(), 2, "DataFrame should have 2 columns");
    }

    #[tokio::test]
    async fn test_mssql_query_df_select_1() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let df = DbAdapter::query_df(&adapter, "SELECT 1 AS n")
            .await
            .expect("query_df(SELECT 1) should succeed");

        assert_eq!(df.height(), 1, "expected 1 row");
        assert_eq!(df.width(), 1, "expected 1 column");
    }

    // ── Type handling ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_type_int_nvarchar_float_bit() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_types";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} \
                 (id INT IDENTITY(1,1) PRIMARY KEY, \
                  name NVARCHAR(100), \
                  score FLOAT, \
                  active BIT)",
                table
            ),
        )
        .await
        .expect("CREATE TABLE with mixed types should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {} (name, score, active) VALUES (N'Alice', 9.5, 1)",
                table
            ),
        )
        .await
        .expect("INSERT with mixed types should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, name, score, active FROM {}", table),
        )
        .await
        .expect("SELECT with mixed types should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(result.rows.len(), 1, "expected 1 row");
        assert_eq!(result.columns.len(), 4, "expected 4 columns");
    }

    #[tokio::test]
    async fn test_mssql_type_null_column_handling() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_nulls";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} \
                 (id INT IDENTITY(1,1) PRIMARY KEY, \
                  nullable_val NVARCHAR(100))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // Insert a row with an explicit NULL value.
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {} (nullable_val) VALUES (CAST(NULL AS NVARCHAR(100)))",
                table
            ),
        )
        .await
        .expect("INSERT NULL should succeed");

        let result =
            DbAdapter::execute_query(&adapter, &format!("SELECT id, nullable_val FROM {}", table))
                .await
                .expect("SELECT with NULL column should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(result.rows.len(), 1, "expected 1 row");
    }

    #[tokio::test]
    async fn test_mssql_type_bit_values() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_bit_type";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, flag BIT)",
                table
            ),
        )
        .await
        .expect("CREATE TABLE with BIT should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {} (flag) VALUES (1), (0)", table),
        )
        .await
        .expect("INSERT BIT values should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, flag FROM {} ORDER BY id", table),
        )
        .await
        .expect("SELECT BIT column should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(result.rows.len(), 2, "expected 2 rows");
    }

    #[tokio::test]
    async fn test_mssql_type_nvarchar_unicode() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_unicode";
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, content NVARCHAR(500))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // Insert Unicode text using N'' prefix.
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {} (content) VALUES (N'こんにちは'), (N'Привет')",
                table
            ),
        )
        .await
        .expect("INSERT Unicode NVARCHAR should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT content FROM {} ORDER BY id", table),
        )
        .await
        .expect("SELECT Unicode NVARCHAR should succeed");

        drop_table_if_exists!(&adapter, table);

        assert_eq!(result.rows.len(), 2, "expected 2 Unicode rows");
    }

    // ── Metadata: get_view_definition ────────────────────────────────────────

    #[tokio::test]
    async fn test_mssql_get_view_definition() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_mssql_vdef_base";
        let view = "arni_mssql_vdef_view";

        drop_view_if_exists!(&adapter, view);
        drop_table_if_exists!(&adapter, table);

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(255))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE VIEW {} AS SELECT id, val FROM {}", view, table),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let def = DbAdapter::get_view_definition(&adapter, view, Some("dbo"))
            .await
            .expect("get_view_definition should succeed");

        drop_view_if_exists!(&adapter, view);
        drop_table_if_exists!(&adapter, table);

        let def_str = def.expect("view definition should be Some");
        assert!(
            def_str.to_lowercase().contains("select"),
            "definition should contain SELECT; got: {}",
            def_str
        );
    }

    #[tokio::test]
    async fn test_mssql_get_view_definition_nonexistent() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result =
            DbAdapter::get_view_definition(&adapter, "arni_mssql_no_such_view_xyzzy", Some("dbo"))
                .await
                .expect("get_view_definition for nonexistent view should return Ok");

        assert!(
            result.is_none(),
            "nonexistent view should return None; got: {:?}",
            result
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    async fn connected_mssql(
        cfg: &arni_data::adapter::ConnectionConfig,
    ) -> arni_data::adapters::mssql::SqlServerAdapter {
        use arni_data::adapters::mssql::SqlServerAdapter;
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, cfg, password.as_deref())
            .await
            .expect("mssql connect should succeed");
        adapter
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BULK OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn test_mssql_bulk_insert_multi_row_returns_count() {
        use arni_data::adapter::QueryValue;

        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let table = "arni_ms_bulk_insert_count";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            ),
        )
        .await;
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {table} (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100), score INT)"
            ),
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

        let result =
            DbAdapter::execute_query(&adapter, &format!("SELECT COUNT(*) FROM {table}"))
                .await
                .unwrap();
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(3)),
            "expected 3 rows in table; got {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_mssql_bulk_insert_empty_rows_returns_zero() {
        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let table = "arni_ms_bulk_insert_empty";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            ),
        )
        .await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT IDENTITY(1,1) PRIMARY KEY, val INT)"),
        )
        .await
        .unwrap();

        let n = DbAdapter::bulk_insert(&adapter, table, &["val".to_string()], &[], None)
            .await
            .expect("empty bulk_insert should succeed");

        assert_eq!(n, 0, "empty rows should return 0");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_mssql_bulk_insert_column_count_mismatch_returns_err() {
        use arni_data::adapter::QueryValue;

        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let columns = vec!["a".to_string(), "b".to_string()]; // 2 columns
        let rows = vec![
            vec![QueryValue::Int(1)], // only 1 value — mismatch
        ];

        let result = DbAdapter::bulk_insert(&adapter, "any_table", &columns, &rows, None).await;
        assert!(result.is_err(), "column count mismatch should return Err");
    }

    #[tokio::test]
    async fn test_mssql_bulk_insert_null_value_round_trips() {
        use arni_data::adapter::QueryValue;

        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let table = "arni_ms_bulk_insert_null";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            ),
        )
        .await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT IDENTITY(1,1) PRIMARY KEY, note NVARCHAR(MAX))"),
        )
        .await
        .unwrap();

        let columns = vec!["note".to_string()];
        let rows = vec![vec![QueryValue::Null]];

        DbAdapter::bulk_insert(&adapter, table, &columns, &rows, None)
            .await
            .expect("inserting NULL should succeed");

        let result =
            DbAdapter::execute_query(&adapter, &format!("SELECT note FROM {table}")).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(
            matches!(result.rows[0][0], QueryValue::Null),
            "NULL value should round-trip as NULL; got {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_mssql_bulk_update_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};
        use std::collections::HashMap;

        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let table = "arni_ms_bulk_update";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            ),
        )
        .await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, status NVARCHAR(50))"),
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {table} VALUES (1, 'pending'), (2, 'pending'), (3, 'done')"
            ),
        )
        .await
        .unwrap();

        // Update only id = 1
        let mut set_clauses = HashMap::new();
        set_clauses.insert("status".to_string(), QueryValue::Text("active".to_string()));
        let filter = FilterExpr::Eq("id".to_string(), QueryValue::Int(1));

        let n = DbAdapter::bulk_update(&adapter, table, &[(set_clauses, filter)], None)
            .await
            .expect("bulk_update should succeed");

        assert_eq!(n, 1, "should update exactly 1 row");

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
    async fn test_mssql_bulk_delete_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};

        let cfg = mssql_config!();
        let adapter = connected_mssql(&cfg).await;

        let table = "arni_ms_bulk_delete";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            ),
        )
        .await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, tag NVARCHAR(10))"),
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
        let n = DbAdapter::bulk_delete(&adapter, table, &[filter], None)
            .await
            .expect("bulk_delete should succeed");

        assert_eq!(n, 2, "should delete 2 rows where tag='a'");

        let result =
            DbAdapter::execute_query(&adapter, &format!("SELECT COUNT(*) FROM {table}")).await.unwrap();
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(1)),
            "1 row should remain after deleting tag='a' rows"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }
}
