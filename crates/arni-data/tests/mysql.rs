//! MySQL adapter integration tests.
//!
//! These tests require a running MySQL instance. Locally, start containers
//! with `arni dev start`. In CI, the `integration-tests` job handles this.
//!
//! Set TEST_MYSQL_AVAILABLE=true to enable:
//! ```bash
//! export TEST_MYSQL_AVAILABLE=true
//! cargo test -p arni-data --features mysql --test mysql
//! ```

mod common;

#[cfg(feature = "mysql")]
mod mysql_tests {
    use super::common;
    use arni_data::adapter::{Connection as ConnectionTrait, DatabaseType, DbAdapter};

    macro_rules! mysql_config {
        () => {{
            if common::skip_if_unavailable("mysql") {
                return;
            }
            match common::load_test_config("mysql-dev") {
                Some(cfg) => cfg,
                None => {
                    println!(
                        "[SKIP] mysql-dev profile not found in ~/.arni/connections.yml or env"
                    );
                    return;
                }
            }
        }};
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_connect_and_disconnect() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("mysql connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("mysql disconnect should succeed");
    }

    #[tokio::test]
    async fn test_mysql_double_disconnect_is_noop() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("first disconnect should succeed");

        // Second disconnect should be a no-op, not an error
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("second disconnect should be a no-op");
    }

    // ── Health check ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_health_check_before_connect_returns_false_or_error() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let adapter = MySqlAdapter::new(cfg.clone());

        // Before connecting, health_check should either return Ok(false) or an Err.
        // Either is acceptable; the key property is it must not return Ok(true).
        match ConnectionTrait::health_check(&adapter).await {
            Ok(healthy) => assert!(
                !healthy,
                "health_check before connect must not return true"
            ),
            Err(_) => { /* expected – not connected */ }
        }
    }

    #[tokio::test]
    async fn test_mysql_health_check_after_connect() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "mysql should be healthy after connect");
    }

    // ── is_connected state ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_is_connected_false_before_connect() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let adapter = MySqlAdapter::new(cfg.clone());

        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "is_connected should be false before connect"
        );
    }

    #[tokio::test]
    async fn test_mysql_is_connected_true_after_connect() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        assert!(
            ConnectionTrait::is_connected(&adapter),
            "is_connected should be true after connect"
        );
    }

    #[tokio::test]
    async fn test_mysql_is_connected_false_after_disconnect() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();

        assert!(
            !ConnectionTrait::is_connected(&adapter),
            "is_connected should be false after disconnect"
        );
    }

    // ── database_type ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_database_type() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let adapter = MySqlAdapter::new(cfg.clone());

        assert_eq!(
            DbAdapter::database_type(&adapter),
            DatabaseType::MySQL,
            "database_type should return DatabaseType::MySQL"
        );
    }

    // ── Query execution ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_execute_select_1() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS value")
            .await
            .expect("SELECT 1 should succeed");
        assert_eq!(result.columns.len(), 1, "should return one column");
        assert_eq!(result.rows.len(), 1, "should return one row");
    }

    #[tokio::test]
    async fn test_mysql_execute_multi_column_select() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT 1 AS a, 'hello' AS b, 3.14 AS c",
        )
        .await
        .expect("multi-column SELECT should succeed");

        assert_eq!(result.columns.len(), 3, "should return three columns");
        assert_eq!(result.rows.len(), 1, "should return one row");
        assert!(result.columns.contains(&"a".to_string()));
        assert!(result.columns.contains(&"b".to_string()));
        assert!(result.columns.contains(&"c".to_string()));
    }

    #[tokio::test]
    async fn test_mysql_execute_select_null_value() {
        use arni_data::adapters::mysql::MySqlAdapter;
        use arni_data::adapter::QueryValue;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT NULL AS nullable_col")
            .await
            .expect("SELECT NULL should succeed");

        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0][0],
            QueryValue::Null,
            "NULL value should be represented as QueryValue::Null"
        );
    }

    #[tokio::test]
    async fn test_mysql_execute_empty_result() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "`arni_mysql_empty_result`";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!("DROP TABLE IF EXISTS {}", table),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (`id` INT AUTO_INCREMENT PRIMARY KEY, `val` VARCHAR(50))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT * FROM `arni_mysql_empty_result` WHERE 1=0",
        )
        .await
        .expect("SELECT returning no rows should succeed");

        assert_eq!(result.rows.len(), 0, "empty result should have 0 rows");

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_empty_result`",
        )
        .await;
    }

    // ── CRUD lifecycle ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_create_table_insert_select_drop() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "`arni_mysql_basic`";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!("DROP TABLE IF EXISTS {}", table),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (`id` INT AUTO_INCREMENT PRIMARY KEY, `label` VARCHAR(100))",
                table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_basic` (`label`) VALUES ('hello'), ('world')",
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT `id`, `label` FROM `arni_mysql_basic` ORDER BY `id`",
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(result.rows.len(), 2, "should return 2 rows");
        assert_eq!(result.columns.len(), 2, "should return 2 columns");

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_basic`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_update_rows() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_update`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_update` (`id` INT AUTO_INCREMENT PRIMARY KEY, `label` VARCHAR(100))",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_update` (`label`) VALUES ('original')",
        )
        .await
        .expect("INSERT should succeed");

        DbAdapter::execute_query(
            &adapter,
            "UPDATE `arni_mysql_update` SET `label` = 'updated' WHERE `label` = 'original'",
        )
        .await
        .expect("UPDATE should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT `label` FROM `arni_mysql_update`",
        )
        .await
        .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 1);

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_update`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_delete_rows() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_delete`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_delete` (`id` INT AUTO_INCREMENT PRIMARY KEY, `label` VARCHAR(100))",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_delete` (`label`) VALUES ('keep'), ('remove')",
        )
        .await
        .expect("INSERT should succeed");

        DbAdapter::execute_query(
            &adapter,
            "DELETE FROM `arni_mysql_delete` WHERE `label` = 'remove'",
        )
        .await
        .expect("DELETE should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT `label` FROM `arni_mysql_delete`",
        )
        .await
        .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 1, "only one row should remain after DELETE");

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_delete`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_rows_affected_on_insert() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_rows_affected`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_rows_affected` (`id` INT AUTO_INCREMENT PRIMARY KEY, `val` INT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_rows_affected` (`val`) VALUES (10), (20), (30)",
        )
        .await
        .expect("INSERT should succeed");

        if let Some(affected) = result.rows_affected {
            assert_eq!(affected, 3, "INSERT of 3 rows should report 3 rows affected");
        }

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_rows_affected`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Error handling ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_invalid_sql_returns_error() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT FROM WHERE").await;
        assert!(result.is_err(), "malformed SQL should return an error");
    }

    #[tokio::test]
    async fn test_mysql_query_nonexistent_table_returns_error() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT * FROM `arni_mysql_definitely_does_not_exist_xyzzy`",
        )
        .await;
        assert!(
            result.is_err(),
            "querying a nonexistent table should return an error"
        );
    }

    #[tokio::test]
    async fn test_mysql_query_before_connect_returns_error() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let adapter = MySqlAdapter::new(cfg.clone());

        // Attempt to execute a query without calling connect first
        let result = DbAdapter::execute_query(&adapter, "SELECT 1").await;
        assert!(
            result.is_err(),
            "executing a query before connect should return an error"
        );
    }

    // ── Metadata - list_tables ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_list_tables() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
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
    async fn test_mysql_list_tables_contains_created_table() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_list_tables_check`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_list_tables_check` (`id` INT AUTO_INCREMENT PRIMARY KEY)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");

        assert!(
            tables.iter().any(|t| t.eq_ignore_ascii_case("arni_mysql_list_tables_check")),
            "newly created table should appear in list_tables: {:?}",
            tables
        );

        DbAdapter::execute_query(
            &adapter,
            "DROP TABLE `arni_mysql_list_tables_check`",
        )
        .await
        .expect("DROP TABLE should succeed");
    }

    // ── Metadata - describe_table ────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_describe_table() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_describe`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_describe` (\
                `id` INT AUTO_INCREMENT PRIMARY KEY, \
                `name` VARCHAR(255) NOT NULL, \
                `score` FLOAT, \
                `active` TINYINT(1) \
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let table_info = DbAdapter::describe_table(&adapter, "arni_mysql_describe", None)
            .await
            .expect("describe_table should succeed");

        assert_eq!(
            table_info.name.to_lowercase(),
            "arni_mysql_describe",
            "table name should match"
        );

        let col_names: Vec<String> = table_info
            .columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();

        assert!(col_names.contains(&"id".to_string()), "should have 'id' column");
        assert!(col_names.contains(&"name".to_string()), "should have 'name' column");
        assert!(col_names.contains(&"score".to_string()), "should have 'score' column");
        assert!(col_names.contains(&"active".to_string()), "should have 'active' column");

        assert_eq!(
            table_info.columns.len(),
            4,
            "should describe exactly 4 columns"
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_describe`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_describe_table_nullable_flag() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_nullable`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_nullable` (\
                `required_col` INT NOT NULL, \
                `optional_col` INT \
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let table_info = DbAdapter::describe_table(&adapter, "arni_mysql_nullable", None)
            .await
            .expect("describe_table should succeed");

        let required = table_info
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case("required_col"))
            .expect("required_col should exist");
        let optional = table_info
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case("optional_col"))
            .expect("optional_col should exist");

        assert!(!required.nullable, "NOT NULL column should have nullable=false");
        assert!(optional.nullable, "nullable column should have nullable=true");

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_nullable`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Metadata - get_views ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_get_views() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP VIEW IF EXISTS `arni_mysql_test_view`",
        )
        .await;
        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_view_base`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_view_base` (`id` INT AUTO_INCREMENT PRIMARY KEY, `val` INT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "CREATE VIEW `arni_mysql_test_view` AS SELECT `id`, `val` FROM `arni_mysql_view_base`",
        )
        .await
        .expect("CREATE VIEW should succeed");

        let views = DbAdapter::get_views(&adapter, None)
            .await
            .expect("get_views should succeed");

        assert!(
            views.iter().any(|v| v.name.eq_ignore_ascii_case("arni_mysql_test_view")),
            "created view should appear in get_views: {:?}",
            views.iter().map(|v| &v.name).collect::<Vec<_>>()
        );

        DbAdapter::execute_query(&adapter, "DROP VIEW `arni_mysql_test_view`")
            .await
            .expect("DROP VIEW should succeed");
        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_view_base`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Metadata - get_indexes ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_get_indexes() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_indexes`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_indexes` (\
                `id` INT AUTO_INCREMENT PRIMARY KEY, \
                `email` VARCHAR(255) NOT NULL \
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "CREATE UNIQUE INDEX `idx_email` ON `arni_mysql_indexes` (`email`)",
        )
        .await
        .expect("CREATE INDEX should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, "arni_mysql_indexes", None)
            .await
            .expect("get_indexes should succeed");

        assert!(
            !indexes.is_empty(),
            "table with a PRIMARY KEY and an explicit index should have at least one index"
        );

        let idx_names: Vec<String> = indexes.iter().map(|i| i.name.to_lowercase()).collect();
        assert!(
            idx_names.iter().any(|n| n.contains("email") || n.contains("idx")),
            "explicit index on email should appear in get_indexes: {:?}",
            idx_names
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_indexes`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_get_indexes_primary_key_present() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_idx_pk`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_idx_pk` (`id` INT AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(100))",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, "arni_mysql_idx_pk", None)
            .await
            .expect("get_indexes should succeed");

        let has_primary = indexes.iter().any(|i| i.is_primary);
        assert!(
            has_primary,
            "table with AUTO_INCREMENT PRIMARY KEY should have a primary index: {:?}",
            indexes
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_idx_pk`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Metadata - get_foreign_keys ──────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_get_foreign_keys() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Disable FK checks temporarily so we can drop/recreate in any order.
        let _ = DbAdapter::execute_query(
            &adapter,
            "SET foreign_key_checks=0",
        )
        .await;

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_fk_child`",
        )
        .await;
        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_fk_parent`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_fk_parent` (\
                `id` INT AUTO_INCREMENT PRIMARY KEY, \
                `name` VARCHAR(100) NOT NULL \
            ) ENGINE=InnoDB",
        )
        .await
        .expect("CREATE parent TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_fk_child` (\
                `id` INT AUTO_INCREMENT PRIMARY KEY, \
                `parent_id` INT NOT NULL, \
                CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) \
                    REFERENCES `arni_mysql_fk_parent` (`id`) \
                    ON DELETE CASCADE \
            ) ENGINE=InnoDB",
        )
        .await
        .expect("CREATE child TABLE with FK should succeed");

        let _ = DbAdapter::execute_query(&adapter, "SET foreign_key_checks=1").await;

        let fks = DbAdapter::get_foreign_keys(&adapter, "arni_mysql_fk_child", None)
            .await
            .expect("get_foreign_keys should succeed");

        assert!(
            !fks.is_empty(),
            "child table should have at least one foreign key"
        );

        let fk = &fks[0];
        assert!(
            fk.referenced_table.eq_ignore_ascii_case("arni_mysql_fk_parent"),
            "FK should reference parent table, got: {}",
            fk.referenced_table
        );
        assert!(
            fk.columns.iter().any(|c| c.eq_ignore_ascii_case("parent_id")),
            "FK should include 'parent_id' column: {:?}",
            fk.columns
        );

        let _ = DbAdapter::execute_query(&adapter, "SET foreign_key_checks=0").await;
        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_fk_child`")
            .await
            .expect("DROP child TABLE should succeed");
        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_fk_parent`")
            .await
            .expect("DROP parent TABLE should succeed");
        let _ = DbAdapter::execute_query(&adapter, "SET foreign_key_checks=1").await;
    }

    #[tokio::test]
    async fn test_mysql_get_foreign_keys_empty_for_no_fk_table() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_no_fk`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_no_fk` (`id` INT AUTO_INCREMENT PRIMARY KEY)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, "arni_mysql_no_fk", None)
            .await
            .expect("get_foreign_keys should succeed even with no FKs");

        assert!(
            fks.is_empty(),
            "table with no FK constraints should return empty Vec"
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_no_fk`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Metadata - list_databases ────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_list_databases() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let databases = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        assert!(
            !databases.is_empty(),
            "MySQL server should have at least one database (e.g., information_schema)"
        );
    }

    #[tokio::test]
    async fn test_mysql_list_databases_contains_information_schema() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let databases = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        assert!(
            databases
                .iter()
                .any(|d| d.eq_ignore_ascii_case("information_schema")),
            "list_databases should include information_schema on any MySQL server: {:?}",
            databases
        );
    }

    // ── Metadata - list_stored_procedures ────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_list_stored_procedures() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Should succeed and return either an empty list or a list of procedures.
        let procedures = DbAdapter::list_stored_procedures(&adapter, None)
            .await
            .expect("list_stored_procedures should succeed (may return empty Vec)");

        // We only assert it didn't error; the result set may be empty.
        let _ = procedures;
    }

    // ── DataFrame operations ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_query_df_returns_correct_shape() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_df`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_df` (`id` INT AUTO_INCREMENT PRIMARY KEY, `val` INT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_df` (`val`) VALUES (10), (20), (30)",
        )
        .await
        .expect("INSERT should succeed");

        let df = DbAdapter::query_df(
            &adapter,
            "SELECT `id`, `val` FROM `arni_mysql_df` ORDER BY `id`",
        )
        .await
        .expect("query_df should succeed");

        assert_eq!(df.height(), 3, "DataFrame should have 3 rows");
        assert_eq!(df.width(), 2, "DataFrame should have 2 columns");

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_df`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_query_df_empty_result() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_df_empty`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_df_empty` (`id` INT AUTO_INCREMENT PRIMARY KEY, `val` INT)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        let df = DbAdapter::query_df(
            &adapter,
            "SELECT `id`, `val` FROM `arni_mysql_df_empty` WHERE 1=0",
        )
        .await
        .expect("query_df on empty result should succeed");

        assert_eq!(df.height(), 0, "empty DataFrame should have 0 rows");

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_df_empty`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Type handling ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_type_handling_int_varchar_float_bool() {
        use arni_data::adapters::mysql::MySqlAdapter;
        use arni_data::adapter::QueryValue;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_types`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_types` (\
                `int_col` INT, \
                `str_col` VARCHAR(100), \
                `float_col` FLOAT, \
                `bool_col` TINYINT(1) \
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_types` (`int_col`, `str_col`, `float_col`, `bool_col`) \
             VALUES (42, 'hello', 3.14, 1)",
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT `int_col`, `str_col`, `float_col`, `bool_col` FROM `arni_mysql_types`",
        )
        .await
        .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];

        // INT column: should be Int variant
        assert!(
            matches!(row[0], QueryValue::Int(_)),
            "INT column should map to QueryValue::Int, got: {:?}",
            row[0]
        );

        // VARCHAR column: should be Text variant
        assert!(
            matches!(row[1], QueryValue::Text(_)),
            "VARCHAR column should map to QueryValue::Text, got: {:?}",
            row[1]
        );

        // FLOAT column: should be Float or Int variant
        assert!(
            matches!(row[2], QueryValue::Float(_) | QueryValue::Int(_)),
            "FLOAT column should map to QueryValue::Float or ::Int, got: {:?}",
            row[2]
        );

        // TINYINT(1) - boolean: Int or Bool variant are both acceptable
        assert!(
            matches!(row[3], QueryValue::Int(_) | QueryValue::Bool(_)),
            "TINYINT(1) column should map to QueryValue::Int or ::Bool, got: {:?}",
            row[3]
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_types`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_type_handling_null_fields() {
        use arni_data::adapters::mysql::MySqlAdapter;
        use arni_data::adapter::QueryValue;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_null_fields`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_null_fields` (\
                `id` INT AUTO_INCREMENT PRIMARY KEY, \
                `nullable_int` INT, \
                `nullable_str` VARCHAR(100) \
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            "INSERT INTO `arni_mysql_null_fields` (`nullable_int`, `nullable_str`) VALUES (NULL, NULL)",
        )
        .await
        .expect("INSERT NULL values should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT `nullable_int`, `nullable_str` FROM `arni_mysql_null_fields`",
        )
        .await
        .expect("SELECT with NULL values should succeed");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];

        assert_eq!(
            row[0],
            QueryValue::Null,
            "nullable INT with NULL value should be QueryValue::Null"
        );
        assert_eq!(
            row[1],
            QueryValue::Null,
            "nullable VARCHAR with NULL value should be QueryValue::Null"
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_null_fields`")
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Schema filtering ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mysql_list_tables_with_explicit_schema() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let db_name = cfg.database.clone();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let _ = DbAdapter::execute_query(
            &adapter,
            "DROP TABLE IF EXISTS `arni_mysql_schema_filter`",
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            "CREATE TABLE `arni_mysql_schema_filter` (`id` INT AUTO_INCREMENT PRIMARY KEY)",
        )
        .await
        .expect("CREATE TABLE should succeed");

        // In MySQL, schema parameter = database name.
        let tables = DbAdapter::list_tables(&adapter, Some(db_name.as_str()))
            .await
            .expect("list_tables with explicit schema/database should succeed");

        assert!(
            tables
                .iter()
                .any(|t| t.eq_ignore_ascii_case("arni_mysql_schema_filter")),
            "table should appear when listing with explicit db name '{}': {:?}",
            db_name,
            tables
        );

        DbAdapter::execute_query(&adapter, "DROP TABLE `arni_mysql_schema_filter`")
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    async fn test_mysql_list_tables_with_wrong_schema_returns_empty_or_error() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Using a schema/database name that almost certainly doesn't exist.
        let result =
            DbAdapter::list_tables(&adapter, Some("arni_nonexistent_db_xyzzy_99999")).await;

        // Either an error or an empty list is acceptable behavior.
        match result {
            Ok(tables) => assert!(
                tables.is_empty(),
                "listing tables for a nonexistent database should return empty list"
            ),
            Err(_) => { /* also acceptable */ }
        }
    }
}
