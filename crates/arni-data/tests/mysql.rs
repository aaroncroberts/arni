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
    use arni_data::adapter::{Connection as ConnectionTrait, DbAdapter};

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
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_mysql_create_table_insert_select_drop() {
        use arni_data::adapters::mysql::MySqlAdapter;

        let cfg = mysql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MySqlAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_test_mysql_basic";
        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {}", table)).await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {} (id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(100))",
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

    // ── Schema introspection ─────────────────────────────────────────────────

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
}
