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
    use arni_data::adapter::{Connection as ConnectionTrait, DbAdapter};

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

    // ── Connection lifecycle ─────────────────────────────────────────────────

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

    // ── Query execution ──────────────────────────────────────────────────────

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
    async fn test_mssql_create_table_insert_select_drop() {
        use arni_data::adapters::mssql::SqlServerAdapter;

        let cfg = mssql_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = SqlServerAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "arni_test_mssql_basic";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!("IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}", table, table),
        )
        .await;

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

    // ── Schema introspection ─────────────────────────────────────────────────

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
}
