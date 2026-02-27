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
    use arni_data::adapter::{Connection as ConnectionTrait, DbAdapter};

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

    // ── Connection lifecycle ─────────────────────────────────────────────────

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

    // ── Query execution ──────────────────────────────────────────────────────

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
    async fn test_postgres_create_table_insert_select_drop() {
        use arni_data::adapters::postgres::PostgresAdapter;

        let cfg = pg_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = PostgresAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Use a unique table name to avoid conflicts
        let table = "arni_test_pg_basic";
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!("DROP TABLE IF EXISTS {}", table),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {} (id SERIAL PRIMARY KEY, label TEXT)", table),
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
        // list_tables returns a Vec; the test DB may or may not have tables
        let _ = tables;
    }

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
}
