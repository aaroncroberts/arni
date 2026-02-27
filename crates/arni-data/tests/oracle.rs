//! Oracle adapter integration tests.
//!
//! These tests require a running Oracle database. Locally, start containers
//! with `arni dev start`. In CI, Oracle is **excluded** due to resource
//! requirements (~2 GB shared memory + ~60 s startup time).
//!
//! To run locally:
//! ```bash
//! arni dev start
//! export TEST_ORACLE_AVAILABLE=true
//! cargo test -p arni-data --features oracle --test oracle -- --include-ignored
//! ```

mod common;

#[cfg(feature = "oracle")]
mod oracle_tests {
    use super::common;
    use arni_data::adapter::{Connection as ConnectionTrait, DbAdapter};

    macro_rules! oracle_config {
        () => {{
            if common::skip_if_unavailable("oracle") {
                return;
            }
            match common::load_test_config("oracle-dev") {
                Some(cfg) => cfg,
                None => {
                    println!(
                        "[SKIP] oracle-dev profile not found in ~/.arni/connections.yml or env"
                    );
                    return;
                }
            }
        }};
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

    /// NOTE: Oracle container requires ~2 GB shared memory and ~60 s to start.
    /// Run locally with `arni dev start`; Oracle is excluded from CI.
    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_connect_and_disconnect() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("oracle connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("oracle disconnect should succeed");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_health_check_after_connect() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "oracle should be healthy after connect");
    }

    // ── Query execution ──────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_execute_select_1() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Oracle requires FROM DUAL for constant selects
        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS value FROM DUAL")
            .await
            .expect("SELECT 1 FROM DUAL should succeed");
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_create_table_insert_select_drop() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_TEST_ORACLE_BASIC";

        // Oracle: drop if exists via PL/SQL block
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = table
            ),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {t} \
                 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
                  label VARCHAR2(100))",
                t = table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // Oracle does not support multi-row VALUES; insert one row at a time
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('hello')", t = table),
        )
        .await
        .expect("INSERT hello should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('world')", t = table),
        )
        .await
        .expect("INSERT world should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, label FROM {t} ORDER BY id", t = table),
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(result.rows.len(), 2);

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Schema introspection ─────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_tables() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
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
