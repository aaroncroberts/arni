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
                    println!("[SKIP] oracle-dev profile not found");
                    return;
                }
            }
        }};
    }

    // ── 1. Connection lifecycle ──────────────────────────────────────────────

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
    async fn test_oracle_disconnect_when_not_connected_is_ok() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let mut adapter = OracleAdapter::new(cfg);

        // Disconnect without ever connecting should not panic or return an error.
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("disconnect without prior connect should be a no-op");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_reconnect_after_disconnect() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("first connect should succeed");
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("first disconnect should succeed");

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("second connect should succeed");
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("second disconnect should succeed");
    }

    // ── 2. Health check ──────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_health_check_before_connect_returns_false() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let adapter = OracleAdapter::new(cfg);

        // health_check() before connect should return Ok(false), not panic.
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .unwrap_or(false);
        assert!(!healthy, "health_check before connect should be false");
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

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_health_check_after_disconnect_returns_false() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .unwrap_or(false);
        assert!(!healthy, "health_check after disconnect should be false");
    }

    // ── 3. is_connected state ────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_is_connected_state_transitions() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());

        assert!(
            !DbAdapter::is_connected(&adapter),
            "should not be connected before connect()"
        );

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        assert!(
            DbAdapter::is_connected(&adapter),
            "should be connected after connect()"
        );

        DbAdapter::disconnect(&mut adapter).await.unwrap();
        assert!(
            !DbAdapter::is_connected(&adapter),
            "should not be connected after disconnect()"
        );
    }

    // ── 4. database_type (no connection required) ────────────────────────────

    #[test]
    fn test_oracle_database_type() {
        use arni_data::adapter::DatabaseType;
        use arni_data::adapter::{ConnectionConfig, DbAdapter};
        use arni_data::adapters::oracle::OracleAdapter;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test-oracle".to_string(),
            name: "Test Oracle".to_string(),
            db_type: DatabaseType::Oracle,
            host: Some("localhost".to_string()),
            port: Some(1521),
            database: "FREE".to_string(),
            username: Some("system".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let adapter = OracleAdapter::new(config);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::Oracle);
    }

    // ── 5. Query execution ───────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_execute_select_1_from_dual() {
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
        assert_eq!(result.columns.len(), 1, "should have 1 column");
        assert_eq!(result.rows.len(), 1, "should have 1 row");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_execute_multi_column_select_from_dual() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result =
            DbAdapter::execute_query(&adapter, "SELECT 1 AS a, 'hello' AS b, 3.14 AS c FROM DUAL")
                .await
                .expect("multi-column SELECT FROM DUAL should succeed");
        assert_eq!(result.columns.len(), 3, "should have 3 columns");
        assert_eq!(result.rows.len(), 1, "should have 1 row");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_execute_null_from_dual() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "SELECT NULL AS nothing FROM DUAL")
            .await
            .expect("SELECT NULL FROM DUAL should succeed");
        assert_eq!(result.columns.len(), 1, "should have 1 column");
        assert_eq!(result.rows.len(), 1, "should have 1 row");

        use arni_data::adapter::QueryValue;
        assert_eq!(
            result.rows[0][0],
            QueryValue::Null,
            "NULL column should be QueryValue::Null"
        );
    }

    // ── 6. CRUD lifecycle ────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_crud_lifecycle() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_CRUD";

        // Drop-if-exists using Oracle PL/SQL block
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = table
            ),
        )
        .await;

        // CREATE TABLE with Oracle IDENTITY column (no AUTO_INCREMENT)
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

        // INSERT — Oracle requires separate INSERT statements, no multi-row VALUES
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('hello')", t = table),
        )
        .await
        .expect("INSERT first row should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('world')", t = table),
        )
        .await
        .expect("INSERT second row should succeed");

        // SELECT
        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, label FROM {t} ORDER BY id", t = table),
        )
        .await
        .expect("SELECT should succeed");
        assert_eq!(result.rows.len(), 2, "should have 2 rows after two inserts");
        assert_eq!(result.columns.len(), 2, "should have 2 columns: id, label");

        // UPDATE
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "UPDATE {t} SET label = 'updated' WHERE label = 'hello'",
                t = table
            ),
        )
        .await
        .expect("UPDATE should succeed");

        let updated = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT label FROM {t} WHERE label = 'updated'", t = table),
        )
        .await
        .expect("SELECT after UPDATE should succeed");
        assert_eq!(updated.rows.len(), 1, "should find exactly one updated row");

        // DELETE
        DbAdapter::execute_query(
            &adapter,
            &format!("DELETE FROM {t} WHERE label = 'world'", t = table),
        )
        .await
        .expect("DELETE should succeed");

        let after_delete = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT COUNT(*) AS cnt FROM {t}", t = table),
        )
        .await
        .expect("COUNT after DELETE should succeed");
        assert_eq!(after_delete.rows.len(), 1, "COUNT should return one row");

        // DROP
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── 7. Error handling ────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_invalid_sql_returns_error() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, "THIS IS NOT VALID SQL").await;
        assert!(result.is_err(), "invalid SQL should return an error");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_select_nonexistent_table_returns_error() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::execute_query(
            &adapter,
            "SELECT * FROM ARNI_ORA_TABLE_THAT_DOES_NOT_EXIST_AT_ALL",
        )
        .await;
        assert!(result.is_err(), "selecting non-existent table should error");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_execute_query_before_connect_returns_error() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let adapter = OracleAdapter::new(cfg);

        let result = DbAdapter::execute_query(&adapter, "SELECT 1 FROM DUAL").await;
        assert!(
            result.is_err(),
            "execute_query without connect should return an error"
        );
    }

    // ── 8. Metadata — list_databases (NotSupported) ──────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_databases_returns_not_supported() {
        use arni_data::adapters::oracle::OracleAdapter;
        use arni_data::DataError;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(
            result.is_err(),
            "list_databases should return an error for Oracle"
        );
        match result.unwrap_err() {
            DataError::NotSupported(_) => {}
            other => panic!("expected NotSupported error, got: {:?}", other),
        }
    }

    // ── 9. Metadata — list_tables ────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_tables_no_schema() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // With no schema, the adapter uses the connected user's schema
        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        // Oracle may or may not have tables for this user; simply verify no error.
        let _ = tables;
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_tables_with_schema() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        // Oracle schema = uppercase username; default test user is SYSTEM
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let tables = DbAdapter::list_tables(&adapter, Some(&schema))
            .await
            .expect("list_tables with schema should succeed");
        // Result is a Vec<String>; may be empty, but must not error.
        let _ = tables;
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_tables_contains_created_table() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_LISTTABLES";

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
            &format!("CREATE TABLE {t} (col1 NUMBER)", t = table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let tables = DbAdapter::list_tables(&adapter, Some(&schema))
            .await
            .expect("list_tables should succeed");
        assert!(
            tables.contains(&table.to_string()),
            "list_tables should include the newly created table; got: {:?}",
            tables
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE cleanup should succeed");
    }

    // ── 10. Metadata — describe_table ────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_describe_table_columns() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_DESCRIBE";

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
                  name VARCHAR2(200) NOT NULL, \
                  score NUMBER(10,2))",
                t = table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let info = DbAdapter::describe_table(&adapter, table, Some(&schema))
            .await
            .expect("describe_table should succeed");

        assert_eq!(
            info.name, table,
            "table name in TableInfo should match the queried table"
        );
        assert!(
            !info.columns.is_empty(),
            "describe_table should return at least one column"
        );

        // Verify column names are present (Oracle uppercases column names)
        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(
            col_names.contains(&"ID") || col_names.contains(&"id"),
            "expected ID column, got: {:?}",
            col_names
        );
        assert!(
            col_names.contains(&"NAME") || col_names.contains(&"name"),
            "expected NAME column, got: {:?}",
            col_names
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE cleanup should succeed");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_describe_nonexistent_table_returns_error() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result =
            DbAdapter::describe_table(&adapter, "ARNI_ORA_DOES_NOT_EXIST_XYZ", Some(&schema)).await;
        assert!(
            result.is_err(),
            "describe_table on non-existent table should return an error"
        );
    }

    // ── 11. Metadata — get_views ─────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_views_returns_vec() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let views = DbAdapter::get_views(&adapter, Some(&schema))
            .await
            .expect("get_views should succeed");
        // There may be zero or more views; result must be a Vec.
        let _ = views;
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_views_contains_created_view() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let base_table = "ARNI_ORA_VIEWBASE";
        let view_name = "ARNI_ORA_VIEW1";

        // Clean up prior runs
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP VIEW {v}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                v = view_name
            ),
        )
        .await;
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = base_table
            ),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {t} (x NUMBER)", t = base_table),
        )
        .await
        .expect("CREATE base TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE VIEW {v} AS SELECT x FROM {t}",
                v = view_name,
                t = base_table
            ),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let views = DbAdapter::get_views(&adapter, Some(&schema))
            .await
            .expect("get_views should succeed");
        let view_names: Vec<&str> = views.iter().map(|v| v.name.as_str()).collect();
        assert!(
            view_names.contains(&view_name),
            "get_views should include the newly created view; got: {:?}",
            view_names
        );

        // Cleanup
        DbAdapter::execute_query(&adapter, &format!("DROP VIEW {v}", v = view_name))
            .await
            .expect("DROP VIEW should succeed");
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = base_table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── 12. Metadata — get_indexes ───────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_indexes_returns_vec() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_INDEXES";

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
                  code VARCHAR2(50))",
                t = table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        // Create an explicit index on CODE
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE INDEX IDX_ARNI_ORA_CODE ON {t} (code)", t = table),
        )
        .await
        .expect("CREATE INDEX should succeed");

        let indexes = DbAdapter::get_indexes(&adapter, table, Some(&schema))
            .await
            .expect("get_indexes should succeed");
        assert!(
            !indexes.is_empty(),
            "should have at least one index (the primary key index)"
        );

        // Cleanup
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── 13. Metadata — get_foreign_keys ─────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_foreign_keys_returns_vec() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let parent = "ARNI_ORA_FK_PARENT";
        let child = "ARNI_ORA_FK_CHILD";

        // Cleanup
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = child
            ),
        )
        .await;
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = parent
            ),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {p} (id NUMBER PRIMARY KEY, name VARCHAR2(100))",
                p = parent
            ),
        )
        .await
        .expect("CREATE parent TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {c} \
                 (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
                  parent_id NUMBER, \
                  CONSTRAINT fk_arni_ora_parent FOREIGN KEY (parent_id) REFERENCES {p} (id))",
                c = child,
                p = parent
            ),
        )
        .await
        .expect("CREATE child TABLE with FK should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, child, Some(&schema))
            .await
            .expect("get_foreign_keys should succeed");
        assert!(!fks.is_empty(), "should return the defined foreign key");
        assert_eq!(fks[0].referenced_table, parent);

        // Cleanup (child first, then parent due to FK constraint)
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = child))
            .await
            .expect("DROP child TABLE should succeed");
        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = parent))
            .await
            .expect("DROP parent TABLE should succeed");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_foreign_keys_empty_for_no_fk_table() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_NOFK";

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
            &format!("CREATE TABLE {t} (id NUMBER PRIMARY KEY)", t = table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        let fks = DbAdapter::get_foreign_keys(&adapter, table, Some(&schema))
            .await
            .expect("get_foreign_keys should succeed for table with no FKs");
        assert!(
            fks.is_empty(),
            "table with no foreign keys should return empty Vec"
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── 14. Metadata — list_stored_procedures ────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_stored_procedures_returns_vec() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let procs = DbAdapter::list_stored_procedures(&adapter, Some(&schema))
            .await
            .expect("list_stored_procedures should succeed");
        // May be empty for a fresh schema; just verify it does not error.
        let _ = procs;
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_list_stored_procedures_language_is_plsql() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let schema = cfg.username.as_deref().unwrap_or("SYSTEM").to_uppercase();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Create a simple PL/SQL procedure so we have something to find
        let proc_name = "ARNI_ORA_PROC1";
        let _ =
            DbAdapter::execute_query(&adapter, &format!("DROP PROCEDURE {p}", p = proc_name)).await;

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE OR REPLACE PROCEDURE {p} AS BEGIN NULL; END;",
                p = proc_name
            ),
        )
        .await
        .expect("CREATE PROCEDURE should succeed");

        let procs = DbAdapter::list_stored_procedures(&adapter, Some(&schema))
            .await
            .expect("list_stored_procedures should succeed");

        assert!(
            !procs.is_empty(),
            "should find at least one procedure after creating one"
        );
        // All returned procedures should have PL/SQL as their language
        for proc in &procs {
            if let Some(lang) = &proc.language {
                assert_eq!(
                    lang, "PL/SQL",
                    "Oracle procedures should report PL/SQL language"
                );
            }
        }

        DbAdapter::execute_query(&adapter, &format!("DROP PROCEDURE {p}", p = proc_name))
            .await
            .expect("DROP PROCEDURE should succeed");
    }

    // ── 15. DataFrame operations ─────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_query_df_returns_dataframe() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let df = DbAdapter::query_df(&adapter, "SELECT 1 AS value FROM DUAL")
            .await
            .expect("query_df should succeed");

        assert_eq!(df.height(), 1, "DataFrame should have 1 row");
        assert_eq!(df.width(), 1, "DataFrame should have 1 column");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_query_df_multi_row() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_QUERYDF";

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

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('alpha')", t = table),
        )
        .await
        .expect("INSERT 1 should succeed");
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('beta')", t = table),
        )
        .await
        .expect("INSERT 2 should succeed");
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (label) VALUES ('gamma')", t = table),
        )
        .await
        .expect("INSERT 3 should succeed");

        let df = DbAdapter::query_df(
            &adapter,
            &format!("SELECT * FROM {t} ORDER BY id", t = table),
        )
        .await
        .expect("query_df should succeed for multi-row table");

        assert_eq!(df.height(), 3, "DataFrame should have 3 rows");
        assert!(df.width() >= 2, "DataFrame should have at least 2 columns");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── 16. Type handling ────────────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_number_type() {
        use arni_data::adapter::QueryValue;
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_NUMTYPE";

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
                 (int_col NUMBER(10), decimal_col NUMBER(10,2))",
                t = table
            ),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {t} (int_col, decimal_col) VALUES (42, 3.14)",
                t = table
            ),
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT int_col, decimal_col FROM {t}", t = table),
        )
        .await
        .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 1, "should have 1 row");
        assert_eq!(result.columns.len(), 2, "should have 2 columns");

        // The value should come back as a numeric type
        let int_val = &result.rows[0][0];
        assert!(
            matches!(
                int_val,
                QueryValue::Int(_) | QueryValue::Float(_) | QueryValue::Text(_)
            ),
            "NUMBER column should be a numeric or text QueryValue, got: {:?}",
            int_val
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_varchar2_type() {
        use arni_data::adapter::QueryValue;
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_VARCHARTYPE";

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
            &format!("CREATE TABLE {t} (txt VARCHAR2(200))", t = table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT INTO {t} (txt) VALUES ('Oracle VARCHAR2 test')",
                t = table
            ),
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT txt FROM {t}", t = table))
            .await
            .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 1);
        assert!(
            matches!(&result.rows[0][0], QueryValue::Text(s) if s == "Oracle VARCHAR2 test"),
            "VARCHAR2 should come back as QueryValue::Text, got: {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_date_type() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_DATETYPE";

        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t}'; \
                 EXCEPTION WHEN OTHERS THEN NULL; END;",
                t = table
            ),
        )
        .await;

        DbAdapter::execute_query(&adapter, &format!("CREATE TABLE {t} (dt DATE)", t = table))
            .await
            .expect("CREATE TABLE should succeed");

        // Use SYSDATE for Oracle date
        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {t} (dt) VALUES (SYSDATE)", t = table),
        )
        .await
        .expect("INSERT with SYSDATE should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT dt FROM {t}", t = table))
            .await
            .expect("SELECT DATE column should succeed");

        assert_eq!(result.rows.len(), 1, "should have 1 row");
        assert!(
            !result.columns.is_empty(),
            "should have at least one column"
        );
        // Date comes back as text or some value; just verify it is not an error
        let _ = &result.rows[0][0];

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {t}", t = table))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Metadata: get_server_info ────────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_server_info() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        assert!(
            !info.version.is_empty(),
            "server version should not be empty; got: {:?}",
            info.version
        );
        assert_eq!(
            info.server_type, "Oracle",
            "server_type should be 'Oracle'; got: {:?}",
            info.server_type
        );
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_server_info_version_contains_oracle() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let info = DbAdapter::get_server_info(&adapter)
            .await
            .expect("get_server_info should succeed");

        // v$version banner contains "Oracle Database" or "Oracle"
        let version_lower = info.version.to_lowercase();
        assert!(
            version_lower.contains("oracle"),
            "version string should contain 'Oracle'; got: {}",
            info.version
        );
    }

    // ── Metadata: get_view_definition ────────────────────────────────────────

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_view_definition() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let table = "ARNI_ORA_VDEF_BASE";
        let view = "ARNI_ORA_VDEF_VIEW";

        // Cleanup from any previous run (ignore errors)
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP VIEW {}'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                view
            ),
        )
        .await;
        let _ = DbAdapter::execute_query(
            &adapter,
            &format!(
                "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {}'; EXCEPTION WHEN OTHERS THEN NULL; END;",
                table
            ),
        )
        .await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {} (id NUMBER, val VARCHAR2(255))", table),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE VIEW {} AS SELECT id, val FROM {}", view, table),
        )
        .await
        .expect("CREATE VIEW should succeed");

        let def = DbAdapter::get_view_definition(&adapter, view, None)
            .await
            .expect("get_view_definition should succeed");

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP VIEW {}", view)).await;
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {}", table)).await;

        let def_str = def.expect("view definition should be Some");
        assert!(
            def_str.to_lowercase().contains("select"),
            "definition should contain SELECT; got: {}",
            def_str
        );
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_get_view_definition_nonexistent() {
        use arni_data::adapters::oracle::OracleAdapter;

        let cfg = oracle_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let result = DbAdapter::get_view_definition(&adapter, "ARNI_ORA_NO_SUCH_VIEW_XYZZY", None)
            .await
            .expect("get_view_definition for nonexistent view should return Ok");

        assert!(
            result.is_none(),
            "nonexistent view should return None; got: {:?}",
            result
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    async fn connected_oracle(
        cfg: &arni_data::adapter::ConnectionConfig,
    ) -> arni_data::adapters::oracle::OracleAdapter {
        use arni_data::adapters::oracle::OracleAdapter;
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = OracleAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, cfg, password.as_deref())
            .await
            .expect("oracle connect should succeed");
        adapter
    }

    // ═══════════════════════════════════════════════════════════════════════
    // BULK OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_insert_multi_row_returns_count() {
        use arni_data::adapter::QueryValue;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_BULK_INS";
        // Oracle: drop with PURGE to avoid recycle bin noise
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {table} (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
                 name VARCHAR2(100), score NUMBER)"
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

        let result = DbAdapter::execute_query(&adapter, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        assert!(
            matches!(result.rows[0][0], QueryValue::Int(3)),
            "expected 3 rows in table; got {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_insert_empty_rows_returns_zero() {
        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_BULK_EMPTY";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {table} (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val NUMBER)"
            ),
        )
        .await
        .unwrap();

        let n = DbAdapter::bulk_insert(&adapter, table, &["val".to_string()], &[], None)
            .await
            .expect("empty bulk_insert should succeed");

        assert_eq!(n, 0, "empty rows should return 0");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_insert_column_count_mismatch_returns_err() {
        use arni_data::adapter::QueryValue;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let columns = vec!["a".to_string(), "b".to_string()];
        let rows = vec![vec![QueryValue::Int(1)]]; // 1 value vs 2 columns

        let result = DbAdapter::bulk_insert(&adapter, "ANY_TABLE", &columns, &rows, None).await;
        assert!(result.is_err(), "column count mismatch should return Err");
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_insert_null_value_round_trips() {
        use arni_data::adapter::QueryValue;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_BULK_NULL";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "CREATE TABLE {table} (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note VARCHAR2(500))"
            ),
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
            "NULL value should round-trip as NULL; got {:?}",
            result.rows[0][0]
        );

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_update_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};
        use std::collections::HashMap;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_BULK_UPD";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY, status VARCHAR2(50))"),
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT ALL \
                 INTO {table} VALUES (1, 'pending') \
                 INTO {table} VALUES (2, 'pending') \
                 INTO {table} VALUES (3, 'done') \
                 SELECT 1 FROM DUAL"
            ),
        )
        .await
        .unwrap();

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

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_bulk_delete_matching_rows_only() {
        use arni_data::adapter::{FilterExpr, QueryValue};

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_BULK_DEL";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY, tag VARCHAR2(10))"),
        )
        .await
        .unwrap();
        DbAdapter::execute_query(
            &adapter,
            &format!(
                "INSERT ALL \
                 INTO {table} VALUES (1, 'a') \
                 INTO {table} VALUES (2, 'b') \
                 INTO {table} VALUES (3, 'a') \
                 SELECT 1 FROM DUAL"
            ),
        )
        .await
        .unwrap();

        let filter = FilterExpr::Eq("tag".to_string(), QueryValue::Text("a".to_string()));
        let n = DbAdapter::bulk_delete(&adapter, table, &[filter], None)
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

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DATAFRAME ROUND-TRIP TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_round_trip_schema_matches() {
        use polars::prelude::*;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_RT_SCHEMA";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;

        let original = df! {
            "ID"    => [1i64, 2, 3],
            "NAME"  => ["alice", "bob", "carol"],
            "SCORE" => [9.5f64, 8.0, 7.25],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, table, None, true)
            .await
            .expect("export should succeed");

        let read_back = DbAdapter::read_table(&adapter, table, None)
            .await
            .expect("read_table should succeed");

        // Oracle uppercases all identifiers
        let mut orig_cols: Vec<String> = original
            .get_column_names()
            .iter()
            .map(|s| s.to_uppercase())
            .collect();
        let mut back_cols: Vec<String> = read_back
            .get_column_names()
            .iter()
            .map(|s| s.to_uppercase())
            .collect();
        orig_cols.sort_unstable();
        back_cols.sort_unstable();
        assert_eq!(
            orig_cols, back_cols,
            "column names must match after round-trip"
        );
        assert_eq!(read_back.height(), 3, "row count must be preserved");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "Oracle requires 2 GB RAM + 60 s startup; run locally with arni dev start"]
    async fn test_oracle_round_trip_number_values_preserved() {
        use polars::prelude::*;

        let cfg = oracle_config!();
        let adapter = connected_oracle(&cfg).await;

        let table = "ARNI_ORA_RT_NUMBER";
        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE")).await;

        let original = df! {
            "ID"  => [10i64, 20, 30],
            "VAL" => [1.1f64, 2.2, 3.3],
        }
        .unwrap();

        DbAdapter::export_dataframe(&adapter, &original, table, None, true)
            .await
            .unwrap();

        let read_back = DbAdapter::read_table(&adapter, table, None).await.unwrap();

        let orig_ids: Vec<i64> = original
            .column("ID")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let back_ids: Vec<i64> = read_back
            .column("ID")
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(orig_ids, back_ids, "NUMBER column values must round-trip");

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table} PURGE"))
            .await
            .unwrap();
    }
}
