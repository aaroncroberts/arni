//! Cloudflare D1 adapter integration tests.
//!
//! Requires real Cloudflare credentials. See `docs/cloudflare-integration-test-setup.md`.
//!
//! Set the following environment variables and run:
//! ```bash
//! TEST_CLOUDFLARE_D1_AVAILABLE=true \
//!   CLOUDFLARE_ACCOUNT_ID=... \
//!   CLOUDFLARE_API_TOKEN=... \
//!   CLOUDFLARE_D1_DATABASE_ID=... \
//!   cargo test -p arni --features cloudflare-d1 --test cloudflare_d1
//! ```

mod common;

#[cfg(feature = "cloudflare-d1")]
mod d1_tests {
    use super::common;
    use arni::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType, DbAdapter};
    use arni::adapters::cloudflare::d1::D1Adapter;
    use std::collections::HashMap;

    fn d1_config() -> Option<ConnectionConfig> {
        if common::skip_if_unavailable("cloudflare-d1") {
            return None;
        }
        let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID").ok()?;
        let api_token = std::env::var("CLOUDFLARE_API_TOKEN").ok()?;
        let database_id = std::env::var("CLOUDFLARE_D1_DATABASE_ID").ok()?;

        let mut parameters = HashMap::new();
        parameters.insert("account_id".to_string(), account_id);
        parameters.insert("api_token".to_string(), api_token);
        parameters.insert("database_id".to_string(), database_id);

        Some(ConnectionConfig {
            id: "test-d1".to_string(),
            name: "test-d1".to_string(),
            db_type: DatabaseType::CloudflareD1,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters,
            pool_config: None,
        })
    }

    async fn connected() -> Option<D1Adapter> {
        let cfg = d1_config()?;
        let mut adapter = D1Adapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("D1 connect should succeed");
        Some(adapter)
    }

    // ── Connection ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_d1_connect_and_disconnect() {
        let cfg = match d1_config() {
            Some(c) => c,
            None => return,
        };
        let mut adapter = D1Adapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("connect should succeed");
        assert!(ConnectionTrait::is_connected(&adapter));
        ConnectionTrait::disconnect(&mut adapter)
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn test_d1_health_check() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "D1 should be healthy after connect");
    }

    // ── Query execution ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_d1_execute_select_1() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let result = DbAdapter::execute_query(&adapter, "SELECT 1 AS value")
            .await
            .expect("SELECT 1 should succeed");
        assert_eq!(result.columns, vec!["value"]);
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_d1_create_table_insert_query_drop() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let table = "arni_test_d1_basic";

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;

        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"),
        )
        .await
        .expect("CREATE TABLE should succeed");

        DbAdapter::execute_query(
            &adapter,
            &format!("INSERT INTO {table} VALUES (1, 'alpha'), (2, 'beta')"),
        )
        .await
        .expect("INSERT should succeed");

        let result = DbAdapter::execute_query(
            &adapter,
            &format!("SELECT id, label FROM {table} ORDER BY id"),
        )
        .await
        .expect("SELECT should succeed");

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns, vec!["id", "label"]);

        DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}"))
            .await
            .expect("DROP TABLE should succeed");
    }

    // ── Schema introspection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_d1_list_tables_includes_created_table() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let table = "arni_test_d1_list";

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(&adapter, &format!("CREATE TABLE {table} (x INTEGER)"))
            .await
            .unwrap();

        let tables = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed");
        assert!(
            tables.iter().any(|t| t == table),
            "list_tables should include {table}; got {tables:?}"
        );

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}")).await;
    }

    #[tokio::test]
    async fn test_d1_describe_table() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let table = "arni_test_d1_describe";

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
        DbAdapter::execute_query(
            &adapter,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
        )
        .await
        .unwrap();

        let info = DbAdapter::describe_table(&adapter, table, None)
            .await
            .expect("describe_table should succeed");
        assert_eq!(info.name, table);
        let col_names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"), "should include id");
        assert!(col_names.contains(&"name"), "should include name");

        let _ = DbAdapter::execute_query(&adapter, &format!("DROP TABLE {table}")).await;
    }

    // ── Bulk operations ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_d1_bulk_insert_returns_not_supported() {
        use arni::adapter::QueryValue;

        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let cols = vec!["id".to_string(), "label".to_string()];
        let rows = vec![vec![
            QueryValue::Int(1),
            QueryValue::Text("one".to_string()),
        ]];
        // D1 uses execute_query for DML; bulk_insert is not implemented.
        let result = DbAdapter::bulk_insert(&adapter, "any_table", &cols, &rows, None).await;
        assert!(result.is_err(), "D1 bulk_insert should return NotSupported");
    }
}
