//! Cloudflare KV adapter integration tests.
//!
//! Requires real Cloudflare credentials. See `docs/cloudflare-integration-test-setup.md`.
//!
//! ```bash
//! TEST_CLOUDFLARE_KV_AVAILABLE=true \
//!   CLOUDFLARE_ACCOUNT_ID=... \
//!   CLOUDFLARE_API_TOKEN=... \
//!   CLOUDFLARE_KV_NAMESPACE_ID=... \
//!   cargo test -p arni --features cloudflare-kv --test cloudflare_kv
//! ```

mod common;

#[cfg(feature = "cloudflare-kv")]
mod kv_tests {
    use super::common;
    use arni::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType, DbAdapter};
    use arni::adapters::cloudflare::kv::KVAdapter;
    use arni::adapter::QueryValue;
    use std::collections::HashMap;

    fn kv_config() -> Option<ConnectionConfig> {
        if common::skip_if_unavailable("cloudflare-kv") {
            return None;
        }
        let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID").ok()?;
        let api_token = std::env::var("CLOUDFLARE_API_TOKEN").ok()?;
        let namespace_id = std::env::var("CLOUDFLARE_KV_NAMESPACE_ID").ok()?;

        let mut parameters = HashMap::new();
        parameters.insert("account_id".to_string(), account_id);
        parameters.insert("api_token".to_string(), api_token);
        parameters.insert("namespace_id".to_string(), namespace_id);

        Some(ConnectionConfig {
            id: "test-kv".to_string(),
            name: "test-kv".to_string(),
            db_type: DatabaseType::CloudflareKV,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters,
            pool_config: None,
        })
    }

    async fn connected() -> Option<KVAdapter> {
        let cfg = kv_config()?;
        let mut adapter = KVAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("KV connect should succeed");
        Some(adapter)
    }

    // ── Connection ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_kv_connect_and_disconnect() {
        let cfg = match kv_config() {
            Some(c) => c,
            None => return,
        };
        let mut adapter = KVAdapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("connect should succeed");
        assert!(ConnectionTrait::is_connected(&adapter));
        ConnectionTrait::disconnect(&mut adapter)
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn test_kv_health_check() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "KV should be healthy after connect");
    }

    // ── DSL: PUT / GET / DELETE ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_kv_put_and_get() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let key = "arni-test-put-get";

        DbAdapter::execute_query(&adapter, &format!("PUT {key} hello-world"))
            .await
            .expect("PUT should succeed");

        let result = DbAdapter::execute_query(&adapter, &format!("GET {key}"))
            .await
            .expect("GET should succeed");

        assert_eq!(result.rows.len(), 1);
        // GET result columns are [key, value] — value is at index 1
        assert!(
            matches!(&result.rows[0][1], QueryValue::Text(v) if v == "hello-world"),
            "GET should return the stored value; got {:?}",
            result.rows[0][1]
        );

        // Clean up
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {key}")).await;
    }

    #[tokio::test]
    async fn test_kv_delete_key() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let key = "arni-test-delete";

        DbAdapter::execute_query(&adapter, &format!("PUT {key} to-be-deleted"))
            .await
            .expect("PUT should succeed");

        DbAdapter::execute_query(&adapter, &format!("DELETE {key}"))
            .await
            .expect("DELETE should succeed");

        // After delete, GET should return empty rows or an empty value (key not found).
        // Cloudflare KV returns 404 which the adapter surfaces as empty bytes → empty value.
        let result = DbAdapter::execute_query(&adapter, &format!("GET {key}")).await;
        match result {
            Err(_) => {} // 404 surfaced as error — acceptable
            Ok(r) => {
                // If it returned a row, the value (index 1) must be empty or null
                let value_empty = r.rows.is_empty()
                    || matches!(&r.rows[0][1], QueryValue::Null)
                    || matches!(&r.rows[0][1], QueryValue::Text(v) if v.is_empty());
                assert!(
                    value_empty,
                    "deleted key should have no value; got {:?}",
                    r.rows
                );
            }
        }
    }

    #[tokio::test]
    async fn test_kv_list() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let prefix = "arni-test-list";

        // Insert a couple of keys under the prefix
        DbAdapter::execute_query(&adapter, &format!("PUT {prefix}/a value-a"))
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, &format!("PUT {prefix}/b value-b"))
            .await
            .unwrap();

        let result = DbAdapter::execute_query(&adapter, &format!("LIST {prefix}"))
            .await
            .expect("LIST should succeed");

        assert!(
            result.rows.len() >= 2,
            "LIST should return at least 2 keys; got {:?}",
            result.rows.len()
        );

        // Clean up
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {prefix}/a")).await;
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {prefix}/b")).await;
    }

    // ── read_table ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_kv_read_table_prefix() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let prefix = "arni-test-rt";

        DbAdapter::execute_query(&adapter, &format!("PUT {prefix}/x val-x"))
            .await
            .unwrap();
        DbAdapter::execute_query(&adapter, &format!("PUT {prefix}/y val-y"))
            .await
            .unwrap();

        let result = DbAdapter::read_table(&adapter, prefix, None)
            .await
            .expect("read_table should succeed");

        assert!(
            result.rows.len() >= 2,
            "read_table should return at least 2 rows; got {}",
            result.rows.len()
        );
        // Columns should be key and value
        assert!(result.columns.contains(&"key".to_string()), "should have key column");
        assert!(result.columns.contains(&"value".to_string()), "should have value column");

        // Clean up
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {prefix}/x")).await;
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {prefix}/y")).await;
    }
}
