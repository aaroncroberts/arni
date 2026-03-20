//! Cloudflare R2 adapter integration tests.
//!
//! Requires real Cloudflare R2 credentials. See `docs/cloudflare-integration-test-setup.md`.
//!
//! ```bash
//! TEST_CLOUDFLARE_R2_AVAILABLE=true \
//!   CLOUDFLARE_ACCOUNT_ID=... \
//!   CLOUDFLARE_R2_ACCESS_KEY_ID=... \
//!   CLOUDFLARE_R2_SECRET_ACCESS_KEY=... \
//!   CLOUDFLARE_R2_BUCKET_NAME=arni-test \
//!   cargo test -p arni --features cloudflare-r2 --test cloudflare_r2
//! ```

mod common;

#[cfg(feature = "cloudflare-r2")]
mod r2_tests {
    use super::common;
    use arni::adapter::QueryValue;
    use arni::adapter::{Connection as ConnectionTrait, ConnectionConfig, DatabaseType, DbAdapter};
    use arni::adapters::cloudflare::r2::R2Adapter;
    use std::collections::HashMap;

    fn r2_config() -> Option<ConnectionConfig> {
        if common::skip_if_unavailable("cloudflare-r2") {
            return None;
        }
        let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID").ok()?;
        let access_key_id = std::env::var("CLOUDFLARE_R2_ACCESS_KEY_ID").ok()?;
        let secret_access_key = std::env::var("CLOUDFLARE_R2_SECRET_ACCESS_KEY").ok()?;
        let bucket_name = std::env::var("CLOUDFLARE_R2_BUCKET_NAME").ok()?;

        let mut parameters = HashMap::new();
        parameters.insert("account_id".to_string(), account_id);
        parameters.insert("r2_access_key_id".to_string(), access_key_id);
        parameters.insert("r2_secret_access_key".to_string(), secret_access_key);
        parameters.insert("bucket_name".to_string(), bucket_name);

        Some(ConnectionConfig {
            id: "test-r2".to_string(),
            name: "test-r2".to_string(),
            db_type: DatabaseType::CloudflareR2,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters,
            pool_config: None,
        })
    }

    async fn connected() -> Option<R2Adapter> {
        let cfg = r2_config()?;
        let mut adapter = R2Adapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("R2 connect should succeed");
        Some(adapter)
    }

    // ── Connection ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_r2_connect_and_disconnect() {
        let cfg = match r2_config() {
            Some(c) => c,
            None => return,
        };
        let mut adapter = R2Adapter::new(cfg);
        ConnectionTrait::connect(&mut adapter)
            .await
            .expect("connect should succeed");
        assert!(ConnectionTrait::is_connected(&adapter));
        ConnectionTrait::disconnect(&mut adapter)
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn test_r2_health_check() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed");
        assert!(healthy, "R2 should be healthy after connect");
    }

    // ── DSL: LIST / GET / DELETE ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_r2_put_get_delete_object() {
        use futures_util::StreamExt;

        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        // export_dataframe appends ".parquet" to the table_name to form the object key
        let table_name = "arni-test/put-get-delete";
        let key = format!("{table_name}.parquet");
        let body = b"hello from arni integration test";

        // Upload via export_dataframe is for DataFrames; use execute_query for DSL.
        // R2 DSL doesn't have a PUT command, so upload via the S3-backed put path
        // by calling execute_query on GET (should be empty first), then use
        // read_table (LIST) to verify, and DELETE to clean up.
        //
        // Actually the R2 adapter's execute_query only supports LIST/GET/DELETE.
        // We upload the test object via a raw AWS SDK call through the adapter's
        // internal client — but since we can't access that from here, we'll use
        // export_dataframe with a tiny Parquet to seed an object, then GET and DELETE.

        // Use a polars DataFrame to seed an object (requires polars feature)
        #[cfg(feature = "polars")]
        {
            use polars::prelude::*;
            let df = df! { "id" => [1i32], "msg" => ["arni-r2-test"] }.unwrap();
            let rows = DbAdapter::export_dataframe(&adapter, &df, table_name, None, true)
                .await
                .expect("export_dataframe (PUT via R2) should succeed");
            assert_eq!(rows, 1, "export_dataframe should report 1 row exported");

            // Verify it shows up in LIST
            let list_result = DbAdapter::execute_query(&adapter, "LIST arni-test/")
                .await
                .expect("LIST should succeed");
            let keys: Vec<String> = list_result
                .rows
                .iter()
                .filter_map(|r| {
                    if let Some(QueryValue::Text(k)) = r.first() {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(
                keys.iter().any(|k| k.contains("put-get-delete")),
                "LIST should include uploaded key; got {keys:?}"
            );

            // GET via streaming
            let mut stream = DbAdapter::execute_query_stream(&adapter, &format!("GET {key}"))
                .await
                .expect("execute_query_stream for GET should succeed");

            let mut total_bytes = 0usize;
            while let Some(row) = stream.next().await {
                let values = row.expect("stream chunk should not error");
                if let Some(QueryValue::Bytes(chunk)) = values.first() {
                    total_bytes += chunk.len();
                }
            }
            assert!(
                total_bytes > 0,
                "streaming GET should yield at least one byte chunk"
            );

            // DELETE
            DbAdapter::execute_query(&adapter, &format!("DELETE {key}"))
                .await
                .expect("DELETE should succeed");
        }

        #[cfg(not(feature = "polars"))]
        {
            // Without polars, just verify LIST on an empty prefix doesn't error
            let _ = body; // suppress unused warning
            let result = DbAdapter::execute_query(&adapter, "LIST arni-test/")
                .await
                .expect("LIST should succeed even on empty prefix");
            let _ = result;
        }
    }

    #[tokio::test]
    async fn test_r2_list_empty_prefix() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        // LIST on a prefix that doesn't exist should return empty, not error
        let result = DbAdapter::execute_query(&adapter, "LIST arni-test-nonexistent-prefix-xyz/")
            .await
            .expect("LIST on empty prefix should return Ok");
        assert!(
            result.rows.is_empty(),
            "LIST on nonexistent prefix should return 0 rows"
        );
    }

    #[tokio::test]
    async fn test_r2_read_table_returns_metadata() {
        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        // read_table returns (key, size, etag, last_modified) metadata rows.
        // Just verify it doesn't error on the bucket root.
        let result = DbAdapter::read_table(&adapter, "", None)
            .await
            .expect("read_table should succeed");
        // Columns: key, size, etag, last_modified
        if !result.rows.is_empty() {
            assert!(
                result.columns.contains(&"key".to_string()),
                "should have key column"
            );
            assert!(
                result.columns.contains(&"size".to_string()),
                "should have size column"
            );
        }
    }

    // ── Streaming GET ────────────────────────────────────────────────────────

    #[cfg(feature = "polars")]
    #[tokio::test]
    async fn test_r2_streaming_get_yields_bytes() {
        use futures_util::StreamExt;
        use polars::prelude::*;

        let adapter = match connected().await {
            Some(a) => a,
            None => return,
        };
        let table_name = "arni-test/stream-test";
        let key = format!("{table_name}.parquet");

        // Upload a small Parquet object
        let df = df! { "x" => [1i32, 2, 3] }.unwrap();
        DbAdapter::export_dataframe(&adapter, &df, table_name, None, true)
            .await
            .expect("export_dataframe should succeed");

        // Stream it back
        let mut stream = DbAdapter::execute_query_stream(&adapter, &format!("GET {key}"))
            .await
            .expect("execute_query_stream should succeed");

        let mut chunks = 0usize;
        let mut total = 0usize;
        while let Some(row) = stream.next().await {
            let values = row.expect("chunk should not error");
            if let Some(QueryValue::Bytes(b)) = values.first() {
                chunks += 1;
                total += b.len();
            }
        }
        assert!(chunks > 0, "should have received at least one chunk");
        assert!(total > 0, "total bytes should be > 0");

        // Clean up
        let _ = DbAdapter::execute_query(&adapter, &format!("DELETE {key}")).await;
    }
}
