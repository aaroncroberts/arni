//! Cloudflare Workers KV adapter — key-value DSL over the KV REST API.
//!
//! Since KV is not a SQL database, [`DbAdapter::execute_query`] accepts a
//! simple line-oriented DSL:
//!
//! | Command | Example | Effect |
//! |---|---|---|
//! | `GET <key>` | `GET user:123` | Read value for key |
//! | `PUT <key> <value>` | `PUT user:123 {"name":"alice"}` | Write value |
//! | `DELETE <key>` | `DELETE user:123` | Delete key |
//! | `LIST [prefix]` | `LIST user:` | List all keys (with optional prefix) |
//!
//! [`DbAdapter::read_table`] treats `table_name` as a key prefix and returns
//! all matching keys with their values as a two-column result (`key`, `value`).
//!
//! # ConnectionConfig parameters
//!
//! | Key | Required | Description |
//! |---|---|---|
//! | `account_id` | Yes | Cloudflare account ID |
//! | `api_token` | Yes | API token with `Workers KV Storage:Read/Write` scope |
//! | `namespace_id` | Yes | KV namespace UUID |

use async_trait::async_trait;

use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, QueryResult, QueryValue, RowStream, TableInfo,
};
use crate::DataError;

use super::http::{Bytes, CloudflareClient};

type Result<T> = std::result::Result<T, DataError>;

// ── DSL ───────────────────────────────────────────────────────────────────────

enum KvCommand {
    Get(String),
    Put(String, String),
    Delete(String),
    List(Option<String>),
}

fn parse_dsl(query: &str) -> Result<KvCommand> {
    let query = query.trim();
    let (cmd, rest) = query
        .split_once(char::is_whitespace)
        .map(|(c, r)| (c, r.trim()))
        .unwrap_or((query, ""));

    match cmd.to_uppercase().as_str() {
        "GET" => {
            if rest.is_empty() {
                return Err(DataError::Query("KV DSL: GET requires a key".to_string()));
            }
            Ok(KvCommand::Get(rest.to_string()))
        }
        "PUT" => {
            let (key, value) = rest
                .split_once(char::is_whitespace)
                .map(|(k, v)| (k.trim(), v.trim()))
                .ok_or_else(|| {
                    DataError::Query(
                        "KV DSL: PUT requires a key and value (PUT <key> <value>)".to_string(),
                    )
                })?;
            Ok(KvCommand::Put(key.to_string(), value.to_string()))
        }
        "DELETE" => {
            if rest.is_empty() {
                return Err(DataError::Query(
                    "KV DSL: DELETE requires a key".to_string(),
                ));
            }
            Ok(KvCommand::Delete(rest.to_string()))
        }
        "LIST" => Ok(KvCommand::List(if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        })),
        other => Err(DataError::NotSupported(format!(
            "KV DSL: unknown command '{other}'. Supported: GET, PUT, DELETE, LIST"
        ))),
    }
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Cloudflare Workers KV adapter.
pub struct KVAdapter {
    config: ConnectionConfig,
    client: Option<CloudflareClient>,
}

impl KVAdapter {
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    fn get_param<'a>(config: &'a ConnectionConfig, key: &str) -> Result<&'a str> {
        config
            .parameters
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| {
                DataError::Connection(format!(
                    "KV adapter requires parameters['{key}'] in ConnectionConfig"
                ))
            })
    }

    fn client(&self) -> Result<&CloudflareClient> {
        self.client
            .as_ref()
            .ok_or_else(super::super::common::not_connected_error)
    }

    fn ns_base(&self) -> Result<String> {
        let account_id = self.client()?.account_id();
        let ns_id = Self::get_param(&self.config, "namespace_id")?;
        Ok(format!(
            "/accounts/{account_id}/storage/kv/namespaces/{ns_id}"
        ))
    }

    async fn list_all_keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let base = self.ns_base()?;
        let client = self.client()?;
        let mut keys: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut path = format!("{base}/keys?limit=1000");
            if let Some(pfx) = prefix {
                path.push_str(&format!("&prefix={}", urlencoding_simple(pfx)));
            }
            if let Some(ref cur) = cursor {
                path.push_str(&format!("&cursor={}", urlencoding_simple(cur)));
            }

            // KV list returns {success, result:[{name}], result_info:{cursor, list_complete}}
            // We need the full envelope, not just result — use get_bytes for raw JSON
            let raw = client.get_bytes(&path).await?;
            let envelope: serde_json::Value = serde_json::from_slice(&raw)
                .map_err(|e| DataError::Query(format!("failed to parse KV list response: {e}")))?;

            if let Some(arr) = envelope["result"].as_array() {
                for item in arr {
                    if let Some(name) = item["name"].as_str() {
                        keys.push(name.to_string());
                    }
                }
            }

            let list_complete = envelope["result_info"]["list_complete"]
                .as_bool()
                .unwrap_or(true);

            if list_complete {
                break;
            }

            cursor = envelope["result_info"]["cursor"].as_str().map(String::from);

            // Guard: if no cursor returned but not complete, stop to avoid infinite loop
            if cursor.is_none() {
                break;
            }
        }

        Ok(keys)
    }
}

// ── Connection trait ──────────────────────────────────────────────────────────

#[async_trait]
impl ConnectionTrait for KVAdapter {
    async fn connect(&mut self) -> Result<()> {
        let api_token = Self::get_param(&self.config, "api_token")?.to_string();
        let account_id = Self::get_param(&self.config, "account_id")?.to_string();
        self.client = Some(CloudflareClient::new(api_token, account_id)?);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.client = None;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    async fn health_check(&self) -> Result<bool> {
        if self.client.is_none() {
            return Ok(false);
        }
        self.list_all_keys(None).await.map(|_| true).or(Ok(false))
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

// ── DbAdapter trait ───────────────────────────────────────────────────────────

#[async_trait]
impl DbAdapter for KVAdapter {
    async fn connect(&mut self, config: &ConnectionConfig, _password: Option<&str>) -> Result<()> {
        self.config = config.clone();
        ConnectionTrait::connect(self).await
    }

    async fn disconnect(&mut self) -> Result<()> {
        ConnectionTrait::disconnect(self).await
    }

    fn is_connected(&self) -> bool {
        ConnectionTrait::is_connected(self)
    }

    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        _password: Option<&str>,
    ) -> Result<bool> {
        let api_token = Self::get_param(config, "api_token")?;
        let account_id = Self::get_param(config, "account_id")?;
        let ns_id = Self::get_param(config, "namespace_id")?;
        let client = CloudflareClient::new(api_token.to_string(), account_id.to_string())?;
        let path = format!("/accounts/{account_id}/storage/kv/namespaces/{ns_id}/keys?limit=1");
        client.get_bytes(&path).await.map(|_| true).or(Ok(false))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::CloudflareKV
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let base = self.ns_base()?;
        let client = self.client()?;

        match parse_dsl(query)? {
            KvCommand::Get(key) => {
                let path = format!("{base}/values/{}", urlencoding_simple(&key));
                let bytes = client.get_bytes(&path).await?;
                let value = String::from_utf8_lossy(&bytes).to_string();
                Ok(QueryResult {
                    columns: vec!["key".to_string(), "value".to_string()],
                    rows: vec![vec![QueryValue::Text(key), QueryValue::Text(value)]],
                    rows_affected: Some(1),
                })
            }

            KvCommand::Put(key, value) => {
                let path = format!("{base}/values/{}", urlencoding_simple(&key));
                client
                    .put_bytes(&path, Bytes::from(value), "text/plain")
                    .await?;
                Ok(QueryResult {
                    columns: vec!["rows_written".to_string()],
                    rows: vec![vec![QueryValue::Int(1)]],
                    rows_affected: Some(1),
                })
            }

            KvCommand::Delete(key) => {
                let path = format!("{base}/values/{}", urlencoding_simple(&key));
                client.cf_delete(&path).await?;
                Ok(QueryResult {
                    columns: vec!["rows_written".to_string()],
                    rows: vec![vec![QueryValue::Int(1)]],
                    rows_affected: Some(1),
                })
            }

            KvCommand::List(prefix) => {
                let keys = self.list_all_keys(prefix.as_deref()).await?;
                let count = keys.len() as u64;
                let rows = keys
                    .into_iter()
                    .map(|k| vec![QueryValue::Text(k)])
                    .collect();
                Ok(QueryResult {
                    columns: vec!["key".to_string()],
                    rows,
                    rows_affected: Some(count),
                })
            }
        }
    }

    async fn execute_query_stream(&self, query: &str) -> Result<RowStream<Vec<QueryValue>>> {
        let result = self.execute_query(query).await?;
        let rows = result.rows;
        let stream = async_stream::stream! {
            for row in rows {
                yield Ok(row);
            }
        };
        Ok(Box::pin(stream))
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(vec![self
            .config
            .parameters
            .get("namespace_id")
            .cloned()
            .unwrap_or_default()])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>> {
        // KV has no table concept; list top-level key prefixes (keys before first '/')
        let keys = self.list_all_keys(None).await?;
        let mut prefixes: Vec<String> = keys
            .iter()
            .filter_map(|k| k.split('/').next().map(String::from))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        prefixes.sort();
        Ok(prefixes)
    }

    async fn describe_table(&self, _table_name: &str, _schema: Option<&str>) -> Result<TableInfo> {
        Ok(TableInfo {
            name: _table_name.to_string(),
            schema: None,
            columns: vec![
                ColumnInfo {
                    name: "key".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                ColumnInfo {
                    name: "value".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
            ],
            row_count: None,
            size_bytes: None,
            created_at: None,
        })
    }

    async fn read_table(&self, table_name: &str, _schema: Option<&str>) -> Result<QueryResult> {
        let base = self.ns_base()?;
        let client = self.client()?;
        let keys = self.list_all_keys(Some(table_name)).await?;

        let mut rows = Vec::new();
        for key in keys {
            let path = format!("{base}/values/{}", urlencoding_simple(&key));
            let value = match client.get_bytes(&path).await {
                Ok(b) => String::from_utf8_lossy(&b).to_string(),
                Err(_) => String::new(),
            };
            rows.push(vec![QueryValue::Text(key), QueryValue::Text(value)]);
        }

        Ok(QueryResult {
            columns: vec!["key".to_string(), "value".to_string()],
            rows,
            rows_affected: None,
        })
    }

    #[cfg(feature = "polars")]
    async fn export_dataframe(
        &self,
        df: polars::prelude::DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64> {
        use polars::prelude::*;

        let base = self.ns_base()?;
        let client = self.client()?;
        let height = df.height();
        let col_names = df.get_column_names();

        for i in 0..height {
            let mut obj = serde_json::Map::new();
            for col in &col_names {
                let val = df
                    .column(col)
                    .ok()
                    .and_then(|s| s.get(i).ok())
                    .map(|v| serde_json::Value::String(format!("{v}")))
                    .unwrap_or(serde_json::Value::Null);
                obj.insert(col.to_string(), val);
            }
            let key = format!("{table_name}/{i}");
            let json_bytes = serde_json::to_vec(&obj)
                .map_err(|e| DataError::Query(format!("failed to serialize row: {e}")))?;
            let path = format!("{base}/values/{}", urlencoding_simple(&key));
            client
                .put_bytes(&path, Bytes::from(json_bytes), "application/json")
                .await?;
        }

        Ok(height as u64)
    }
}

// ── URL encoding ──────────────────────────────────────────────────────────────

fn urlencoding_simple(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' | ':' | '/' => out.push(c),
            c => {
                for byte in c.to_string().as_bytes() {
                    out.push('%');
                    out.push_str(&format!("{byte:02X}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        match parse_dsl("GET user:123").unwrap() {
            KvCommand::Get(k) => assert_eq!(k, "user:123"),
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_get_case_insensitive() {
        match parse_dsl("get user:123").unwrap() {
            KvCommand::Get(k) => assert_eq!(k, "user:123"),
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_put() {
        match parse_dsl("PUT mykey hello world").unwrap() {
            KvCommand::Put(k, v) => {
                assert_eq!(k, "mykey");
                assert_eq!(v, "hello world");
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse_dsl("DELETE user:123").unwrap() {
            KvCommand::Delete(k) => assert_eq!(k, "user:123"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_list_no_prefix() {
        match parse_dsl("LIST").unwrap() {
            KvCommand::List(None) => {}
            _ => panic!("expected List(None)"),
        }
    }

    #[test]
    fn test_parse_list_with_prefix() {
        match parse_dsl("LIST user:").unwrap() {
            KvCommand::List(Some(p)) => assert_eq!(p, "user:"),
            _ => panic!("expected List(Some)"),
        }
    }

    #[test]
    fn test_parse_unknown() {
        assert!(matches!(
            parse_dsl("SCAN *"),
            Err(DataError::NotSupported(_))
        ));
    }

    #[test]
    fn test_parse_get_missing_key() {
        assert!(matches!(parse_dsl("GET"), Err(DataError::Query(_))));
    }

    #[test]
    fn test_urlencoding_plain() {
        assert_eq!(urlencoding_simple("hello"), "hello");
    }

    #[test]
    fn test_urlencoding_spaces() {
        assert_eq!(urlencoding_simple("hello world"), "hello%20world");
    }

    #[test]
    fn test_urlencoding_colon_slash() {
        assert_eq!(urlencoding_simple("user:123/data"), "user:123/data");
    }

    #[test]
    fn test_not_connected() {
        use std::collections::HashMap;
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::CloudflareKV,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let adapter = KVAdapter::new(config);
        assert!(!ConnectionTrait::is_connected(&adapter));
    }
}
