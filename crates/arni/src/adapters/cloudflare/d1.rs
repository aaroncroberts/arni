//! Cloudflare D1 adapter — SQL-over-REST using the D1 `/raw` endpoint.
//!
//! # ConnectionConfig parameters
//!
//! | Key | Required | Description |
//! |---|---|---|
//! | `account_id` | Yes | Cloudflare account ID |
//! | `api_token` | Yes | API token with `D1:Read` / `D1:Write` scope |
//! | `database_id` | Yes | D1 database UUID |

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, QueryResult, QueryValue, RowStream, TableInfo,
};
use crate::DataError;

use super::http::CloudflareClient;

type Result<T> = std::result::Result<T, DataError>;

// ── D1 response types ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct D1RawResult {
    results: D1RawRows,
    #[allow(dead_code)]
    meta: Value,
}

#[derive(Deserialize)]
struct D1RawRows {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Serialize)]
struct D1QueryRequest<'a> {
    sql: &'a str,
    params: Vec<Value>,
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Cloudflare D1 adapter.
pub struct D1Adapter {
    config: ConnectionConfig,
    client: Option<CloudflareClient>,
}

impl D1Adapter {
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
                    "D1 adapter requires parameters['{key}'] in ConnectionConfig"
                ))
            })
    }

    fn client(&self) -> Result<&CloudflareClient> {
        self.client
            .as_ref()
            .ok_or_else(super::super::common::not_connected_error)
    }

    fn d1_raw_path(&self) -> Result<String> {
        let db_id = Self::get_param(&self.config, "database_id")?;
        let account_id = self.client()?.account_id();
        Ok(format!("/accounts/{account_id}/d1/database/{db_id}/raw"))
    }

    async fn execute_raw(&self, sql: &str, params: Vec<Value>) -> Result<QueryResult> {
        let path = self.d1_raw_path()?;
        let body = D1QueryRequest { sql, params };
        let mut results: Vec<D1RawResult> = self.client()?.cf_post(&path, &body).await?;

        let stmt = results
            .pop()
            .ok_or_else(|| DataError::Query("D1 returned empty result array".to_string()))?;

        let columns = stmt.results.columns;
        let rows = stmt
            .results
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(json_to_query_value).collect())
            .collect();

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: None,
        })
    }
}

// ── Connection trait ──────────────────────────────────────────────────────────

#[async_trait]
impl ConnectionTrait for D1Adapter {
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
        self.execute_raw("SELECT 1", vec![])
            .await
            .map(|_| true)
            .or(Ok(false))
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

// ── DbAdapter trait ───────────────────────────────────────────────────────────

#[async_trait]
impl DbAdapter for D1Adapter {
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
        let database_id = Self::get_param(config, "database_id")?;
        let client = CloudflareClient::new(api_token.to_string(), account_id.to_string())?;
        let path = format!("/accounts/{account_id}/d1/database/{database_id}/raw");
        let body = D1QueryRequest {
            sql: "SELECT 1",
            params: vec![],
        };
        client
            .cf_post::<_, Vec<D1RawResult>>(&path, &body)
            .await
            .map(|_| true)
            .or(Ok(false))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::CloudflareD1
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        self.execute_raw(query, vec![]).await
    }

    async fn execute_query_stream(&self, query: &str) -> Result<RowStream<Vec<QueryValue>>> {
        let result = self.execute_raw(query, vec![]).await?;
        let rows = result.rows;
        let stream = async_stream::stream! {
            for row in rows {
                yield Ok(row);
            }
        };
        Ok(Box::pin(stream))
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        // D1 databases are identified by their UUID in the config
        Ok(vec![self
            .config
            .parameters
            .get("database_id")
            .cloned()
            .unwrap_or_default()])
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>> {
        let result = self
            .execute_raw(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                vec![],
            )
            .await?;

        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(name)) = row.into_iter().next() {
                    Some(name)
                } else {
                    None
                }
            })
            .collect())
    }

    async fn describe_table(&self, table_name: &str, _schema: Option<&str>) -> Result<TableInfo> {
        let result = self
            .execute_raw(&format!("PRAGMA table_info(\"{table_name}\")"), vec![])
            .await?;

        let mut columns = Vec::new();
        for row in result.rows {
            if row.len() >= 6 {
                let name = match &row[1] {
                    QueryValue::Text(s) => s.clone(),
                    _ => continue,
                };
                let data_type = match &row[2] {
                    QueryValue::Text(s) => s.clone(),
                    _ => String::new(),
                };
                let nullable = match &row[3] {
                    QueryValue::Int(i) => *i == 0,
                    _ => true,
                };
                let is_primary_key = match &row[5] {
                    QueryValue::Int(i) => *i > 0,
                    _ => false,
                };
                columns.push(ColumnInfo {
                    name,
                    data_type,
                    nullable,
                    default_value: None,
                    is_primary_key,
                });
            }
        }

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: None,
            columns,
            row_count: None,
            size_bytes: None,
            created_at: None,
        })
    }

    #[cfg(feature = "polars")]
    async fn export_dataframe(
        &self,
        df: &polars::prelude::DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        replace: bool,
    ) -> Result<u64> {
        use crate::adapters::common::polars_dtype_to_generic_sql;
        let df = df.clone();

        let client = self.client()?;
        let path = self.d1_raw_path()?;

        if replace {
            let drop_sql = format!("DROP TABLE IF EXISTS \"{table_name}\"");
            client
                .cf_post::<_, Vec<serde_json::Value>>(
                    &path,
                    &D1QueryRequest {
                        sql: &drop_sql,
                        params: vec![],
                    },
                )
                .await?;
        }

        let schema = df.schema();
        let cols: Vec<String> = schema
            .iter()
            .map(|(name, dtype)| format!("\"{}\" {}", name, polars_dtype_to_generic_sql(dtype)))
            .collect();
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" ({})",
            cols.join(", ")
        );
        client
            .cf_post::<_, Vec<serde_json::Value>>(
                &path,
                &D1QueryRequest {
                    sql: &create_sql,
                    params: vec![],
                },
            )
            .await?;

        let height = df.height();
        if height == 0 {
            return Ok(0);
        }

        let col_names: Vec<String> = df
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let placeholders = col_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let insert_sql = format!(
            "INSERT INTO \"{table_name}\" ({}) VALUES ({})",
            col_names
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders
        );

        let mut rows_written = 0u64;
        for i in 0..height {
            let params: Vec<Value> = col_names
                .iter()
                .map(|col| {
                    df.column(col)
                        .ok()
                        .and_then(|s| s.get(i).ok())
                        .map(any_value_to_json)
                        .unwrap_or(Value::Null)
                })
                .collect();

            client
                .cf_post::<_, Vec<serde_json::Value>>(
                    &path,
                    &D1QueryRequest {
                        sql: &insert_sql,
                        params,
                    },
                )
                .await?;
            rows_written += 1;
        }

        Ok(rows_written)
    }
}

// ── Value conversion ──────────────────────────────────────────────────────────

fn json_to_query_value(v: Value) -> QueryValue {
    match v {
        Value::Null => QueryValue::Null,
        Value::Bool(b) => QueryValue::Bool(b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QueryValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                QueryValue::Float(f)
            } else {
                QueryValue::Text(n.to_string())
            }
        }
        Value::String(s) => QueryValue::Text(s),
        Value::Array(a) => QueryValue::Text(serde_json::to_string(&a).unwrap_or_default()),
        Value::Object(o) => QueryValue::Text(serde_json::to_string(&o).unwrap_or_default()),
    }
}

#[cfg(feature = "polars")]
fn any_value_to_json(v: polars::prelude::AnyValue) -> Value {
    use polars::prelude::AnyValue;
    match v {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(b),
        AnyValue::Int8(i) => Value::Number(i.into()),
        AnyValue::Int16(i) => Value::Number(i.into()),
        AnyValue::Int32(i) => Value::Number(i.into()),
        AnyValue::Int64(i) => Value::Number(i.into()),
        AnyValue::UInt8(i) => Value::Number(i.into()),
        AnyValue::UInt16(i) => Value::Number(i.into()),
        AnyValue::UInt32(i) => Value::Number(i.into()),
        AnyValue::UInt64(i) => Value::Number(i.into()),
        AnyValue::Float32(f) => serde_json::Number::from_f64(f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::Float64(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::StringOwned(s) => Value::String(s.to_string()),
        _ => Value::String(format!("{v}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_null() {
        assert_eq!(json_to_query_value(Value::Null), QueryValue::Null);
    }

    #[test]
    fn test_json_bool() {
        assert_eq!(
            json_to_query_value(Value::Bool(true)),
            QueryValue::Bool(true)
        );
    }

    #[test]
    fn test_json_int() {
        assert_eq!(
            json_to_query_value(serde_json::json!(42i64)),
            QueryValue::Int(42)
        );
    }

    #[test]
    fn test_json_float() {
        assert_eq!(
            json_to_query_value(serde_json::json!(1.5f64)),
            QueryValue::Float(1.5)
        );
    }

    #[test]
    fn test_json_string() {
        assert_eq!(
            json_to_query_value(Value::String("hello".into())),
            QueryValue::Text("hello".to_string())
        );
    }

    #[test]
    fn test_not_connected() {
        use std::collections::HashMap;
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::CloudflareD1,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let adapter = D1Adapter::new(config);
        assert!(!ConnectionTrait::is_connected(&adapter));
    }
}
