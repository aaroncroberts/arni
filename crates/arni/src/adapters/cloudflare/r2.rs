//! Cloudflare R2 adapter — object storage DSL using the S3-compatible API.
//!
//! R2 exposes an S3-compatible endpoint, so this adapter uses `aws-sdk-s3`
//! with a custom endpoint URL.
//!
//! Since R2 is not a SQL database, [`DbAdapter::execute_query`] accepts a
//! simple line-oriented DSL:
//!
//! | Command | Example | Effect |
//! |---|---|---|
//! | `LIST [prefix]` | `LIST uploads/` | List objects (with optional prefix) |
//! | `GET <key>` | `GET uploads/report.csv` | Download object bytes |
//! | `DELETE <key>` | `DELETE uploads/old.csv` | Delete object |
//! | `PUT <key>` | _(not supported)_ | Use `export_dataframe` for uploads |
//!
//! [`DbAdapter::read_table`] treats `table_name` as a key prefix and returns
//! a four-column result: `key`, `size`, `etag`, `last_modified`.
//!
//! [`DbAdapter::export_dataframe`] (requires `polars` feature) serializes the
//! DataFrame to Parquet and uploads it as `<table_name>.parquet`.
//!
//! # ConnectionConfig parameters
//!
//! | Key | Required | Description |
//! |---|---|---|
//! | `account_id` | Yes | Cloudflare account ID |
//! | `r2_access_key_id` | Yes | R2 access key ID (from R2 dashboard) |
//! | `r2_secret_access_key` | Yes | R2 secret access key |
//! | `bucket_name` | Yes | R2 bucket name |

use async_trait::async_trait;
use aws_config::Region;

use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Builder as S3ConfigBuilder, Client as S3Client};

use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, QueryResult, QueryValue, RowStream, TableInfo,
};
use crate::DataError;

type Result<T> = std::result::Result<T, DataError>;

// ── DSL ───────────────────────────────────────────────────────────────────────

enum R2Command {
    List(Option<String>),
    Get(String),
    Delete(String),
}

fn parse_dsl(query: &str) -> Result<R2Command> {
    let query = query.trim();
    let (cmd, rest) = query
        .split_once(char::is_whitespace)
        .map(|(c, r)| (c, r.trim()))
        .unwrap_or((query, ""));

    match cmd.to_uppercase().as_str() {
        "LIST" => Ok(R2Command::List(if rest.is_empty() {
            None
        } else {
            Some(rest.to_string())
        })),
        "GET" => {
            if rest.is_empty() {
                return Err(DataError::Query("R2 DSL: GET requires a key".to_string()));
            }
            Ok(R2Command::Get(rest.to_string()))
        }
        "DELETE" => {
            if rest.is_empty() {
                return Err(DataError::Query(
                    "R2 DSL: DELETE requires a key".to_string(),
                ));
            }
            Ok(R2Command::Delete(rest.to_string()))
        }
        "PUT" => Err(DataError::NotSupported(
            "R2 DSL: PUT via execute_query is not supported — use export_dataframe".to_string(),
        )),
        other => Err(DataError::NotSupported(format!(
            "R2 DSL: unknown command '{other}'. Supported: LIST, GET, DELETE"
        ))),
    }
}

// ── R2 client builder ─────────────────────────────────────────────────────────

/// Build an `aws-sdk-s3` client configured for Cloudflare R2.
///
/// **Critical:** `force_path_style(true)` is required — R2 requires path-style
/// addressing. Region is set to "auto" (R2 ignores it but the SDK requires it).
fn build_r2_client(account_id: &str, access_key_id: &str, secret_access_key: &str) -> S3Client {
    let creds = Credentials::new(
        access_key_id,
        secret_access_key,
        None, // session token — not used for R2
        None,
        "r2",
    );

    let config = S3ConfigBuilder::new()
        .endpoint_url(format!("https://{account_id}.r2.cloudflarestorage.com"))
        .credentials_provider(creds)
        .region(Region::new("auto"))
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();

    S3Client::from_conf(config)
}

// ── Adapter ───────────────────────────────────────────────────────────────────

/// Cloudflare R2 adapter.
pub struct R2Adapter {
    config: ConnectionConfig,
    client: Option<S3Client>,
}

impl R2Adapter {
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
                    "R2 adapter requires parameters['{key}'] in ConnectionConfig"
                ))
            })
    }

    fn client(&self) -> Result<&S3Client> {
        self.client
            .as_ref()
            .ok_or_else(super::super::common::not_connected_error)
    }

    fn bucket(&self) -> Result<&str> {
        Self::get_param(&self.config, "bucket_name")
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<QueryResult> {
        let client = self.client()?;
        let bucket = self.bucket()?;
        let mut rows: Vec<Vec<QueryValue>> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client.list_objects_v2().bucket(bucket).max_keys(1000);
            if let Some(pfx) = prefix {
                req = req.prefix(pfx);
            }
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| DataError::Query(format!("R2 ListObjectsV2 failed: {e}")))?;

            for obj in resp.contents() {
                let key = obj.key().unwrap_or_default().to_string();
                let size = obj.size().unwrap_or(0);
                let etag = obj
                    .e_tag()
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                let last_modified = obj
                    .last_modified()
                    .map(|t| t.to_string())
                    .unwrap_or_default();

                rows.push(vec![
                    QueryValue::Text(key),
                    QueryValue::Int(size),
                    QueryValue::Text(etag),
                    QueryValue::Text(last_modified),
                ]);
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        Ok(QueryResult {
            columns: vec![
                "key".to_string(),
                "size".to_string(),
                "etag".to_string(),
                "last_modified".to_string(),
            ],
            rows,
            rows_affected: None,
        })
    }
}

// ── Connection trait ──────────────────────────────────────────────────────────

#[async_trait]
impl ConnectionTrait for R2Adapter {
    async fn connect(&mut self) -> Result<()> {
        let account_id = Self::get_param(&self.config, "account_id")?;
        let access_key_id = Self::get_param(&self.config, "r2_access_key_id")?;
        let secret_access_key = Self::get_param(&self.config, "r2_secret_access_key")?;
        self.client = Some(build_r2_client(
            account_id,
            access_key_id,
            secret_access_key,
        ));
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
        let client = self.client()?;
        client
            .list_objects_v2()
            .bucket(self.bucket()?)
            .max_keys(1)
            .send()
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
impl DbAdapter for R2Adapter {
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
        let account_id = Self::get_param(config, "account_id")?;
        let access_key_id = Self::get_param(config, "r2_access_key_id")?;
        let secret_access_key = Self::get_param(config, "r2_secret_access_key")?;
        let bucket = Self::get_param(config, "bucket_name")?;
        let client = build_r2_client(account_id, access_key_id, secret_access_key);
        client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1)
            .send()
            .await
            .map(|_| true)
            .or(Ok(false))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::CloudflareR2
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let client = self.client()?;
        let bucket = self.bucket()?;

        match parse_dsl(query)? {
            R2Command::List(prefix) => self.list_objects(prefix.as_deref()).await,

            R2Command::Get(key) => {
                let resp = client
                    .get_object()
                    .bucket(bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| DataError::Query(format!("R2 GetObject failed: {e}")))?;

                let content_type = resp
                    .content_type()
                    .unwrap_or("application/octet-stream")
                    .to_string();
                let size = resp.content_length().unwrap_or(0);
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| DataError::Query(format!("R2 GetObject body read failed: {e}")))?
                    .into_bytes();

                Ok(QueryResult {
                    columns: vec![
                        "key".to_string(),
                        "content_type".to_string(),
                        "size".to_string(),
                        "body_bytes".to_string(),
                    ],
                    rows: vec![vec![
                        QueryValue::Text(key),
                        QueryValue::Text(content_type),
                        QueryValue::Int(size),
                        QueryValue::Bytes(body.to_vec()),
                    ]],
                    rows_affected: Some(1),
                })
            }

            R2Command::Delete(key) => {
                client
                    .delete_object()
                    .bucket(bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| DataError::Query(format!("R2 DeleteObject failed: {e}")))?;

                Ok(QueryResult {
                    columns: vec!["rows_written".to_string()],
                    rows: vec![vec![QueryValue::Int(1)]],
                    rows_affected: Some(1),
                })
            }
        }
    }

    async fn execute_query_stream(&self, query: &str) -> Result<RowStream<Vec<QueryValue>>> {
        // For GET, stream the body as chunks rather than materialising the whole object.
        // LIST and DELETE are already small/metadata-only; keep them on the materialized path.
        if let R2Command::Get(key) = parse_dsl(query)? {
            let client = self.client()?.clone();
            let bucket = self.bucket()?.to_string();
            let stream = async_stream::try_stream! {
                let resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| DataError::Query(format!("R2 GetObject failed: {e}")))?;

                let mut body = resp.body;
                while let Some(result) = body.next().await {
                    let chunk = result
                        .map_err(|e| DataError::Query(format!("R2 stream read failed: {e}")))?;
                    yield vec![QueryValue::Bytes(chunk.to_vec())];
                }
            };
            return Ok(Box::pin(stream));
        }

        // Non-GET: materialise and wrap (list / delete results are small).
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
            .get("bucket_name")
            .cloned()
            .unwrap_or_default()])
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        // Return top-level "directories" (key prefixes before first '/')
        let result = self.list_objects(schema).await?;
        let mut prefixes: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| {
                if let Some(QueryValue::Text(key)) = row.first() {
                    key.split('/').next().map(String::from)
                } else {
                    None
                }
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        prefixes.sort();
        Ok(prefixes)
    }

    async fn describe_table(&self, table_name: &str, _schema: Option<&str>) -> Result<TableInfo> {
        Ok(TableInfo {
            name: table_name.to_string(),
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
                    name: "size".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                ColumnInfo {
                    name: "etag".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                ColumnInfo {
                    name: "last_modified".to_string(),
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
        self.list_objects(Some(table_name)).await
    }

    #[cfg(feature = "polars")]
    async fn export_dataframe(
        &self,
        df: &polars::prelude::DataFrame,
        table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64> {
        use aws_sdk_s3::primitives::ByteStream;
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
        use polars::prelude::*;
        use std::io::Cursor;

        const MULTIPART_THRESHOLD: usize = 50 * 1024 * 1024; // 50 MB
        const PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB (S3 minimum part size)

        let mut df = df.clone();
        let mut buf = Cursor::new(Vec::new());
        ParquetWriter::new(&mut buf).finish(&mut df).map_err(|e| {
            DataError::Query(format!("failed to serialize DataFrame to Parquet: {e}"))
        })?;

        let parquet_bytes = buf.into_inner();
        let row_count = df.height() as u64;
        let key = format!("{table_name}.parquet");

        let client = self.client()?;
        let bucket = self.bucket()?;

        if parquet_bytes.len() < MULTIPART_THRESHOLD {
            // Small object — single PUT is simpler and has lower latency.
            client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .content_type("application/octet-stream")
                .body(ByteStream::from(parquet_bytes))
                .send()
                .await
                .map_err(|e| DataError::Query(format!("R2 PutObject failed: {e}")))?;
        } else {
            // Large object — use multipart upload (5 MB parts).
            let upload = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .content_type("application/octet-stream")
                .send()
                .await
                .map_err(|e| {
                    DataError::Query(format!("R2 CreateMultipartUpload failed: {e}"))
                })?;
            let upload_id = upload
                .upload_id()
                .ok_or_else(|| DataError::Query("R2 CreateMultipartUpload: no upload_id".into()))?
                .to_string();

            let mut completed_parts: Vec<CompletedPart> = Vec::new();
            let mut upload_err: Option<DataError> = None;

            for (i, chunk) in parquet_bytes.chunks(PART_SIZE).enumerate() {
                let part_number = (i + 1) as i32;
                match client
                    .upload_part()
                    .bucket(bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(chunk.to_vec()))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        completed_parts.push(
                            CompletedPart::builder()
                                .part_number(part_number)
                                .e_tag(resp.e_tag().unwrap_or_default())
                                .build(),
                        );
                    }
                    Err(e) => {
                        upload_err = Some(DataError::Query(format!(
                            "R2 UploadPart {part_number} failed: {e}"
                        )));
                        break;
                    }
                }
            }

            if let Some(err) = upload_err {
                // Best-effort abort — ignore abort errors, surface the upload error.
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                return Err(err);
            }

            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(&key)
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed_parts))
                        .build(),
                )
                .send()
                .await
                .map_err(|e| {
                    DataError::Query(format!("R2 CompleteMultipartUpload failed: {e}"))
                })?;
        }

        Ok(row_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_no_prefix() {
        match parse_dsl("LIST").unwrap() {
            R2Command::List(None) => {}
            _ => panic!("expected List(None)"),
        }
    }

    #[test]
    fn test_parse_list_with_prefix() {
        match parse_dsl("LIST uploads/").unwrap() {
            R2Command::List(Some(p)) => assert_eq!(p, "uploads/"),
            _ => panic!("expected List(Some)"),
        }
    }

    #[test]
    fn test_parse_get() {
        match parse_dsl("GET data/report.csv").unwrap() {
            R2Command::Get(k) => assert_eq!(k, "data/report.csv"),
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse_dsl("DELETE old/file.parquet").unwrap() {
            R2Command::Delete(k) => assert_eq!(k, "old/file.parquet"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_put_not_supported() {
        assert!(matches!(
            parse_dsl("PUT mykey value"),
            Err(DataError::NotSupported(_))
        ));
    }

    #[test]
    fn test_parse_unknown() {
        assert!(matches!(
            parse_dsl("COPY src dest"),
            Err(DataError::NotSupported(_))
        ));
    }

    #[test]
    fn test_not_connected() {
        use std::collections::HashMap;
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DatabaseType::CloudflareR2,
            host: None,
            port: None,
            database: String::new(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        };
        let adapter = R2Adapter::new(config);
        assert!(!ConnectionTrait::is_connected(&adapter));
    }
}
