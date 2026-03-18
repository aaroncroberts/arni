//! [`ArniMcpServer`] — MCP server that exposes all [`arni::DbAdapter`] methods
//! as AI-callable tools via the rmcp 0.12 SDK.
//!
//! Each tool follows the same pattern:
//! 1. Extract the profile name from params.
//! 2. Obtain a [`arni::SharedAdapter`] from the [`arni::ConnectionRegistry`]
//!    (lazy-connects on first call, reuses on subsequent calls).
//! 3. Call the adapter method.
//! 4. Emit `tracing::info!` with tool name, profile, and duration.
//! 5. Return the result serialised as JSON.

use std::sync::Arc;
use std::time::Instant;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    Annotated, Content, Implementation, ListResourcesResult, PaginatedRequestParam,
    ProtocolVersion, RawResource, ReadResourceRequestParam, ReadResourceResult, ResourceContents,
    ServerCapabilities, ServerInfo,
};
use rmcp::serde_json::json;
use rmcp::service::RequestContext;
use rmcp::{tool, tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler};
use tracing::info;

use std::collections::HashMap;

use arni::{
    adapter::{FilterExpr, QueryValue, TableSearchMode},
    ArniConfig, ConnectionRegistry, SharedAdapter,
};

use crate::types::{
    BulkDeleteParams, BulkInsertParams, BulkUpdateParams, ExecuteParams, FindTablesParams,
    ProfileParams, QueryParams, SchemaParams, TableParams,
};

const ARNI_VERSION: &str = env!("CARGO_PKG_VERSION");

// ── Helper: parse filter JSON → FilterExpr ───────────────────────────────────

fn parse_filter(v: &serde_json::Value) -> Result<FilterExpr, String> {
    crate::filter::parse_filter_value(v).map_err(|e| e.to_string())
}

fn parse_query_value(v: &serde_json::Value) -> QueryValue {
    match v {
        serde_json::Value::Null => QueryValue::Null,
        serde_json::Value::Bool(b) => QueryValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                QueryValue::Int(i)
            } else {
                QueryValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => QueryValue::Text(s.clone()),
        serde_json::Value::Array(arr) => {
            let bytes: Vec<u8> = arr
                .iter()
                .filter_map(|x| x.as_u64().map(|b| b as u8))
                .collect();
            QueryValue::Bytes(bytes)
        }
        serde_json::Value::Object(_) => QueryValue::Text(v.to_string()),
    }
}

// ── ArniMcpServer ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ArniMcpServer {
    registry: Arc<ConnectionRegistry>,
    config: Arc<ArniConfig>,
    tool_router: rmcp::handler::server::tool::ToolRouter<Self>,
}

#[tool_router]
impl ArniMcpServer {
    pub fn new(registry: Arc<ConnectionRegistry>, config: Arc<ArniConfig>) -> Self {
        Self {
            registry,
            config,
            tool_router: Self::tool_router(),
        }
    }

    async fn adapter(&self, profile: &str) -> Result<SharedAdapter, String> {
        let config = Arc::clone(&self.config);
        let profile_owned = profile.to_string();
        self.registry
            .get_or_connect(profile, move || {
                let config = Arc::clone(&config);
                let p = profile_owned.clone();
                async move {
                    crate::db::connect_profile(&config, &p)
                        .await
                        .map_err(|e| arni::DataError::Config(e.to_string()))
                }
            })
            .await
            .map_err(|e| e.to_string())
    }

    // ── Core ─────────────────────────────────────────────────────────────────

    /// Execute a SQL SELECT and return all rows as a JSON array.
    #[tool(description = "Execute a SQL SELECT and return all rows as a JSON array of arrays. \
        Each element of `rows` corresponds to a column in `columns`. \
        Use `profile` to select the target database.")]
    pub async fn query(
        &self,
        Parameters(p): Parameters<QueryParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let result = adapter
            .execute_query(&p.sql)
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "query", profile = %p.profile, duration_ms, rows = result.rows.len());
        Content::json(json!({
            "columns": result.columns,
            "rows": result.rows.iter().map(|row| {
                row.iter().map(query_value_to_json).collect::<Vec<_>>()
            }).collect::<Vec<_>>(),
        }))
        .map_err(|e| e.to_string())
    }

    /// Execute a SQL statement (INSERT / UPDATE / DELETE / DDL) and return rows affected.
    #[tool(description = "Execute a SQL DML or DDL statement (INSERT, UPDATE, DELETE, CREATE, \
        DROP, etc.). Returns the number of rows affected. Use for writes and schema changes.")]
    pub async fn execute(
        &self,
        Parameters(p): Parameters<ExecuteParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let result = adapter
            .execute_query(&p.sql)
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "execute", profile = %p.profile, duration_ms);
        Content::json(json!({ "rows_affected": result.rows_affected }))
            .map_err(|e| e.to_string())
    }

    /// List all tables in the connected database.
    #[tool(description = "Return a list of all table names in the database. \
        Optionally filter by schema.")]
    pub async fn tables(
        &self,
        Parameters(p): Parameters<ProfileParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let tables = adapter
            .list_tables(None)
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "tables", profile = %p.profile, duration_ms, count = tables.len());
        Content::json(json!({ "tables": tables })).map_err(|e| e.to_string())
    }

    // ── Metadata ─────────────────────────────────────────────────────────────

    /// Return column definitions and row statistics for a table.
    #[tool(description = "Describe a table: returns column names, data types, nullability, \
        primary key flags, row count, and size. Essential before writing queries.")]
    pub async fn describe_table(
        &self,
        Parameters(p): Parameters<TableParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let info = adapter
            .describe_table(&p.table, p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "describe_table", profile = %p.profile, table = %p.table, duration_ms);
        Content::json(serde_json::to_value(&info).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// List all databases or schemas visible to the connected user.
    #[tool(description = "List all databases or schemas the current user can access.")]
    pub async fn list_databases(
        &self,
        Parameters(p): Parameters<ProfileParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let dbs = adapter.list_databases().await.map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "list_databases", profile = %p.profile, duration_ms);
        Content::json(json!({ "databases": dbs })).map_err(|e| e.to_string())
    }

    /// Return all indexes defined on a table.
    #[tool(description = "Return all indexes for a table, including whether each is unique \
        or a primary key index.")]
    pub async fn get_indexes(
        &self,
        Parameters(p): Parameters<TableParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let indexes = adapter
            .get_indexes(&p.table, p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "get_indexes", profile = %p.profile, table = %p.table, duration_ms);
        Content::json(serde_json::to_value(&indexes).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// Return all foreign keys defined on a table.
    #[tool(description = "Return all foreign key constraints on a table: which columns \
        reference which other tables and columns, and the ON DELETE / ON UPDATE rules.")]
    pub async fn get_foreign_keys(
        &self,
        Parameters(p): Parameters<TableParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let fks = adapter
            .get_foreign_keys(&p.table, p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "get_foreign_keys", profile = %p.profile, table = %p.table, duration_ms);
        Content::json(serde_json::to_value(&fks).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// List all views in a schema.
    #[tool(description = "List all views in the database along with their SQL definition.")]
    pub async fn get_views(
        &self,
        Parameters(p): Parameters<SchemaParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let views = adapter
            .get_views(p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "get_views", profile = %p.profile, duration_ms);
        Content::json(serde_json::to_value(&views).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// Return database server version and type.
    #[tool(description = "Return the database server type (e.g. PostgreSQL, MySQL, DuckDB) \
        and version string.")]
    pub async fn get_server_info(
        &self,
        Parameters(p): Parameters<ProfileParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let info = adapter.get_server_info().await.map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "get_server_info", profile = %p.profile, duration_ms);
        Content::json(serde_json::to_value(&info).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// List stored procedures and functions in a schema.
    #[tool(description = "List all stored procedures and functions visible in the given schema.")]
    pub async fn list_stored_procedures(
        &self,
        Parameters(p): Parameters<SchemaParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let procs = adapter
            .list_stored_procedures(p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "list_stored_procedures", profile = %p.profile, duration_ms);
        Content::json(serde_json::to_value(&procs).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// Search for tables whose names match a pattern.
    #[tool(description = "Search for tables by name pattern. `mode` is one of: \
        \"contains\" (default), \"starts\", \"ends\".")]
    pub async fn find_tables(
        &self,
        Parameters(p): Parameters<FindTablesParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let mode = match p.mode.as_deref() {
            Some("starts") => TableSearchMode::StartsWith,
            Some("ends") => TableSearchMode::EndsWith,
            _ => TableSearchMode::Contains,
        };
        let tables = adapter
            .find_tables(&p.pattern, p.schema.as_deref(), mode)
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "find_tables", profile = %p.profile, pattern = %p.pattern, duration_ms);
        Content::json(json!({ "tables": tables })).map_err(|e| e.to_string())
    }

    // ── Bulk operations ───────────────────────────────────────────────────────

    /// Insert multiple rows into a table in a single batched operation.
    #[tool(description = "Insert multiple rows into a table. `columns` lists the column names; \
        each entry in `rows` is an array of values in the same order. \
        Returns the number of rows inserted.")]
    pub async fn bulk_insert(
        &self,
        Parameters(p): Parameters<BulkInsertParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;

        let converted: Vec<Vec<QueryValue>> = p
            .rows
            .iter()
            .map(|row| row.iter().map(parse_query_value).collect())
            .collect();

        let rows_affected = adapter
            .bulk_insert(&p.table, &p.columns, &converted, p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "bulk_insert", profile = %p.profile, table = %p.table, rows_affected, duration_ms);
        Content::json(json!({ "rows_affected": rows_affected }))
            .map_err(|e| e.to_string())
    }

    /// Update rows matching a filter expression.
    #[tool(description = "Update rows matching a filter. `filter` uses the arni Filter DSL \
        (e.g. {\"id\": {\"eq\": 42}}). `values` is a flat object of column→newValue. \
        Returns rows affected.")]
    pub async fn bulk_update(
        &self,
        Parameters(p): Parameters<BulkUpdateParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let filter = parse_filter(&p.filter)?;

        let values_map = p
            .values
            .as_object()
            .ok_or_else(|| "values must be a JSON object".to_string())?;
        // Build one (column_values, row_filter) entry for the single update operation.
        let col_values: HashMap<String, QueryValue> = values_map
            .iter()
            .map(|(col, val)| (col.clone(), parse_query_value(val)))
            .collect();
        let updates = [(col_values, filter)];

        let rows_affected = adapter
            .bulk_update(&p.table, &updates, p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "bulk_update", profile = %p.profile, table = %p.table, rows_affected, duration_ms);
        Content::json(json!({ "rows_affected": rows_affected }))
            .map_err(|e| e.to_string())
    }

    /// Delete rows matching a filter expression.
    #[tool(description = "Delete rows matching a filter. `filter` uses the arni Filter DSL \
        (e.g. {\"id\": {\"in\": [1, 2, 3]}}). Returns rows affected.")]
    pub async fn bulk_delete(
        &self,
        Parameters(p): Parameters<BulkDeleteParams>,
    ) -> Result<rmcp::model::Content, String> {
        let t = Instant::now();
        let adapter = self.adapter(&p.profile).await?;
        let filter = parse_filter(&p.filter)?;
        let rows_affected = adapter
            .bulk_delete(&p.table, &[filter], p.schema.as_deref())
            .await
            .map_err(|e| e.to_string())?;
        let duration_ms = t.elapsed().as_millis();
        info!(tool = "bulk_delete", profile = %p.profile, table = %p.table, rows_affected, duration_ms);
        Content::json(json!({ "rows_affected": rows_affected }))
            .map_err(|e| e.to_string())
    }
}

// ── ServerHandler ─────────────────────────────────────────────────────────────

#[tool_handler]
impl ServerHandler for ArniMcpServer {
    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, McpError>> + Send + '_ {
        let names = self.registry.active_profiles();
        let resources = names
            .into_iter()
            .map(|name| {
                let mut raw = RawResource::new(format!("arni://profiles/{}", name), name.clone());
                raw.description = Some(format!("Live database connection: {}", name));
                raw.mime_type = Some("application/json".into());
                Annotated::new(raw, None)
            })
            .collect();
        std::future::ready(Ok(ListResourcesResult::with_all_items(resources)))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResult, McpError>> + Send + '_ {
        let uri = request.uri.clone();
        let result = match uri.strip_prefix("arni://profiles/") {
            None => Err(McpError::invalid_params("Unrecognised resource URI", None)),
            Some(name) => {
                let active = self.registry.active_profiles();
                if active.contains(&name.to_string()) {
                    let text = json!({ "profile": name, "status": "connected" }).to_string();
                    Ok(ReadResourceResult {
                        contents: vec![ResourceContents::TextResourceContents {
                            uri: uri.clone(),
                            mime_type: Some("application/json".into()),
                            text,
                            meta: None,
                        }],
                    })
                } else {
                    Err(McpError::invalid_params(
                        format!("Profile '{}' not found or not yet connected", name),
                        None,
                    ))
                }
            }
        };
        std::future::ready(result)
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            server_info: Implementation {
                name: "arni".into(),
                title: None,
                version: ARNI_VERSION.into(),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "arni MCP server — query and manage any configured database profile. \
                 Use `tables` to discover schema, `describe_table` for column details, \
                 `query` for SELECT, `execute` for writes, and bulk_* for batch operations."
                    .into(),
            ),
        }
    }
}

// ── QueryValue → serde_json::Value ───────────────────────────────────────────

fn query_value_to_json(v: &QueryValue) -> serde_json::Value {
    match v {
        QueryValue::Null => serde_json::Value::Null,
        QueryValue::Bool(b) => json!(b),
        QueryValue::Int(i) => json!(i),
        QueryValue::Float(f) => json!(f),
        QueryValue::Text(s) => json!(s),
        QueryValue::Bytes(b) => {
            json!(b.iter().map(|x| *x as i64).collect::<Vec<_>>())
        }
    }
}
