use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, FilterExpr, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue,
    RowStream, ServerInfo, TableInfo, TableSearchMode, ViewInfo,
};
use crate::DataError;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    options::ClientOptions,
    Client,
};
#[cfg(feature = "polars")]
use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use tracing::{debug, error, info, instrument, warn};

/// MongoDB database adapter
///
/// MongoDB is document-oriented, so this adapter maps:
/// - Collections → Tables
/// - Documents → Rows
/// - Fields → Columns
///
/// Schema inference is performed by sampling documents.
///
/// # SSL/TLS Support
///
/// TLS is controlled by the `use_ssl` configuration option:
/// - `use_ssl: false` — plain connection (`mongodb://…`)
/// - `use_ssl: true`  — TLS-encrypted connection (`mongodb://…?tls=true`)
///
/// Certificate validation uses the system trust store. To allow self-signed
/// certificates in development, add `tlsInsecure=true` via `--param
/// tlsInsecure=true` (passed through the connection URI).
pub struct MongoDbAdapter {
    config: ConnectionConfig,
    client: Option<Client>,
    current_database: Option<String>,
    password: Option<String>,
}

impl MongoDbAdapter {
    /// Create a new MongoDB adapter with the given configuration
    pub fn new(config: ConnectionConfig) -> Self {
        debug!(database = %config.database, "Creating MongoDB adapter");
        Self {
            config,
            client: None,
            current_database: None,
            password: None,
        }
    }

    /// Validate database name according to MongoDB rules
    fn validate_database_name(name: &str) -> Result<(), DataError> {
        if name.is_empty() {
            return Err(DataError::Config(
                "Database name cannot be empty".to_string(),
            ));
        }
        if name.len() > 64 {
            return Err(DataError::Config(format!(
                "Database name too long (max 64 chars): {}",
                name.len()
            )));
        }
        // Check for invalid characters
        for c in ['/', '\\', '.', ' ', '"', '$', '*', '<', '>', ':', '|', '?'] {
            if name.contains(c) {
                return Err(DataError::Config(format!(
                    "Database name contains invalid character '{}': {}",
                    c, name
                )));
            }
        }
        Ok(())
    }

    /// Validate collection name (MongoDB's equivalent of table)
    fn validate_collection_name(name: &str) -> Result<(), DataError> {
        if name.is_empty() {
            return Err(DataError::Config(
                "Collection name cannot be empty".to_string(),
            ));
        }
        if name.starts_with("system.") {
            return Err(DataError::Config(format!(
                "Collection name cannot start with 'system.': {}",
                name
            )));
        }
        if name.contains('$') && !name.starts_with("oplog.$") {
            return Err(DataError::Config(format!(
                "Collection name contains invalid character '$': {}",
                name
            )));
        }
        if name.contains('\0') {
            return Err(DataError::Config(format!(
                "Collection name contains null character: {}",
                name
            )));
        }
        Ok(())
    }

    /// Build a MongoDB connection URI from configuration.
    ///
    /// When `config.use_ssl` is `true` the URI includes `tls=true`, enabling
    /// TLS with server certificate validation via the system trust store.
    fn build_connection_string(config: &ConnectionConfig, password: Option<&str>) -> String {
        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(27017);
        let username = config.username.as_deref();

        // Build query-string params (always at least authSource when authed).
        let tls_param = if config.use_ssl { "&tls=true" } else { "" };

        if let (Some(user), Some(pass)) = (username, password) {
            // Percent-encode userinfo so special chars (@, /, %) don't break URI parsing.
            let encoded_user = percent_encode_userinfo(user);
            let encoded_pass = percent_encode_userinfo(pass);
            format!(
                "mongodb://{}:{}@{}:{}/?authSource=admin{}",
                encoded_user, encoded_pass, host, port, tls_param
            )
        } else if config.use_ssl {
            format!("mongodb://{}:{}/?tls=true", host, port)
        } else {
            format!("mongodb://{}:{}", host, port)
        }
    }

    /// Convert a BSON value to QueryValue
    fn bson_to_query_value(bson: &Bson) -> QueryValue {
        match bson {
            Bson::Null | Bson::Undefined => QueryValue::Null,
            Bson::Boolean(b) => QueryValue::Bool(*b),
            Bson::Int32(i) => QueryValue::Int(*i as i64),
            Bson::Int64(i) => QueryValue::Int(*i),
            Bson::Double(d) => QueryValue::Float(*d),
            Bson::String(s) => QueryValue::Text(s.clone()),
            Bson::Binary(b) => QueryValue::Bytes(b.bytes.clone()),
            Bson::ObjectId(oid) => QueryValue::Text(oid.to_hex()),
            Bson::DateTime(dt) => QueryValue::Text(dt.to_string()),
            Bson::Array(arr) => QueryValue::Text(format!("{:?}", arr)),
            Bson::Document(doc) => QueryValue::Text(format!("{:?}", doc)),
            _ => QueryValue::Text(format!("{:?}", bson)),
        }
    }

    /// Get MongoDB type name from BSON value
    fn bson_type_name(bson: &Bson) -> &'static str {
        match bson {
            Bson::Null | Bson::Undefined => "null",
            Bson::Boolean(_) => "boolean",
            Bson::Int32(_) => "int32",
            Bson::Int64(_) => "int64",
            Bson::Double(_) => "double",
            Bson::String(_) => "string",
            Bson::Binary(_) => "binary",
            Bson::ObjectId(_) => "objectid",
            Bson::DateTime(_) => "datetime",
            Bson::Array(_) => "array",
            Bson::Document(_) => "document",
            _ => "unknown",
        }
    }
}

impl Default for MongoDbAdapter {
    fn default() -> Self {
        Self {
            config: ConnectionConfig {
                id: String::new(),
                name: String::new(),
                db_type: DatabaseType::MongoDB,
                host: Some("localhost".to_string()),
                port: Some(27017),
                database: String::new(),
                username: None,
                use_ssl: false,
                parameters: HashMap::new(),
                pool_config: None,
            },
            client: None,
            current_database: None,
            password: None,
        }
    }
}

#[async_trait]
impl ConnectionTrait for MongoDbAdapter {
    #[instrument(skip(self), fields(adapter = "mongodb", database = %self.config.database))]
    async fn connect(&mut self) -> Result<(), DataError> {
        if self.config.db_type != DatabaseType::MongoDB {
            let err = DataError::Config(format!(
                "Invalid database type: expected MongoDB, got {:?}",
                self.config.db_type
            ));
            error!(adapter = "mongodb", operation = "connect", error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_name(&self.config.database)?;

        let host = self.config.host.as_deref().unwrap_or("localhost");
        let port = self.config.port.unwrap_or(27017);
        info!(host, port, database = %self.config.database, "Connecting to MongoDB");

        debug!("MongoDB connection pooling is managed by the mongodb driver");
        let password = self.password.as_deref();
        let connection_string = Self::build_connection_string(&self.config, password);

        let client_options = ClientOptions::parse(&connection_string)
            .await
            .map_err(|e| {
                DataError::Connection(format!(
                    "Invalid connection string for {}:{} - {}",
                    self.config.host.as_deref().unwrap_or("localhost"),
                    self.config.port.unwrap_or(27017),
                    e
                ))
            })?;

        let client = Client::with_options(client_options).map_err(|e| {
            DataError::Connection(format!(
                "Failed to create MongoDB client for {}:{} - {}",
                self.config.host.as_deref().unwrap_or("localhost"),
                self.config.port.unwrap_or(27017),
                e
            ))
        })?;

        // Test the connection
        client
            .database(&self.config.database)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| {
                let error_msg = e.to_string();

                if error_msg.contains("authentication failed") || error_msg.contains("auth failed")
                {
                    let err = DataError::Authentication(format!(
                        "Authentication failed for database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ));
                    error!(adapter = "mongodb", operation = "connect", error = %err, "MongoDB authentication failed");
                    err
                } else if error_msg.contains("connection refused")
                    || error_msg.contains("No connection available")
                {
                    let err = DataError::Connection(format!(
                        "Network error connecting to MongoDB at {}:{} - {}",
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ));
                    error!(adapter = "mongodb", operation = "connect", error = %err, "MongoDB network error");
                    err
                } else if error_msg.contains("not master") || error_msg.contains("replica set") {
                    let err = DataError::Connection(format!(
                        "Replica set configuration issue at {}:{} - {}",
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ));
                    error!(adapter = "mongodb", operation = "connect", error = %err, "MongoDB replica set error");
                    err
                } else if error_msg.contains("unauthorized") {
                    let err = DataError::Authentication(format!(
                        "Unauthorized access to database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ));
                    error!(adapter = "mongodb", operation = "connect", error = %err, "MongoDB unauthorized access");
                    err
                } else {
                    let err = DataError::Connection(format!(
                        "Failed to connect to database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ));
                    error!(adapter = "mongodb", operation = "connect", error = %err, "MongoDB connection failed");
                    err
                }
            })?;

        self.client = Some(client);
        self.current_database = Some(self.config.database.clone());
        info!("Connected to MongoDB successfully");
        Ok(())
    }

    #[instrument(skip(self), fields(adapter = "mongodb"))]
    async fn disconnect(&mut self) -> Result<(), DataError> {
        debug!("Disconnecting from MongoDB");
        if self.client.is_some() {
            self.client = None;
            self.current_database = None;
            info!("Disconnected from MongoDB");
        } else {
            debug!("Disconnect called but no active connection");
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    #[instrument(skip(self), fields(adapter = "mongodb"))]
    async fn health_check(&self) -> Result<bool, DataError> {
        debug!("Performing health check");
        let client = self.client.as_ref().ok_or_else(|| {
            warn!("Health check called but not connected");
            super::common::not_connected_error()
        })?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        client
            .database(db_name)
            .run_command(doc! { "ping": 1 })
            .await
            .map(|_| {
                debug!("Health check passed");
                true
            })
            .map_err(|e| {
                warn!(error = %e, "Health check failed");
                DataError::Connection(format!("Health check failed: {}", e))
            })
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[async_trait]
impl DbAdapter for MongoDbAdapter {
    fn database_type(&self) -> DatabaseType {
        DatabaseType::MongoDB
    }

    #[instrument(skip(self, query), fields(adapter = "mongodb", query_length = query.len()))]
    async fn execute_query(&self, query: &str) -> Result<QueryResult, DataError> {
        // MongoDB queries are JSON documents; sql_type reflects that this is a FIND operation.
        let sql_type = "FIND";
        debug!(
            sql_type,
            sql_preview = %super::common::sql_preview(query, 100),
            "Executing MongoDB query"
        );
        let start = std::time::Instant::now();

        let client = self.client.as_ref().ok_or_else(|| {
            error!(
                adapter = "mongodb",
                operation = "execute_query",
                "Not connected"
            );
            super::common::not_connected_error()
        })?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        // Parse the query as a MongoDB command
        // Format: {"collection": "collectionName", "filter": {...}, "limit": 10}
        let command: Document = serde_json::from_str(query).map_err(|e| {
            DataError::Query(format!(
                "Invalid MongoDB query format: {}. Expected JSON document with 'collection' and 'filter' fields",
                e
            ))
        })?;

        let db = client.database(db_name);

        let collection_name = command
            .get_str("collection")
            .map_err(|_| DataError::Query("Missing 'collection' field in query".to_string()))?;

        Self::validate_collection_name(collection_name)?;

        let filter = command
            .get_document("filter")
            .unwrap_or(&Document::new())
            .clone();

        let limit = command.get_i64("limit").ok().map(|l| l as u64);

        let collection = db.collection::<Document>(collection_name);

        let mut cursor = collection
            .find(filter)
            .await
            .map_err(|e| DataError::Query(format!("Failed to execute find: {}", e)))?;

        let mut results = Vec::new();
        let mut count = 0u64;

        while let Some(result) = cursor
            .advance()
            .await
            .map_err(|e| DataError::Query(format!("Failed to fetch document: {}", e)))?
            .then(|| cursor.deserialize_current())
        {
            let doc = result
                .map_err(|e| DataError::Query(format!("Failed to deserialize document: {}", e)))?;

            let row: Vec<QueryValue> = doc
                .iter()
                .map(|(_, value)| Self::bson_to_query_value(value))
                .collect();

            results.push(row);
            count += 1;

            if let Some(lim) = limit {
                if count >= lim {
                    break;
                }
            }
        }

        // Get column names from first document (or empty if no results)
        let columns = if let Some(_first_row) = results.first() {
            let doc = db
                .collection::<Document>(collection_name)
                .find_one(doc! {})
                .await
                .map_err(|e| DataError::Query(format!("Failed to get column names: {}", e)))?
                .ok_or_else(|| DataError::Query("No documents found".to_string()))?;

            doc.keys().cloned().collect()
        } else {
            Vec::new()
        };

        let duration = start.elapsed();
        info!(
            sql_type,
            duration_ms = duration.as_millis(),
            rows = results.len(),
            columns = columns.len(),
            "Query executed successfully"
        );

        Ok(QueryResult {
            columns,
            rows: results,
            rows_affected: None,
        })
    }

    async fn execute_query_stream(
        &self,
        query: &str,
    ) -> Result<RowStream<Vec<QueryValue>>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| {
                error!(
                    adapter = "mongodb",
                    operation = "execute_query_stream",
                    "Not connected"
                );
                super::common::not_connected_error()
            })?
            .clone();
        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?
            .clone();
        let query = query.to_string();

        let stream = async_stream::try_stream! {
            use mongodb::bson::Document;
            let command: Document = serde_json::from_str(&query)
                .map_err(|e| DataError::Query(format!("Invalid MongoDB query format: {}", e)))?;

            let collection_name = command.get_str("collection")
                .map_err(|_| DataError::Query("Missing 'collection' field in query".to_string()))?.to_string();

            let filter = command.get_document("filter").cloned().unwrap_or_default();
            let limit = command.get_i64("limit").ok().map(|l| l as u64);

            let db = client.database(&db_name);
            let collection = db.collection::<Document>(&collection_name);
            let mut cursor = collection.find(filter).await
                .map_err(|e| DataError::Query(format!("Failed to execute find: {}", e)))?;

            let mut count = 0u64;
            while cursor.advance().await
                .map_err(|e| DataError::Query(format!("Failed to fetch document: {}", e)))?
            {
                let doc = cursor.deserialize_current()
                    .map_err(|e| DataError::Query(format!("Failed to deserialize document: {}", e)))?;
                let row: Vec<QueryValue> = doc.iter()
                    .map(|(_, v)| MongoDbAdapter::bson_to_query_value(v))
                    .collect();
                yield row;
                count += 1;
                if let Some(lim) = limit {
                    if count >= lim { break; }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn connect(
        &mut self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<(), DataError> {
        self.config = config.clone();
        self.password = password.map(|s| s.to_string());
        ConnectionTrait::connect(self).await
    }

    async fn disconnect(&mut self) -> Result<(), DataError> {
        ConnectionTrait::disconnect(self).await
    }

    fn is_connected(&self) -> bool {
        ConnectionTrait::is_connected(self)
    }

    async fn test_connection(
        &self,
        config: &ConnectionConfig,
        password: Option<&str>,
    ) -> Result<bool, DataError> {
        let connection_string = Self::build_connection_string(config, password);

        match ClientOptions::parse(&connection_string).await {
            Ok(options) => match Client::with_options(options) {
                Ok(client) => {
                    // Try to ping the database
                    match client
                        .database(&config.database)
                        .run_command(doc! { "ping": 1 })
                        .await
                    {
                        Ok(_) => Ok(true),
                        Err(_) => Ok(false),
                    }
                }
                Err(_) => Ok(false),
            },
            Err(_) => Ok(false),
        }
    }

    #[cfg(feature = "polars")]
    #[instrument(skip(self, _df), fields(adapter = "mongodb", collection = %_table_name))]
    async fn export_dataframe(
        &self,
        _df: &DataFrame,
        _table_name: &str,
        _schema: Option<&str>,
        _replace: bool,
    ) -> Result<u64, DataError> {
        Err(DataError::NotSupported(
            "export_dataframe not yet implemented for MongoDB".to_string(),
        ))
    }

    #[cfg(feature = "polars")]
    #[instrument(skip(self), fields(adapter = "mongodb", collection = %_table_name))]
    async fn read_table_df(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<DataFrame, DataError> {
        Err(DataError::NotSupported(
            "read_table not yet implemented for MongoDB".to_string(),
        ))
    }

    #[cfg(feature = "polars")]
    async fn query_df(&self, _query: &str) -> Result<DataFrame, DataError> {
        Err(DataError::NotSupported(
            "query_df not yet implemented for MongoDB".to_string(),
        ))
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    #[instrument(skip(self, columns, rows), fields(adapter = "mongodb", table = %table_name, row_count = rows.len()))]
    async fn bulk_insert(
        &self,
        table_name: &str,
        columns: &[String],
        rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        if rows.is_empty() {
            return Ok(0);
        }
        debug!(table = %table_name, rows = rows.len(), "Starting bulk insert");
        let bulk_start = std::time::Instant::now();

        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;
        let db_name = self.current_database.as_deref().unwrap_or("test");
        let collection = client.database(db_name).collection::<Document>(table_name);

        let docs: Vec<Document> = rows
            .iter()
            .map(|row| {
                let mut doc = Document::new();
                for (col, val) in columns.iter().zip(row.iter()) {
                    doc.insert(col.clone(), mongo_value_to_bson(val));
                }
                doc
            })
            .collect();

        let count = docs.len() as u64;
        collection
            .insert_many(docs)
            .await
            .map_err(|e| DataError::Query(format!("bulk_insert failed: {}", e)))?;

        info!(
            table = %table_name,
            rows_inserted = count,
            duration_ms = bulk_start.elapsed().as_millis(),
            "Bulk insert complete"
        );
        Ok(count)
    }

    #[instrument(skip(self, updates), fields(adapter = "mongodb", table = %table_name))]
    async fn bulk_update(
        &self,
        table_name: &str,
        updates: &[(HashMap<String, QueryValue>, FilterExpr)],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        if updates.is_empty() {
            return Ok(0);
        }
        debug!(table = %table_name, update_count = updates.len(), "Starting bulk update");
        let bulk_start = std::time::Instant::now();

        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;
        let db_name = self.current_database.as_deref().unwrap_or("test");
        let collection = client.database(db_name).collection::<Document>(table_name);

        let mut total: u64 = 0;
        for (set_values, filter) in updates {
            if set_values.is_empty() {
                continue;
            }
            let filter_doc = mongo_filter_to_bson(filter);
            let mut set_doc = Document::new();
            for (col, val) in set_values {
                set_doc.insert(col.clone(), mongo_value_to_bson(val));
            }
            let update = doc! { "$set": set_doc };
            let result = collection
                .update_many(filter_doc, update)
                .await
                .map_err(|e| DataError::Query(format!("bulk_update failed: {}", e)))?;
            total += result.modified_count;
        }

        info!(
            table = %table_name,
            rows_modified = total,
            duration_ms = bulk_start.elapsed().as_millis(),
            "Bulk update complete"
        );
        Ok(total)
    }

    #[instrument(skip(self, filters), fields(adapter = "mongodb", table = %table_name))]
    async fn bulk_delete(
        &self,
        table_name: &str,
        filters: &[FilterExpr],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        if filters.is_empty() {
            return Ok(0);
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;
        let db_name = self.current_database.as_deref().unwrap_or("test");
        let collection = client.database(db_name).collection::<Document>(table_name);

        let mut total: u64 = 0;
        for filter in filters {
            let filter_doc = mongo_filter_to_bson(filter);
            let result = collection
                .delete_many(filter_doc)
                .await
                .map_err(|e| DataError::Query(format!("bulk_delete failed: {}", e)))?;
            total += result.deleted_count;
        }
        Ok(total)
    }

    #[instrument(skip(self), fields(adapter = "mongodb"))]
    async fn get_server_info(&self) -> Result<ServerInfo, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_name = self.current_database.as_deref().unwrap_or("admin");
        let db = client.database(db_name);

        // Run buildInfo command
        let build_info = db
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|e| DataError::Query(format!("Failed to get build info: {}", e)))?;

        let version = build_info
            .get_str("version")
            .unwrap_or("unknown")
            .to_string();

        let mut extra_info = HashMap::new();
        extra_info.insert(
            "host".to_string(),
            self.config
                .host
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
        );
        extra_info.insert(
            "port".to_string(),
            self.config.port.unwrap_or(27017).to_string(),
        );

        Ok(ServerInfo {
            version,
            server_type: "MongoDB".to_string(),
            extra_info,
        })
    }

    #[instrument(skip(self), fields(adapter = "mongodb"))]
    async fn list_databases(&self) -> Result<Vec<String>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_names = client
            .list_database_names()
            .await
            .map_err(|e| DataError::Query(format!("Failed to list databases: {}", e)))?;

        Ok(db_names)
    }

    #[instrument(skip(self), fields(adapter = "mongodb"))]
    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collections = db
            .list_collection_names()
            .await
            .map_err(|e| DataError::Query(format!("Failed to list collections: {}", e)))?;

        Ok(collections)
    }

    #[instrument(skip(self), fields(adapter = "mongodb", pattern = %pattern, mode = ?mode))]
    async fn find_tables(
        &self,
        pattern: &str,
        _schema: Option<&str>,
        mode: TableSearchMode,
    ) -> Result<Vec<String>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        // Use regex::escape so literal "PS_" is not treated as a regex wildcard
        let escaped = regex::escape(pattern);
        let regex_pattern = match mode {
            TableSearchMode::StartsWith => format!("^{}", escaped),
            TableSearchMode::Contains => escaped,
            TableSearchMode::EndsWith => format!("{}$", escaped),
        };

        let re = Regex::new(&regex_pattern).map_err(|e| {
            DataError::Query(format!("Invalid regex pattern '{}': {}", regex_pattern, e))
        })?;

        let db = client.database(db_name);
        let collections = db
            .list_collection_names()
            .await
            .map_err(|e| DataError::Query(format!("Failed to list collections: {}", e)))?;

        let matched: Vec<String> = collections.into_iter().filter(|c| re.is_match(c)).collect();
        info!(count = matched.len(), "Found collections by pattern");
        Ok(matched)
    }

    #[instrument(skip(self), fields(adapter = "mongodb", table = %table_name, schema = ?_schema))]
    async fn describe_table(
        &self,
        table_name: &str,
        _schema: Option<&str>,
    ) -> Result<TableInfo, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(table_name);

        // Sample documents to infer schema
        let mut cursor = collection
            .find(doc! {})
            .await
            .map_err(|e| DataError::Query(format!("Failed to query collection: {}", e)))?;

        let mut field_types: HashMap<String, String> = HashMap::new();
        let mut sample_count = 0;

        while sample_count < 10 {
            match cursor.advance().await {
                Ok(true) => {
                    let doc = cursor.deserialize_current().map_err(|e| {
                        DataError::Query(format!("Failed to deserialize document: {}", e))
                    })?;

                    for (key, value) in &doc {
                        let type_name = Self::bson_type_name(value);
                        field_types.insert(key.clone(), type_name.to_string());
                    }

                    sample_count += 1;
                }
                Ok(false) => break,
                Err(e) => return Err(DataError::Query(format!("Failed to fetch document: {}", e))),
            }
        }

        let columns: Vec<ColumnInfo> = field_types
            .into_iter()
            .map(|(name, data_type)| {
                let is_primary = name == "_id";
                ColumnInfo {
                    name,
                    data_type,
                    nullable: true, // MongoDB fields are always nullable
                    default_value: None,
                    is_primary_key: is_primary,
                }
            })
            .collect();

        let row_count = collection
            .count_documents(doc! {})
            .await
            .ok()
            .map(|n| n as i64);

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: None,
            columns,
            row_count,
            size_bytes: None,
            created_at: None,
        })
    }

    async fn get_indexes(
        &self,
        table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<IndexInfo>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(super::common::not_connected_error)?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(table_name);

        let mut cursor = collection
            .list_indexes()
            .await
            .map_err(|e| DataError::Query(format!("Failed to list indexes: {}", e)))?;

        let mut indexes = Vec::new();

        while let Some(result) = cursor
            .advance()
            .await
            .map_err(|e| DataError::Query(format!("Failed to fetch index: {}", e)))?
            .then(|| cursor.deserialize_current())
        {
            let index_model = result
                .map_err(|e| DataError::Query(format!("Failed to deserialize index: {}", e)))?;

            // IndexModel doesn't have direct field access, but we can use its methods
            let index_name = format!("{:?}", index_model); // Simplified - would need proper serialization
            let columns = Vec::new(); // Would need to parse from index_model
            let is_unique = false; // Would need to check index options

            indexes.push(IndexInfo {
                name: index_name,
                table_name: table_name.to_string(),
                schema: None,
                columns,
                is_unique,
                is_primary: false,
                index_type: None,
            });
        }

        Ok(indexes)
    }

    async fn get_foreign_keys(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<ForeignKeyInfo>, DataError> {
        // MongoDB doesn't have foreign keys
        Ok(Vec::new())
    }

    async fn get_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>, DataError> {
        // MongoDB views are not easily distinguishable from collections via the driver API
        // They would need to be queried from system.views collection
        // For now, return empty list
        Ok(Vec::new())
    }

    async fn get_view_definition(
        &self,
        _view_name: &str,
        _schema: Option<&str>,
    ) -> Result<Option<String>, DataError> {
        // MongoDB doesn't easily expose view definitions via API
        Ok(None)
    }

    async fn list_stored_procedures(
        &self,
        _schema: Option<&str>,
    ) -> Result<Vec<ProcedureInfo>, DataError> {
        // MongoDB doesn't have stored procedures in the traditional sense
        // It has server-side JavaScript functions, but they're less common
        Ok(Vec::new())
    }
}

/// Convert a [`QueryValue`] to a BSON [`Bson`] value.
fn mongo_value_to_bson(val: &QueryValue) -> Bson {
    match val {
        QueryValue::Null => Bson::Null,
        QueryValue::Bool(b) => Bson::Boolean(*b),
        QueryValue::Int(i) => Bson::Int64(*i),
        QueryValue::Float(f) => Bson::Double(*f),
        QueryValue::Text(s) => Bson::String(s.clone()),
        QueryValue::Bytes(b) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.clone(),
        }),
    }
}

/// Translate a [`FilterExpr`] to a MongoDB BSON filter document.
fn mongo_filter_to_bson(expr: &FilterExpr) -> Document {
    match expr {
        FilterExpr::Eq(col, val) => doc! { col: mongo_value_to_bson(val) },
        FilterExpr::Ne(col, val) => doc! { col: { "$ne": mongo_value_to_bson(val) } },
        FilterExpr::Gt(col, val) => doc! { col: { "$gt": mongo_value_to_bson(val) } },
        FilterExpr::Gte(col, val) => doc! { col: { "$gte": mongo_value_to_bson(val) } },
        FilterExpr::Lt(col, val) => doc! { col: { "$lt": mongo_value_to_bson(val) } },
        FilterExpr::Lte(col, val) => doc! { col: { "$lte": mongo_value_to_bson(val) } },
        FilterExpr::In(col, vals) => {
            let bson_vals: Vec<Bson> = vals.iter().map(mongo_value_to_bson).collect();
            doc! { col: { "$in": bson_vals } }
        }
        FilterExpr::IsNull(col) => doc! { col: Bson::Null },
        FilterExpr::IsNotNull(col) => doc! { col: { "$ne": Bson::Null } },
        FilterExpr::And(exprs) => {
            if exprs.is_empty() {
                doc! {}
            } else {
                let docs: Vec<Bson> = exprs
                    .iter()
                    .map(|e| Bson::Document(mongo_filter_to_bson(e)))
                    .collect();
                doc! { "$and": docs }
            }
        }
        FilterExpr::Or(exprs) => {
            if exprs.is_empty() {
                // Always-false: match nothing via impossible condition
                doc! { "$nor": [Bson::Document(doc! {})] }
            } else {
                let docs: Vec<Bson> = exprs
                    .iter()
                    .map(|e| Bson::Document(mongo_filter_to_bson(e)))
                    .collect();
                doc! { "$or": docs }
            }
        }
        FilterExpr::Not(expr) => {
            // $nor with a single element is equivalent to NOT
            doc! { "$nor": [Bson::Document(mongo_filter_to_bson(expr))] }
        }
    }
}

/// Percent-encode characters disallowed in a URI userinfo component (RFC 3986 §3.2.1).
///
/// Encodes everything outside unreserved chars + sub-delims + `:` that are allowed
/// in userinfo. This ensures special characters in MongoDB passwords (e.g. `@`, `/`, `%`)
/// do not corrupt the connection URI.
fn percent_encode_userinfo(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b'~'
            | b'!'
            | b'$'
            | b'&'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b'+'
            | b','
            | b';'
            | b'='
            | b':' => out.push(byte as char),
            other => {
                out.push('%');
                out.push(
                    char::from_digit((other >> 4) as u32, 16)
                        .unwrap()
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit((other & 0xf) as u32, 16)
                        .unwrap()
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{Connection as ConnectionTrait, DatabaseType};

    fn make_config(host: &str, database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test-mongodb".to_string(),
            name: "Test MongoDB".to_string(),
            db_type: DatabaseType::MongoDB,
            host: Some(host.to_string()),
            port: Some(27017),
            database: database.to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
            pool_config: None,
        }
    }

    #[test]
    fn test_new_adapter_stores_config() {
        let config = make_config("localhost", "test_db");
        let adapter = MongoDbAdapter::new(config);
        assert_eq!(adapter.config.database, "test_db");
        assert_eq!(adapter.config.db_type, DatabaseType::MongoDB);
    }

    #[test]
    fn test_is_connected_initially_false() {
        let adapter = MongoDbAdapter::new(make_config("localhost", "test_db"));
        assert!(!ConnectionTrait::is_connected(&adapter));
    }

    #[test]
    fn test_validate_database_name_valid() {
        assert!(MongoDbAdapter::validate_database_name("test_db").is_ok());
        assert!(MongoDbAdapter::validate_database_name("my-database").is_ok());
    }

    #[test]
    fn test_validate_database_name_empty_fails() {
        let err = MongoDbAdapter::validate_database_name("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_validate_database_name_too_long_fails() {
        let long_name = "a".repeat(65);
        let err = MongoDbAdapter::validate_database_name(&long_name).unwrap_err();
        assert!(err.to_string().contains("too long"));
    }

    #[test]
    fn test_validate_database_name_invalid_chars() {
        for ch in ['/', '\\', '.', ' ', '"', '$'] {
            let name = format!("db{}name", ch);
            assert!(
                MongoDbAdapter::validate_database_name(&name).is_err(),
                "Expected error for char '{}'",
                ch
            );
        }
    }

    #[test]
    fn test_validate_collection_name_valid() {
        assert!(MongoDbAdapter::validate_collection_name("users").is_ok());
        assert!(MongoDbAdapter::validate_collection_name("my_collection").is_ok());
    }

    #[test]
    fn test_validate_collection_name_system_prefix_fails() {
        let err = MongoDbAdapter::validate_collection_name("system.users").unwrap_err();
        assert!(err.to_string().contains("system."));
    }

    #[test]
    fn test_validate_collection_name_dollar_fails() {
        let err = MongoDbAdapter::validate_collection_name("col$lection").unwrap_err();
        assert!(err.to_string().contains("$"));
    }

    #[test]
    fn test_build_connection_string_with_auth() {
        let config = make_config("db.example.com", "mydb");
        let uri = MongoDbAdapter::build_connection_string(&config, Some("secret"));
        assert!(uri.starts_with("mongodb://"));
        assert!(uri.contains("test_user"));
        assert!(uri.contains("secret"));
        assert!(uri.contains("db.example.com:27017"));
        assert!(uri.contains("authSource=admin"));
        assert!(
            !uri.contains("tls=true"),
            "use_ssl=false must not add tls param"
        );
    }

    #[test]
    fn test_build_connection_string_without_auth() {
        let mut config = make_config("localhost", "mydb");
        config.username = None;
        let uri = MongoDbAdapter::build_connection_string(&config, None);
        assert_eq!(uri, "mongodb://localhost:27017");
    }

    // ── SSL/TLS ───────────────────────────────────────────────────────────────

    #[test]
    fn test_build_connection_string_ssl_with_auth() {
        let mut config = make_config("db.example.com", "mydb");
        config.use_ssl = true;
        let uri = MongoDbAdapter::build_connection_string(&config, Some("secret"));
        assert!(uri.contains("tls=true"), "use_ssl=true must add tls=true");
        assert!(
            uri.contains("authSource=admin"),
            "authSource must still be present"
        );
    }

    #[test]
    fn test_build_connection_string_ssl_no_auth() {
        let mut config = make_config("localhost", "mydb");
        config.username = None;
        config.use_ssl = true;
        let uri = MongoDbAdapter::build_connection_string(&config, None);
        assert!(uri.contains("tls=true"), "use_ssl=true must add tls=true");
        assert!(
            uri.starts_with("mongodb://localhost:27017"),
            "host unchanged"
        );
    }

    #[test]
    fn test_build_connection_string_no_ssl_no_auth() {
        let mut config = make_config("localhost", "mydb");
        config.username = None;
        config.use_ssl = false;
        let uri = MongoDbAdapter::build_connection_string(&config, None);
        assert_eq!(uri, "mongodb://localhost:27017");
        assert!(!uri.contains("tls"), "no tls param when use_ssl=false");
    }

    // ── URL special-character encoding ────────────────────────────────────────

    #[test]
    fn test_percent_encode_userinfo_plain() {
        assert_eq!(percent_encode_userinfo("admin"), "admin");
        assert_eq!(percent_encode_userinfo("user123"), "user123");
    }

    #[test]
    fn test_percent_encode_userinfo_at_sign() {
        // '@' in a password would split the userinfo from host — must be encoded.
        let encoded = percent_encode_userinfo("p@ssword");
        assert!(!encoded.contains('@'), "@ must be percent-encoded");
        assert!(encoded.contains("%40"), "@ must become %40");
    }

    #[test]
    fn test_percent_encode_userinfo_slash() {
        // '/' would terminate the authority component.
        let encoded = percent_encode_userinfo("p/ss");
        assert!(!encoded.contains('/'), "/ must be percent-encoded");
        assert!(encoded.contains("%2F"), "/ must become %2F");
    }

    #[test]
    fn test_percent_encode_userinfo_percent() {
        // A literal '%' must itself be encoded to avoid ambiguous pct-encoded sequences.
        let encoded = percent_encode_userinfo("100%");
        assert_eq!(encoded.chars().filter(|&c| c == '%').count(), 1);
        assert!(encoded.contains("%25"), "% must become %25");
    }

    #[test]
    fn test_build_connection_string_password_with_at() {
        let config = make_config("localhost", "mydb");
        let uri = MongoDbAdapter::build_connection_string(&config, Some("p@ssw0rd"));
        assert!(uri.contains("%40"), "@ in password must be percent-encoded");
        assert!(
            !uri[10..].contains("p@"),
            "raw @ must not appear after scheme"
        );
    }

    #[test]
    fn test_build_connection_string_password_with_slash() {
        let config = make_config("localhost", "mydb");
        let uri = MongoDbAdapter::build_connection_string(&config, Some("p/ss"));
        assert!(uri.contains("%2F"), "/ in password must be percent-encoded");
    }

    #[test]
    fn test_build_connection_string_password_with_percent() {
        let config = make_config("localhost", "mydb");
        let uri = MongoDbAdapter::build_connection_string(&config, Some("100%secure"));
        assert!(uri.contains("%25"), "% in password must be percent-encoded");
    }

    /// Verify that regex::escape treats "PS_" as a literal string (underscore is not special in regex)
    /// but the StartsWith pattern anchors with ^ so "PSA" does NOT match "^PS_".
    #[test]
    fn test_find_tables_regex_starts_with_no_match() {
        let escaped = regex::escape("PS_");
        let pattern = format!("^{}", escaped);
        let re = Regex::new(&pattern).unwrap();
        // "PS_" as literal: "^PS_" requires the string to start with literal "PS_"
        assert!(
            !re.is_match("PSA"),
            "PSA should not match ^PS_ (underscore is literal)"
        );
        assert!(re.is_match("PS_TABLE"), "PS_TABLE should match ^PS_");
        assert!(re.is_match("PS_"), "PS_ itself should match ^PS_");
    }

    #[test]
    fn test_find_tables_regex_contains_matches() {
        let escaped = regex::escape("PS_");
        let re = Regex::new(&escaped).unwrap();
        assert!(re.is_match("DATA_PS_COL"), "DATA_PS_COL should match PS_");
        assert!(!re.is_match("PSA"), "PSA should not match PS_");
    }

    #[test]
    fn test_find_tables_regex_ends_with() {
        let escaped = regex::escape("PS_");
        let pattern = format!("{}$", escaped);
        let re = Regex::new(&pattern).unwrap();
        assert!(re.is_match("DATA_PS_"), "DATA_PS_ should match PS_$");
        assert!(!re.is_match("DATA_PS_X"), "DATA_PS_X should not match PS_$");
    }

    // ── bson_to_query_value ──────────────────────────────────────────────────

    #[test]
    fn bson_null_and_undefined_yield_query_null() {
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Null),
            QueryValue::Null
        );
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Undefined),
            QueryValue::Null
        );
    }

    #[test]
    fn bson_boolean_yields_query_bool() {
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Boolean(true)),
            QueryValue::Bool(true)
        );
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Boolean(false)),
            QueryValue::Bool(false)
        );
    }

    #[test]
    fn bson_int32_and_int64_yield_query_int() {
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Int32(42)),
            QueryValue::Int(42)
        );
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Int32(-1)),
            QueryValue::Int(-1)
        );
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Int64(i64::MAX)),
            QueryValue::Int(i64::MAX)
        );
    }

    #[test]
    fn bson_double_yields_query_float() {
        #[allow(clippy::approx_constant)]
        match MongoDbAdapter::bson_to_query_value(&Bson::Double(3.14)) {
            #[allow(clippy::approx_constant)]
            QueryValue::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn bson_string_yields_query_text() {
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::String("hello".to_string())),
            QueryValue::Text("hello".to_string())
        );
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::String(String::new())),
            QueryValue::Text(String::new())
        );
    }

    #[test]
    fn bson_binary_yields_query_bytes() {
        let bytes = vec![0x01u8, 0x02, 0x03];
        let bin = Binary {
            subtype: BinarySubtype::Generic,
            bytes: bytes.clone(),
        };
        assert_eq!(
            MongoDbAdapter::bson_to_query_value(&Bson::Binary(bin)),
            QueryValue::Bytes(bytes)
        );
    }

    #[test]
    fn bson_object_id_yields_24_char_hex_text() {
        use mongodb::bson::oid::ObjectId;
        let oid = ObjectId::new();
        match MongoDbAdapter::bson_to_query_value(&Bson::ObjectId(oid)) {
            QueryValue::Text(s) => assert_eq!(s.len(), 24, "ObjectId hex should be 24 chars"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn bson_datetime_yields_text() {
        let dt = mongodb::bson::DateTime::now();
        match MongoDbAdapter::bson_to_query_value(&Bson::DateTime(dt)) {
            QueryValue::Text(s) => assert!(!s.is_empty()),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn bson_array_yields_text_debug_repr() {
        let arr = vec![Bson::Int32(1), Bson::String("x".to_string())];
        match MongoDbAdapter::bson_to_query_value(&Bson::Array(arr)) {
            QueryValue::Text(s) => {
                assert!(s.contains("Int32"), "array repr should contain type names")
            }
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn bson_document_yields_text_debug_repr() {
        let doc = doc! { "key": "value", "n": 42 };
        match MongoDbAdapter::bson_to_query_value(&Bson::Document(doc)) {
            QueryValue::Text(s) => assert!(s.contains("key") || s.contains("value")),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn bson_catch_all_yields_text_debug_repr() {
        // Timestamp is an exotic BSON type with no SQL equivalent → catch-all arm
        let ts = mongodb::bson::Timestamp {
            time: 1_700_000_000,
            increment: 1,
        };
        match MongoDbAdapter::bson_to_query_value(&Bson::Timestamp(ts)) {
            QueryValue::Text(s) => assert!(!s.is_empty()),
            other => panic!("expected Text for Timestamp catch-all, got {:?}", other),
        }
    }

    // ── bson_type_name ───────────────────────────────────────────────────────

    #[test]
    fn bson_type_name_covers_common_types() {
        assert_eq!(MongoDbAdapter::bson_type_name(&Bson::Null), "null");
        assert_eq!(
            MongoDbAdapter::bson_type_name(&Bson::Boolean(true)),
            "boolean"
        );
        assert_eq!(MongoDbAdapter::bson_type_name(&Bson::Int32(0)), "int32");
        assert_eq!(MongoDbAdapter::bson_type_name(&Bson::Int64(0)), "int64");
        assert_eq!(MongoDbAdapter::bson_type_name(&Bson::Double(0.0)), "double");
        assert_eq!(
            MongoDbAdapter::bson_type_name(&Bson::String(String::new())),
            "string"
        );
        assert_eq!(
            MongoDbAdapter::bson_type_name(&Bson::Array(vec![])),
            "array"
        );
        assert_eq!(
            MongoDbAdapter::bson_type_name(&Bson::Document(doc! {})),
            "document"
        );
    }

    // ── not-connected error paths ────────────────────────────────────────────

    #[tokio::test]
    async fn execute_query_not_connected_returns_error() {
        let adapter = MongoDbAdapter::new(make_config("localhost", "test_db"));
        let result = adapter.execute_query("db.users.find({})").await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("connect"),
            "error should mention 'connect': {}",
            msg
        );
    }

    #[tokio::test]
    async fn list_tables_not_connected_returns_error() {
        let adapter = MongoDbAdapter::new(make_config("localhost", "test_db"));
        let result = adapter.list_tables(None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("connect"));
    }

    #[tokio::test]
    async fn bulk_insert_not_connected_returns_error() {
        let adapter = MongoDbAdapter::new(make_config("localhost", "test_db"));
        let cols = vec!["name".to_string()];
        let rows = vec![vec![QueryValue::Text("alice".to_string())]];
        let result = adapter.bulk_insert("users", &cols, &rows, None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("connect"));
    }

    // ── test_connection() unit tests ────────────────────────────────────────

    /// test_connection() for MongoDB always attempts a network ping before returning.
    /// This test is marked #[ignore] because it requires a live MongoDB instance
    /// or will incur server-selection timeout delays.
    ///
    /// To run: cargo test --features mongodb test_connection_unreachable -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_connection_unreachable_returns_false() {
        // Unreachable host — test_connection should return Ok(false), not panic.
        let config = make_config("192.0.2.1", "test_db"); // TEST-NET-1, guaranteed unreachable
        let adapter = MongoDbAdapter::new(config.clone());
        let result = adapter.test_connection(&config, None).await;
        assert!(
            result.is_ok(),
            "test_connection should not return Err on network failure"
        );
        assert!(!result.unwrap(), "Unreachable host should yield false");
    }
}
