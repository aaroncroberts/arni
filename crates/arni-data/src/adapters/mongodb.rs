use crate::adapter::{
    AdapterMetadata, ColumnInfo, Connection as ConnectionTrait, ConnectionConfig, DatabaseType,
    DbAdapter, ForeignKeyInfo, IndexInfo, ProcedureInfo, QueryResult, QueryValue, ServerInfo,
    TableInfo, ViewInfo,
};
use crate::DataError;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Bson, Document},
    options::ClientOptions,
    Client,
};
use polars::prelude::*;
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

    /// Build a connection string from configuration
    fn build_connection_string(config: &ConnectionConfig, password: Option<&str>) -> String {
        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(27017);
        let username = config.username.as_deref();

        if let (Some(user), Some(pass)) = (username, password) {
            // Include authSource=admin for root user authentication
            format!(
                "mongodb://{}:{}@{}:{}/?authSource=admin",
                user, pass, host, port
            )
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
            error!(error = %err, "Invalid database type");
            return Err(err);
        }

        Self::validate_database_name(&self.config.database)?;

        let host = self.config.host.as_deref().unwrap_or("localhost");
        let port = self.config.port.unwrap_or(27017);
        info!(host, port, database = %self.config.database, "Connecting to MongoDB");

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
            .run_command(doc! { "ping": 1 }, None)
            .await
            .map_err(|e| {
                let error_msg = e.to_string();

                if error_msg.contains("authentication failed") || error_msg.contains("auth failed")
                {
                    DataError::Connection(format!(
                        "Authentication failed for database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ))
                } else if error_msg.contains("connection refused")
                    || error_msg.contains("No connection available")
                {
                    DataError::Connection(format!(
                        "Network error connecting to MongoDB at {}:{} - {}",
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ))
                } else if error_msg.contains("not master") || error_msg.contains("replica set") {
                    DataError::Connection(format!(
                        "Replica set configuration issue at {}:{} - {}",
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ))
                } else if error_msg.contains("unauthorized") {
                    DataError::Connection(format!(
                        "Unauthorized access to database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ))
                } else {
                    DataError::Connection(format!(
                        "Failed to connect to database '{}' at {}:{} - {}",
                        self.config.database,
                        self.config.host.as_deref().unwrap_or("localhost"),
                        self.config.port.unwrap_or(27017),
                        e
                    ))
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
            DataError::Connection("Not connected".to_string())
        })?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        client
            .database(db_name)
            .run_command(doc! { "ping": 1 }, None)
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
        debug!("Executing MongoDB query");
        let start = std::time::Instant::now();

        let client = self.client.as_ref().ok_or_else(|| {
            error!("Query attempted while not connected");
            DataError::Connection("Not connected".to_string())
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
            .find(filter, None)
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
                .find_one(None, None)
                .await
                .map_err(|e| DataError::Query(format!("Failed to get column names: {}", e)))?
                .ok_or_else(|| DataError::Query("No documents found".to_string()))?;

            doc.keys().cloned().collect()
        } else {
            Vec::new()
        };

        let duration = start.elapsed();
        info!(
            rows = results.len(),
            duration_ms = duration.as_millis(),
            "Query executed successfully"
        );

        Ok(QueryResult {
            columns,
            rows: results,
            rows_affected: None,
        })
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
                        .run_command(doc! { "ping": 1 }, None)
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

    async fn read_table(
        &self,
        _table_name: &str,
        _schema: Option<&str>,
    ) -> Result<DataFrame, DataError> {
        Err(DataError::NotSupported(
            "read_table not yet implemented for MongoDB".to_string(),
        ))
    }

    async fn query_df(&self, _query: &str) -> Result<DataFrame, DataError> {
        Err(DataError::NotSupported(
            "query_df not yet implemented for MongoDB".to_string(),
        ))
    }

    fn metadata(&self) -> AdapterMetadata<'_> {
        AdapterMetadata::new(self)
    }

    async fn bulk_insert(
        &self,
        _table_name: &str,
        _columns: &[String],
        _rows: &[Vec<QueryValue>],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        Err(DataError::NotSupported(
            "bulk_insert not yet implemented for MongoDB".to_string(),
        ))
    }

    async fn bulk_update(
        &self,
        _table_name: &str,
        _updates: &[(HashMap<String, QueryValue>, String)],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        Err(DataError::NotSupported(
            "bulk_update not yet implemented for MongoDB".to_string(),
        ))
    }

    async fn bulk_delete(
        &self,
        _table_name: &str,
        _where_clauses: &[String],
        _schema: Option<&str>,
    ) -> Result<u64, DataError> {
        Err(DataError::NotSupported(
            "bulk_delete not yet implemented for MongoDB".to_string(),
        ))
    }

    async fn get_server_info(&self) -> Result<ServerInfo, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DataError::Connection("Not connected to database".to_string()))?;

        let db_name = self.current_database.as_deref().unwrap_or("admin");
        let db = client.database(db_name);

        // Run buildInfo command
        let build_info = db
            .run_command(doc! { "buildInfo": 1 }, None)
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

    async fn list_databases(&self) -> Result<Vec<String>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DataError::Connection("Not connected".to_string()))?;

        let db_names = client
            .list_database_names(None, None)
            .await
            .map_err(|e| DataError::Query(format!("Failed to list databases: {}", e)))?;

        Ok(db_names)
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DataError::Connection("Not connected".to_string()))?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collections = db
            .list_collection_names(None)
            .await
            .map_err(|e| DataError::Query(format!("Failed to list collections: {}", e)))?;

        Ok(collections)
    }

    async fn describe_table(
        &self,
        table_name: &str,
        _schema: Option<&str>,
    ) -> Result<TableInfo, DataError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| DataError::Connection("Not connected".to_string()))?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(table_name);

        // Sample documents to infer schema
        let mut cursor = collection
            .find(None, None)
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

        Ok(TableInfo {
            name: table_name.to_string(),
            schema: None,
            columns,
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
            .ok_or_else(|| DataError::Connection("Not connected".to_string()))?;

        let db_name = self
            .current_database
            .as_ref()
            .ok_or_else(|| DataError::Connection("No database selected".to_string()))?;

        let db = client.database(db_name);
        let collection = db.collection::<Document>(table_name);

        let mut cursor = collection
            .list_indexes(None)
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
    }

    #[test]
    fn test_build_connection_string_without_auth() {
        let mut config = make_config("localhost", "mydb");
        config.username = None;
        let uri = MongoDbAdapter::build_connection_string(&config, None);
        assert_eq!(uri, "mongodb://localhost:27017");
    }
}
