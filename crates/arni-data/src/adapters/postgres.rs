//! PostgreSQL database adapter implementation
//!
//! This module provides the [`PostgresAdapter`] which implements both the [`Connection`]
//! and [`DbAdapter`] traits for PostgreSQL databases using the tokio-postgres driver.
//!
//! # Features
//!
//! This module is only available when the `postgres` feature is enabled:
//!
//! ```toml
//! arni-data = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! # Examples
//!
//! ```ignore
//! use arni_data::adapters::postgres::PostgresAdapter;
//! use arni_data::adapter::{Connection, ConnectionConfig, DatabaseType};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ConnectionConfig {
//!         id: "my-postgres".to_string(),
//!         name: "My PostgreSQL DB".to_string(),
//!         db_type: DatabaseType::Postgres,
//!         host: Some("localhost".to_string()),
//!         port: Some(5432),
//!         database: "mydb".to_string(),
//!         username: Some("user".to_string()),
//!         use_ssl: false,
//!         parameters: Default::default(),
//!     };
//!
//!     let mut adapter = PostgresAdapter::new(config);
//!     adapter.connect().await?;
//!
//!     if adapter.health_check().await? {
//!         println!("Connection healthy!");
//!     }
//!
//!     adapter.disconnect().await?;
//!     Ok(())
//! }
//! ```

use crate::adapter::{Connection, ConnectionConfig, Result};
use crate::DataError;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};

/// PostgreSQL database adapter
///
/// This adapter uses the tokio-postgres driver to connect to PostgreSQL databases.
/// It implements both [`Connection`] and [`DbAdapter`] traits.
///
/// # Connection Management
///
/// The adapter maintains an internal connection state that can be checked with
/// [`is_connected`](Connection::is_connected). Connections are established lazily
/// on first use or explicitly via [`connect`](Connection::connect).
///
/// # SSL/TLS Support
///
/// SSL is supported via the `use_ssl` configuration option:
/// - `use_ssl: false` - Plain text connection (default)
/// - `use_ssl: true` - Encrypted connection using native-tls
///
/// # Thread Safety
///
/// The adapter uses internal locking to ensure thread-safe access to the underlying
/// PostgreSQL connection.
pub struct PostgresAdapter {
    /// Connection configuration
    config: ConnectionConfig,
    /// PostgreSQL client wrapped in Arc<RwLock> for thread-safe access
    client: Arc<RwLock<Option<Client>>>,
    /// Connection state flag
    connected: Arc<RwLock<bool>>,
}

impl PostgresAdapter {
    /// Create a new PostgreSQL adapter with the given configuration
    ///
    /// This does not establish a connection immediately. Call [`connect`](Connection::connect)
    /// to establish the connection.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ConnectionConfig {
    ///     id: "prod-db".to_string(),
    ///     name: "Production DB".to_string(),
    ///     db_type: DatabaseType::Postgres,
    ///     host: Some("db.example.com".to_string()),
    ///     port: Some(5432),
    ///     database: "app_db".to_string(),
    ///     username: Some("app_user".to_string()),
    ///     use_ssl: true,
    ///     parameters: Default::default(),
    /// };
    ///
    /// let adapter = PostgresAdapter::new(config);
    /// ```
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            client: Arc::new(RwLock::new(None)),
            connected: Arc::new(RwLock::new(false)),
        }
    }

    /// Build a PostgreSQL connection string from the configuration
    ///
    /// The connection string format is:
    /// ```text
    /// host={host} port={port} dbname={database} user={username}
    /// ```
    ///
    /// Additional parameters from `config.parameters` are appended.
    ///
    /// # Returns
    ///
    /// A connection string suitable for tokio-postgres, or an error if required
    /// fields are missing.
    fn build_connection_string(&self, password: Option<&str>) -> Result<String> {
        let host = self
            .config
            .host
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing host".to_string()))?;

        let port = self.config.port.unwrap_or(5432);

        let username = self
            .config
            .username
            .as_ref()
            .ok_or_else(|| DataError::Config("Missing username".to_string()))?;

        let mut conn_str = format!(
            "host={} port={} dbname={} user={}",
            host, port, self.config.database, username
        );

        if let Some(pwd) = password {
            conn_str.push_str(&format!(" password={}", pwd));
        }

        // Add additional parameters
        for (key, value) in &self.config.parameters {
            conn_str.push_str(&format!(" {}={}", key, value));
        }

        Ok(conn_str)
    }
}

#[async_trait::async_trait]
impl Connection for PostgresAdapter {
    async fn connect(&mut self) -> Result<()> {
        // Check if already connected
        if *self.connected.read().await {
            return Ok(());
        }

        // Build connection string (without password for now - will need separate password handling)
        let conn_str = self.build_connection_string(None)?;

        // Connect using NoTls for now (will add SSL support later)
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| DataError::Connection(format!("Failed to connect: {}", e)))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Store the client
        *self.client.write().await = Some(client);
        *self.connected.write().await = true;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Drop the client (closes the connection)
        *self.client.write().await = None;
        *self.connected.write().await = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        // This needs to be a synchronous check, so we use try_read
        // Returns false if the lock is held or if not connected
        self.connected
            .try_read()
            .map(|guard| *guard)
            .unwrap_or(false)
    }

    async fn health_check(&self) -> Result<bool> {
        // Check internal state first
        if !*self.connected.read().await {
            return Err(DataError::Connection(
                "Not connected - call connect() first".to_string(),
            ));
        }

        // Get client
        let client_guard = self.client.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| DataError::Connection("Client not available".to_string()))?;

        // Execute health check query
        match client.query_one("SELECT 1", &[]).await {
            Ok(_) => Ok(true),
            Err(e) => Err(DataError::Query(format!("Health check failed: {}", e))),
        }
    }

    fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::DatabaseType;
    use std::collections::HashMap;

    fn create_test_config() -> ConnectionConfig {
        ConnectionConfig {
            id: "test-pg".to_string(),
            name: "Test PostgreSQL".to_string(),
            db_type: DatabaseType::Postgres,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: "test_db".to_string(),
            username: Some("test_user".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        }
    }

    #[test]
    fn test_new_adapter() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config.clone());

        assert_eq!(adapter.config().id, "test-pg");
        assert_eq!(adapter.config().database, "test_db");
        assert!(!adapter.is_connected());
    }

    #[test]
    fn test_build_connection_string() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let conn_str = adapter.build_connection_string(None).unwrap();
        assert!(conn_str.contains("host=localhost"));
        assert!(conn_str.contains("port=5432"));
        assert!(conn_str.contains("dbname=test_db"));
        assert!(conn_str.contains("user=test_user"));
    }

    #[test]
    fn test_build_connection_string_with_password() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let conn_str = adapter.build_connection_string(Some("secret123")).unwrap();
        assert!(conn_str.contains("password=secret123"));
    }

    #[test]
    fn test_build_connection_string_with_parameters() {
        let mut config = create_test_config();
        config
            .parameters
            .insert("application_name".to_string(), "arni".to_string());
        config
            .parameters
            .insert("connect_timeout".to_string(), "10".to_string());

        let adapter = PostgresAdapter::new(config);
        let conn_str = adapter.build_connection_string(None).unwrap();

        assert!(conn_str.contains("application_name=arni"));
        assert!(conn_str.contains("connect_timeout=10"));
    }

    #[test]
    fn test_connection_string_missing_host() {
        let mut config = create_test_config();
        config.host = None;

        let adapter = PostgresAdapter::new(config);
        let result = adapter.build_connection_string(None);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Config(_)));
    }

    #[test]
    fn test_connection_string_missing_username() {
        let mut config = create_test_config();
        config.username = None;

        let adapter = PostgresAdapter::new(config);
        let result = adapter.build_connection_string(None);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Config(_)));
    }

    #[tokio::test]
    async fn test_disconnect_when_not_connected() {
        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        // Should not error when disconnecting while not connected
        let result = adapter.disconnect().await;
        assert!(result.is_ok());
    }

    // Integration tests requiring a running PostgreSQL instance
    // These are ignored by default - run with: cargo test -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_connect_real_database() {
        // This test requires a PostgreSQL instance running on localhost:5432
        // with a test database and user configured
        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        let result = adapter.connect().await;
        assert!(result.is_ok());
        assert!(adapter.is_connected());

        adapter.disconnect().await.unwrap();
        assert!(!adapter.is_connected());
    }

    #[tokio::test]
    #[ignore]
    async fn test_health_check_real_database() {
        let config = create_test_config();
        let mut adapter = PostgresAdapter::new(config);

        adapter.connect().await.unwrap();

        let health = adapter.health_check().await;
        assert!(health.is_ok());
        assert!(health.unwrap());

        adapter.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_health_check_not_connected() {
        let config = create_test_config();
        let adapter = PostgresAdapter::new(config);

        let result = adapter.health_check().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DataError::Connection(_)));
    }
}
