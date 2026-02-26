//! Core traits for database adapters

use async_trait::async_trait;

use crate::{DataFrame, Result};

/// Connection trait for managing database connections
#[async_trait]
pub trait Connection: Send + Sync {
    /// Connect to the database
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from the database
    async fn disconnect(&mut self) -> Result<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;

    /// Perform a health check
    async fn health_check(&self) -> Result<bool>;
}

/// Database adapter trait - core interface for all adapters
#[async_trait]
pub trait DbAdapter: Connection {
    /// Execute a query and return results as a DataFrame
    async fn query(&self, sql: &str) -> Result<DataFrame>;

    /// Execute a query with parameters
    async fn query_with_params(&self, sql: &str, params: &[&dyn std::any::Any])
        -> Result<DataFrame>;

    /// Insert data
    async fn insert(&self, table: &str, data: &DataFrame) -> Result<u64>;

    /// Update data
    async fn update(&self, table: &str, data: &DataFrame, condition: &str) -> Result<u64>;

    /// Delete data
    async fn delete(&self, table: &str, condition: &str) -> Result<u64>;

    /// List all tables/collections
    async fn list_tables(&self) -> Result<DataFrame>;

    /// Describe table schema
    async fn describe_table(&self, table: &str) -> Result<DataFrame>;

    /// List columns for a table
    async fn list_columns(&self, table: &str) -> Result<DataFrame>;
}
