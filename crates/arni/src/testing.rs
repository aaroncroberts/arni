//! Test utilities and helpers
//!
//! This module provides common utilities for testing, including mock implementations
//! and test data generators. Only available when testing.

#![cfg(test)]

use crate::{Connection, DataFrame, DbAdapter, Error, Result};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Mock connection for testing
#[derive(Debug, Clone)]
pub struct MockConnection {
    /// Connection state
    pub connected: Arc<Mutex<bool>>,
    /// Call log for verification
    pub call_log: Arc<Mutex<Vec<String>>>,
}

impl MockConnection {
    /// Create a new mock connection
    pub fn new() -> Self {
        Self {
            connected: Arc::new(Mutex::new(false)),
            call_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the call log
    pub fn get_calls(&self) -> Vec<String> {
        self.call_log.lock().unwrap().clone()
    }

    /// Clear the call log
    pub fn clear_calls(&self) {
        self.call_log.lock().unwrap().clear();
    }

    /// Record a call
    fn log_call(&self, call: &str) {
        self.call_log.lock().unwrap().push(call.to_string());
    }
}

impl Default for MockConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Connection for MockConnection {
    async fn connect(&mut self) -> Result<()> {
        self.log_call("connect");
        *self.connected.lock().unwrap() = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.log_call("disconnect");
        *self.connected.lock().unwrap() = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        *self.connected.lock().unwrap()
    }

    async fn health_check(&self) -> Result<bool> {
        self.log_call("health_check");
        Ok(self.is_connected())
    }
}

/// Mock database adapter for testing
#[derive(Debug, Clone)]
pub struct MockDbAdapter {
    /// Mock connection
    pub connection: MockConnection,
    /// Simulated query results
    pub query_results: Arc<Mutex<Vec<DataFrame>>>,
    /// Simulate errors on next operation
    pub next_error: Arc<Mutex<Option<Error>>>,
}

impl MockDbAdapter {
    /// Create a new mock adapter
    pub fn new() -> Self {
        Self {
            connection: MockConnection::new(),
            query_results: Arc::new(Mutex::new(Vec::new())),
            next_error: Arc::new(Mutex::new(None)),
        }
    }

    /// Set the next query result
    pub fn set_query_result(&self, df: DataFrame) {
        self.query_results.lock().unwrap().push(df);
    }

    /// Set the next operation to return an error
    pub fn set_next_error(&self, error: Error) {
        *self.next_error.lock().unwrap() = Some(error);
    }

    /// Check if there's a pending error
    fn check_error(&self) -> Result<()> {
        if let Some(err) = self.next_error.lock().unwrap().take() {
            return Err(err);
        }
        Ok(())
    }

    /// Get the next query result or return empty DataFrame
    fn get_query_result(&self) -> DataFrame {
        self.query_results.lock().unwrap().pop().unwrap_or_else(|| {
            // Return empty DataFrame
            use polars::prelude::*;
            let df = DataFrame::default();
            crate::DataFrame::from(df)
        })
    }
}

impl Default for MockDbAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Connection for MockDbAdapter {
    async fn connect(&mut self) -> Result<()> {
        self.connection.connect().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connection.disconnect().await
    }

    fn is_connected(&self) -> bool {
        self.connection.is_connected()
    }

    async fn health_check(&self) -> Result<bool> {
        self.connection.health_check().await
    }
}

#[async_trait]
impl DbAdapter for MockDbAdapter {
    async fn query(&self, _sql: &str) -> Result<DataFrame> {
        self.check_error()?;
        self.connection.log_call(&format!("query: {}", _sql));
        Ok(self.get_query_result())
    }

    async fn query_with_params(
        &self,
        _sql: &str,
        _params: &[&dyn std::any::Any],
    ) -> Result<DataFrame> {
        self.check_error()?;
        self.connection
            .log_call(&format!("query_with_params: {}", _sql));
        Ok(self.get_query_result())
    }

    async fn insert(&self, table: &str, _data: &DataFrame) -> Result<u64> {
        self.check_error()?;
        self.connection.log_call(&format!("insert: {}", table));
        Ok(1)
    }

    async fn update(&self, table: &str, _data: &DataFrame, condition: &str) -> Result<u64> {
        self.check_error()?;
        self.connection
            .log_call(&format!("update: {} where {}", table, condition));
        Ok(1)
    }

    async fn delete(&self, table: &str, condition: &str) -> Result<u64> {
        self.check_error()?;
        self.connection
            .log_call(&format!("delete: {} where {}", table, condition));
        Ok(1)
    }

    async fn list_tables(&self) -> Result<DataFrame> {
        self.check_error()?;
        self.connection.log_call("list_tables");
        Ok(self.get_query_result())
    }

    async fn describe_table(&self, table: &str) -> Result<DataFrame> {
        self.check_error()?;
        self.connection
            .log_call(&format!("describe_table: {}", table));
        Ok(self.get_query_result())
    }

    async fn list_columns(&self, table: &str) -> Result<DataFrame> {
        self.check_error()?;
        self.connection
            .log_call(&format!("list_columns: {}", table));
        Ok(self.get_query_result())
    }
}

/// Create a sample DataFrame for testing
pub fn create_test_dataframe() -> DataFrame {
    use polars::prelude::*;

    let df = df! {
        "id" => &[1, 2, 3],
        "name" => &["Alice", "Bob", "Charlie"],
        "age" => &[30, 25, 35],
    }
    .unwrap();

    crate::DataFrame::from(df)
}

/// Create an empty DataFrame for testing
pub fn create_empty_dataframe() -> DataFrame {
    let polars_df = polars::frame::DataFrame::default();
    crate::DataFrame::from(polars_df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_connection() {
        let mut conn = MockConnection::new();
        assert!(!conn.is_connected());

        conn.connect().await.unwrap();
        assert!(conn.is_connected());
        assert!(conn.get_calls().contains(&"connect".to_string()));

        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());
        assert!(conn.get_calls().contains(&"disconnect".to_string()));
    }

    #[tokio::test]
    async fn test_mock_adapter_query() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df);

        let _result = adapter.query("SELECT * FROM test").await.unwrap();
        assert!(adapter
            .connection
            .get_calls()
            .iter()
            .any(|c| c.contains("query")));
    }

    #[tokio::test]
    async fn test_mock_adapter_error() {
        let adapter = MockDbAdapter::new();
        adapter.set_next_error(Error::Query("Simulated error".to_string()));

        let result = adapter.query("SELECT * FROM test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_adapter_operations() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();

        adapter.insert("test_table", &test_df).await.unwrap();
        adapter
            .update("test_table", &test_df, "id = 1")
            .await
            .unwrap();
        adapter.delete("test_table", "id = 1").await.unwrap();

        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("insert")));
        assert!(calls.iter().any(|c| c.contains("update")));
        assert!(calls.iter().any(|c| c.contains("delete")));
    }

    #[test]
    fn test_create_test_dataframe() {
        let df = create_test_dataframe();
        // DataFrame should have data
        assert!(df.0.height() > 0);
    }

    #[test]
    fn test_create_empty_dataframe() {
        let df = create_empty_dataframe();
        assert_eq!(df.0.height(), 0);
    }
}
