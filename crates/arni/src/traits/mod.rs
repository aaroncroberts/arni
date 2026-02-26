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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{create_test_dataframe, MockConnection, MockDbAdapter};

    #[tokio::test]
    async fn test_connection_trait() {
        let mut conn = MockConnection::new();
        
        // Initially not connected
        assert!(!conn.is_connected());
        
        // Connect
        conn.connect().await.unwrap();
        assert!(conn.is_connected());
        
        // Health check when connected
        let health = conn.health_check().await.unwrap();
        assert!(health);
        
        // Disconnect
        conn.disconnect().await.unwrap();
        assert!(!conn.is_connected());
    }

    #[tokio::test]
    async fn test_dbadapter_query() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df.clone());
        
        let result = adapter.query("SELECT * FROM users").await;
        assert!(result.is_ok());
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("query")));
    }

    #[tokio::test]
    async fn test_dbadapter_query_with_params() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df);
        
        let params: Vec<&dyn std::any::Any> = vec![&1, &"test"];
        let result = adapter
            .query_with_params("SELECT * FROM users WHERE id = ?", &params)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dbadapter_insert() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        
        let rows = adapter.insert("users", &test_df).await.unwrap();
        assert_eq!(rows, 1);
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("insert")));
    }

    #[tokio::test]
    async fn test_dbadapter_update() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        
        let rows = adapter
            .update("users", &test_df, "id = 1")
            .await
            .unwrap();
        assert_eq!(rows, 1);
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("update")));
    }

    #[tokio::test]
    async fn test_dbadapter_delete() {
        let adapter = MockDbAdapter::new();
        
        let rows = adapter.delete("users", "id = 1").await.unwrap();
        assert_eq!(rows, 1);
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("delete")));
    }

    #[tokio::test]
    async fn test_dbadapter_list_tables() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df);
        
        let result = adapter.list_tables().await;
        assert!(result.is_ok());
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("list_tables")));
    }

    #[tokio::test]
    async fn test_dbadapter_describe_table() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df);
        
        let result = adapter.describe_table("users").await;
        assert!(result.is_ok());
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("describe_table")));
    }

    #[tokio::test]
    async fn test_dbadapter_list_columns() {
        let adapter = MockDbAdapter::new();
        let test_df = create_test_dataframe();
        adapter.set_query_result(test_df);
        
        let result = adapter.list_columns("users").await;
        assert!(result.is_ok());
        
        let calls = adapter.connection.get_calls();
        assert!(calls.iter().any(|c| c.contains("list_columns")));
    }

    #[tokio::test]
    async fn test_traits_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockConnection>();
        assert_send_sync::<MockDbAdapter>();
    }
}
