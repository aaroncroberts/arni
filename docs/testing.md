# Testing Patterns and Guidelines

This document describes the testing patterns and conventions used in the Arni project.

## Test Organization

### Unit Tests

Unit tests are placed inline with the source code using `#[cfg(test)]` modules:

```rust
// src/my_module.rs
pub fn my_function() -> i32 {
    42
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_function() {
        assert_eq!(my_function(), 42);
    }
}
```

### Integration Tests

Integration tests are placed in `tests/integration/` and test the library as a whole:

```rust
// tests/integration/test_postgres.rs
use arni::PostgresAdapter;

#[tokio::test]
async fn test_postgres_connection() {
    // Test implementation
}
```

## Test Helpers and Mocks

The `testing` module (`src/testing.rs`) provides utilities for testing:

### MockConnection

A mock implementation of the `Connection` trait that tracks method calls:

```rust
use arni::testing::MockConnection;

#[tokio::test]
async fn test_with_mock_connection() {
    let mut conn = MockConnection::new();
    
    conn.connect().await.unwrap();
    assert!(conn.is_connected());
    
    // Verify calls
    let calls = conn.get_calls();
    assert!(calls.contains(&"connect".to_string()));
}
```

### MockDbAdapter

A mock implementation of the `DbAdapter` trait with configurable behavior:

```rust
use arni::testing::{MockDbAdapter, create_test_dataframe};
use arni::Error;

#[tokio::test]
async fn test_with_mock_adapter() {
    let adapter = MockDbAdapter::new();
    
    // Set up test data
    let test_df = create_test_dataframe();
    adapter.set_query_result(test_df);
    
    // Test query
    let result = adapter.query("SELECT * FROM test").await.unwrap();
    
    // Verify behavior
    let calls = adapter.connection.get_calls();
    assert!(calls.iter().any(|c| c.contains("query")));
}
```

### Simulating Errors

The `MockDbAdapter` can be configured to return errors:

```rust
#[tokio::test]
async fn test_error_handling() {
    let adapter = MockDbAdapter::new();
    adapter.set_next_error(Error::Query("Connection lost".to_string()));
    
    let result = adapter.query("SELECT * FROM test").await;
    assert!(result.is_err());
}
```

### Test Data Helpers

Use the helper functions to create test DataFrames:

```rust
use arni::testing::{create_test_dataframe, create_empty_dataframe};

#[test]
fn test_with_dataframe() {
    let df = create_test_dataframe();
    assert!(df.inner().height() > 0);
    
    let empty = create_empty_dataframe();
    assert_eq!(empty.inner().height(), 0);
}
```

## Testing Patterns

### Testing Async Code

Use `tokio::test` for async tests:

```rust
#[tokio::test]
async fn test_async_operation() {
    let result = some_async_function().await;
    assert!(result.is_ok());
}
```

### Testing Error Cases

Always test both success and error paths:

```rust
#[test]
fn test_validation_success() {
    let result = validate_input("valid");
    assert!(result.is_ok());
}

#[test]
fn test_validation_failure() {
    let result = validate_input("");
    assert!(result.is_err());
    
    if let Err(Error::Config(msg)) = result {
        assert!(msg.contains("cannot be empty"));
    } else {
        panic!("Expected Config error");
    }
}
```

### Testing Trait Implementations

Use mock implementations to test trait behavior:

```rust
#[tokio::test]
async fn test_trait_implementation() {
    let mut adapter = MockDbAdapter::new();
    
    // Test Connection trait
    adapter.connect().await.unwrap();
    assert!(adapter.is_connected());
    
    // Test DbAdapter trait
    let df = create_test_dataframe();
    adapter.set_query_result(df);
    let result = adapter.query("SELECT 1").await.unwrap();
    assert!(result.inner().height() >= 0);
}
```

### Testing Send + Sync

Verify that types can be sent across thread boundaries:

```rust
#[test]
fn test_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MyType>();
}
```

### Testing Display Implementations

Test that display formatting works correctly:

```rust
#[test]
fn test_display() {
    let item = MyType::new();
    let display = format!("{}", item);
    assert!(display.contains("expected text"));
}
```

## Running Tests

### Run All Tests

```bash
cargo test
```

### Run Specific Test Module

```bash
cargo test --lib config
```

### Run With Output

```bash
cargo test -- --nocapture
```

### Run Integration Tests Only

```bash
cargo test --test '*'
```

### Run With Coverage

```bash
cargo tarpaulin --out Html
```

## Test Naming Conventions

- Test function names should start with `test_`
- Use descriptive names: `test_postgres_connection_with_invalid_host`
- Group related tests in modules: `mod postgres_tests { ... }`

## Best Practices

1. **Test One Thing**: Each test should verify one specific behavior
2. **Use Descriptive Names**: Test names should clearly indicate what they test
3. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification phases
4. **Test Edge Cases**: Always test boundary conditions and error cases
5. **Use Test Helpers**: DRY - use helper functions for common setup
6. **Mock External Dependencies**: Use mocks instead of real database connections
7. **Keep Tests Fast**: Unit tests should run quickly
8. **Make Tests Deterministic**: Tests should always produce the same result
9. **Clean Up Resources**: Ensure tests clean up after themselves
10. **Document Complex Tests**: Add comments explaining non-obvious test logic

## Example: Complete Test Module

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;

    // Helper function for common setup
    fn setup_test_adapter() -> MockDbAdapter {
        let adapter = MockDbAdapter::new();
        adapter.set_query_result(create_test_dataframe());
        adapter
    }

    #[tokio::test]
    async fn test_query_execution() {
        // Arrange
        let adapter = setup_test_adapter();
        
        // Act
        let result = adapter.query("SELECT * FROM users").await;
        
        // Assert
        assert!(result.is_ok());
        let df = result.unwrap();
        assert!(df.inner().height() > 0);
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Arrange
        let adapter = MockDbAdapter::new();
        adapter.set_next_error(Error::Query("Test error".to_string()));
        
        // Act
        let result = adapter.query("SELECT * FROM users").await;
        
        // Assert
        assert!(result.is_err());
        match result {
            Err(Error::Query(msg)) => assert_eq!(msg, "Test error"),
            _ => panic!("Expected Query error"),
        }
    }
}
```

## Continuous Integration

Tests are automatically run on every push via GitHub Actions. All tests must pass before merging.

See `.github/workflows/test.yml` for CI configuration.
