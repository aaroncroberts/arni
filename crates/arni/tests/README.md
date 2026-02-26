# Integration Tests

This directory contains integration tests for Arni database adapters.

## Overview

Integration tests verify that Arni works correctly with actual database systems. Unlike unit tests, these tests require running database instances and are conditionally enabled.

## Test Organization

```
tests/
└── integration/
    ├── mod.rs              # Main integration test module
    ├── common.rs           # Common test utilities
    ├── fixtures.rs         # Test data fixtures
    └── test_postgres.rs    # PostgreSQL integration tests
```

## Running Tests

### All Tests
```bash
# Run all tests (integration tests skipped if databases not available)
cargo test

# Or using make
make test
```

### Integration Tests Only
```bash
# Run integration tests
cargo test --test '*'

# Run with ignored tests (requires database setup)
cargo test --test '*' -- --ignored
```

### Specific Adapter Tests
```bash
# PostgreSQL tests
cargo test --test '*' postgres

# Future: MongoDB tests
cargo test --test '*' mongodb
```

## Database Configuration

Integration tests are disabled by default. Enable them by setting environment variables:

### PostgreSQL

Create a `.env.test` file in the project root:

```bash
TEST_POSTGRES_AVAILABLE=true
TEST_POSTGRES_HOST=localhost
TEST_POSTGRES_PORT=5432
TEST_POSTGRES_DB=arni_test
TEST_POSTGRES_USER=postgres
TEST_POSTGRES_PASSWORD=postgres
```

### MongoDB (Future)

```bash
TEST_MONGODB_AVAILABLE=true
TEST_MONGODB_HOST=localhost
TEST_MONGODB_PORT=27017
TEST_MONGODB_DB=arni_test
TEST_MONGODB_USER=mongo
TEST_MONGODB_PASSWORD=mongo
```

## Test Database Setup

### PostgreSQL with Docker

```bash
# Start PostgreSQL container
docker run --name arni-test-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=arni_test \
  -p 5432:5432 \
  -d postgres:15

# Enable tests
export TEST_POSTGRES_AVAILABLE=true

# Run integration tests
cargo test --test '*' -- --ignored
```

### PostgreSQL Locally

```sql
-- Create test database
CREATE DATABASE arni_test;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE arni_test TO postgres;
```

## Test Fixtures

The `fixtures` module provides sample data for testing:

- `sample_users_dataframe()` - 5 sample users
- `sample_products_dataframe()` - 3 sample products
- `sample_orders_dataframe()` - 3 sample orders
- `empty_dataframe()` - Empty DataFrame for edge cases

SQL schema creation:
- `TestSchema::create_users_table()`
- `TestSchema::create_products_table()`
- `TestSchema::create_orders_table()`
- `TestSchema::drop_all_tables()`

## Writing Integration Tests

### Basic Test Structure

```rust
#[test]
#[ignore] // Remove when database is available
fn test_my_query() {
    if !is_postgres_available() {
        eprintln!("Skipping: PostgreSQL not available");
        return;
    }

    // 1. Setup: Create connection and tables
    let config = TestDbConfig::postgres_from_env();
    // ... create connection ...

    // 2. Execute: Run your test
    // ... execute query ...

    // 3. Assert: Verify results
    // ... assertions ...

    // 4. Cleanup: Drop test tables
    // ... cleanup ...
}
```

### Using Fixtures

```rust
use crate::fixtures::*;

#[test]
fn test_with_fixtures() {
    // Get sample data
    let users = sample_users_dataframe();
    assert_eq!(users.height(), 5);

    // Get schema SQL
    let create_sql = TestSchema::create_users_table();
    // ... execute SQL ...
}
```

### Cleanup Utility

```rust
use crate::common::TestCleanup;

#[test]
fn test_with_cleanup() {
    let mut cleanup = TestCleanup::new();
    cleanup.register_table("test_table");

    // ... run test ...

    // Generate cleanup SQL
    let sql = cleanup.cleanup_sql();
    // ... execute cleanup ...
}
```

## CI/CD Integration

### GitHub Actions

```yaml
jobs:
  test:
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: arni_test
        ports:
          - 5432:5432

    steps:
      - name: Run integration tests
        env:
          TEST_POSTGRES_AVAILABLE: true
          TEST_POSTGRES_HOST: localhost
          TEST_POSTGRES_PORT: 5432
          TEST_POSTGRES_DB: arni_test
          TEST_POSTGRES_USER: postgres
          TEST_POSTGRES_PASSWORD: postgres
        run: cargo test --test '*' -- --ignored
```

## Best Practices

1. **Isolation**: Each test should be independent and clean up after itself
2. **Conditional**: Use availability checks to skip tests when databases aren't available
3. **Fixtures**: Use shared fixtures for consistent test data
4. **Cleanup**: Always cleanup test data, even on failure
5. **Documentation**: Document required environment variables and setup
6. **Ignored**: Mark database-dependent tests with `#[ignore]` by default

## Troubleshooting

### Tests Are Skipped

- Ensure `TEST_<DB>_AVAILABLE=true` is set
- Verify database is running and accessible
- Check connection parameters

### Connection Failures

- Verify database is running: `docker ps` or `systemctl status postgresql`
- Check host and port configuration
- Verify credentials and permissions
- Check firewall rules

### Schema Errors

- Ensure test database exists
- Verify user has CREATE/DROP privileges
- Check for conflicting table names

## Future Additions

As more adapters are added, create corresponding test modules:

- `test_mongodb.rs` - MongoDB integration tests
- `test_oracle.rs` - Oracle integration tests
- `test_sqlserver.rs` - SQL Server integration tests
- `test_duckdb.rs` - DuckDB integration tests
