//! PostgreSQL integration tests
//!
//! Run with: cargo test --test postgres
//!
//! Configuration via environment variables:
//! - TEST_POSTGRES_AVAILABLE (default: false)
//! - TEST_POSTGRES_HOST (default: localhost)
//! - TEST_POSTGRES_PORT (default: 5432)
//! - TEST_POSTGRES_DB (default: arni_test)
//! - TEST_POSTGRES_USER (default: postgres)
//! - TEST_POSTGRES_PASSWORD (default: postgres)

mod common;
mod fixtures;

use common::*;
use fixtures::*;

#[test]
fn test_postgres_config() {
    let config = TestDbConfig::postgres_from_env();
    assert!(!config.host.is_empty());
    assert!(config.port > 0);
    assert!(!config.database.is_empty());

    let conn_str = config.postgres_connection_string();
    assert!(conn_str.starts_with("postgresql://"));
    assert!(conn_str.contains(&config.host));
    assert!(conn_str.contains(&config.database));
}

#[test]
fn test_connection_string_format() {
    let config = TestDbConfig {
        host: "testhost".to_string(),
        port: 5433,
        database: "testdb".to_string(),
        username: "testuser".to_string(),
        password: "testpass".to_string(),
    };

    let conn_str = config.postgres_connection_string();
    assert_eq!(
        conn_str,
        "postgresql://testuser:testpass@testhost:5433/testdb"
    );
}

#[test]
fn test_fixtures_available() {
    let users = sample_users_dataframe();
    assert_eq!(users.height(), 5);
    assert!(users.column("name").is_ok());
    assert!(users.column("email").is_ok());

    let products = sample_products_dataframe();
    assert_eq!(products.height(), 3);
    assert!(products.column("price").is_ok());

    let orders = sample_orders_dataframe();
    assert_eq!(orders.height(), 3);
    assert!(orders.column("user_id").is_ok());
}

#[test]
fn test_schema_sql() {
    let create_users = TestSchema::create_users_table();
    assert!(create_users.contains("CREATE TABLE"));
    assert!(create_users.contains("test_users"));
    assert!(create_users.contains("SERIAL PRIMARY KEY"));

    let create_products = TestSchema::create_products_table();
    assert!(create_products.contains("test_products"));

    let create_orders = TestSchema::create_orders_table();
    assert!(create_orders.contains("test_orders"));
    assert!(create_orders.contains("FOREIGN KEY"));
}

#[test]
fn test_cleanup_sql() {
    let drops = TestSchema::drop_all_tables();
    assert_eq!(drops.len(), 3);
    assert!(drops[0].contains("DROP TABLE"));
    assert!(drops[0].contains("test_orders"));
    assert!(drops[1].contains("test_products"));
    assert!(drops[2].contains("test_users"));
}

#[test]
fn test_cleanup_utility() {
    let mut cleanup = TestCleanup::new();
    cleanup.register_table("test_table_1");
    cleanup.register_table("test_table_2");

    let sql = cleanup.cleanup_sql();
    assert_eq!(sql.len(), 2);
    assert!(sql[0].contains("DROP TABLE IF EXISTS"));
}

#[test]
fn test_postgres_availability_check() {
    let available = is_postgres_available();
    if available {
        println!("PostgreSQL is configured as available");
    } else {
        println!("PostgreSQL is not available (set TEST_POSTGRES_AVAILABLE=true)");
    }
}

// NOTE: The following tests are disabled by default because they require
// a real PostgreSQL database connection. Enable them by setting
// TEST_POSTGRES_AVAILABLE=true in your environment and removing #[ignore].

#[test]
#[ignore]
fn test_postgres_connection() {
    if !is_postgres_available() {
        eprintln!("Skipping test: PostgreSQL not available");
        eprintln!("Set TEST_POSTGRES_AVAILABLE=true to enable");
        return;
    }

    // This is a placeholder for actual connection tests
    // When the PostgreSQL adapter is fully implemented, add:
    // 1. Create connection using TestDbConfig
    // 2. Test connect() method
    // 3. Test disconnect() method
    // 4. Test is_connected() method
    todo!("Implement when PostgreSQL adapter is complete");
}

#[test]
#[ignore]
fn test_postgres_query_execution() {
    if !is_postgres_available() {
        eprintln!("Skipping test: PostgreSQL not available");
        return;
    }

    // This is a placeholder for actual query tests
    // When the PostgreSQL adapter is fully implemented, add:
    // 1. Create test table using TestSchema
    // 2. Insert sample data using fixtures
    // 3. Execute SELECT query
    // 4. Verify DataFrame results
    // 5. Cleanup test data
    todo!("Implement when PostgreSQL adapter is complete");
}

#[test]
#[ignore]
fn test_postgres_transaction_support() {
    if !is_postgres_available() {
        eprintln!("Skipping test: PostgreSQL not available");
        return;
    }

    // This is a placeholder for transaction tests
    // When the PostgreSQL adapter is fully implemented, add:
    // 1. Begin transaction
    // 2. Execute multiple queries
    // 3. Test commit
    // 4. Test rollback
    // 5. Verify data state
    todo!("Implement when PostgreSQL adapter is complete");
}
