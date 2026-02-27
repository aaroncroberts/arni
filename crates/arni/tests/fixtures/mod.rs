//! Test fixtures for integration tests
//!
//! This module provides test data fixtures and generators
//! for use in integration tests.

use polars::prelude::*;

/// Generate sample user data for testing
pub fn sample_users_dataframe() -> DataFrame {
    df! {
        "id" => [1i32, 2, 3, 4, 5],
        "name" => ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", "eve@example.com"],
        "age" => [25i32, 30, 35, 28, 42],
        "active" => [true, true, false, true, true],
    }
    .expect("Failed to create sample users dataframe")
}

/// Generate sample product data for testing
pub fn sample_products_dataframe() -> DataFrame {
    df! {
        "id" => [1i32, 2, 3],
        "name" => ["Widget", "Gadget", "Doohickey"],
        "price" => [19.99f64, 29.99, 39.99],
        "in_stock" => [true, false, true],
    }
    .expect("Failed to create sample products dataframe")
}

/// Generate sample order data for testing
pub fn sample_orders_dataframe() -> DataFrame {
    df! {
        "id" => [1i32, 2, 3],
        "user_id" => [1i32, 2, 1],
        "product_id" => [1i32, 2, 3],
        "quantity" => [2i32, 1, 3],
        "total" => [39.98f64, 29.99, 119.97],
    }
    .expect("Failed to create sample orders dataframe")
}

/// Generate an empty dataframe for testing
pub fn empty_dataframe() -> DataFrame {
    df! {
        "id" => Series::new_empty("id".into(), &DataType::Int32),
        "name" => Series::new_empty("name".into(), &DataType::String),
    }
    .expect("Failed to create empty dataframe")
}

/// SQL statements for creating test tables
pub struct TestSchema;

impl TestSchema {
    /// SQL to create users table
    pub fn create_users_table() -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            age INTEGER,
            active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#
    }

    /// SQL to create products table
    pub fn create_products_table() -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS test_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            in_stock BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#
    }

    /// SQL to create orders table
    pub fn create_orders_table() -> &'static str {
        r#"
        CREATE TABLE IF NOT EXISTS test_orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            total DECIMAL(10, 2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES test_users(id),
            FOREIGN KEY (product_id) REFERENCES test_products(id)
        )
        "#
    }

    /// SQL to drop all test tables
    pub fn drop_all_tables() -> Vec<&'static str> {
        vec![
            "DROP TABLE IF EXISTS test_orders CASCADE",
            "DROP TABLE IF EXISTS test_products CASCADE",
            "DROP TABLE IF EXISTS test_users CASCADE",
        ]
    }

    /// SQL to insert sample user data
    pub fn insert_sample_users() -> &'static str {
        r#"
        INSERT INTO test_users (name, email, age, active) VALUES
        ('Alice', 'alice@example.com', 25, true),
        ('Bob', 'bob@example.com', 30, true),
        ('Charlie', 'charlie@example.com', 35, false),
        ('Diana', 'diana@example.com', 28, true),
        ('Eve', 'eve@example.com', 42, true)
        ON CONFLICT (email) DO NOTHING
        "#
    }

    /// SQL to insert sample product data
    pub fn insert_sample_products() -> &'static str {
        r#"
        INSERT INTO test_products (name, price, in_stock) VALUES
        ('Widget', 19.99, true),
        ('Gadget', 29.99, false),
        ('Doohickey', 39.99, true)
        ON CONFLICT DO NOTHING
        "#
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_fixtures() {
        let users = sample_users_dataframe();
        assert_eq!(users.height(), 5);
        assert_eq!(users.width(), 5);

        let products = sample_products_dataframe();
        assert_eq!(products.height(), 3);
        assert_eq!(products.width(), 4);

        let orders = sample_orders_dataframe();
        assert_eq!(orders.height(), 3);
        assert_eq!(orders.width(), 5);
    }

    #[test]
    fn test_empty_fixture() {
        let df = empty_dataframe();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn test_schema_sql() {
        let create_users = TestSchema::create_users_table();
        assert!(create_users.contains("CREATE TABLE"));
        assert!(create_users.contains("test_users"));

        let drops = TestSchema::drop_all_tables();
        assert_eq!(drops.len(), 3);
    }
}
