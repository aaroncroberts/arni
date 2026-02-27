//! MongoDB adapter integration tests.
//!
//! These tests require a running MongoDB instance. Locally, start containers
//! with `arni dev start`. In CI, the `integration-tests` job handles this.
//!
//! Set TEST_MONGODB_AVAILABLE=true to enable:
//! ```bash
//! export TEST_MONGODB_AVAILABLE=true
//! cargo test -p arni-data --features mongodb --test mongodb
//! ```

mod common;

#[cfg(feature = "mongodb")]
mod mongodb_tests {
    use super::common;
    use arni_data::adapter::{Connection as ConnectionTrait, DbAdapter};

    macro_rules! mongo_config {
        () => {{
            if common::skip_if_unavailable("mongodb") {
                return;
            }
            match common::load_test_config("mongodb-dev") {
                Some(cfg) => cfg,
                None => {
                    println!("[SKIP] mongodb-dev profile not found");
                    return;
                }
            }
        }};
    }

    // ── 1. Connection lifecycle ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_connect_and_disconnect() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("mongodb connect should succeed");

        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("mongodb disconnect should succeed");
    }

    #[tokio::test]
    async fn test_mongodb_disconnect_when_not_connected_is_ok() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let mut adapter = MongoDbAdapter::new(cfg);

        // Disconnect without ever connecting must be a no-op, not an error.
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("disconnect without connect should be a no-op");
    }

    #[tokio::test]
    async fn test_mongodb_reconnect_after_disconnect() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("first connect should succeed");
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("first disconnect should succeed");

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .expect("second connect should succeed");
        DbAdapter::disconnect(&mut adapter)
            .await
            .expect("second disconnect should succeed");
    }

    // ── 2. Health check ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_health_check_before_connect_returns_false() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        // health_check() before connect should return Ok(false) or Err; not panic.
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .unwrap_or(false);
        assert!(!healthy, "health_check before connect should be false");
    }

    #[tokio::test]
    async fn test_mongodb_health_check_after_connect() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .expect("health_check should succeed after connect");
        assert!(healthy, "mongodb should be healthy after connect");
    }

    #[tokio::test]
    async fn test_mongodb_health_check_after_disconnect_returns_false() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        DbAdapter::disconnect(&mut adapter).await.unwrap();

        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .unwrap_or(false);
        assert!(
            !healthy,
            "health_check after disconnect should be false or error"
        );
    }

    // ── 3. is_connected state ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_is_connected_initially_false() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);
        assert!(
            !DbAdapter::is_connected(&adapter),
            "adapter should not be connected before connect()"
        );
    }

    #[tokio::test]
    async fn test_mongodb_is_connected_state_transitions() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());

        assert!(
            !DbAdapter::is_connected(&adapter),
            "should not be connected before connect()"
        );

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();
        assert!(
            DbAdapter::is_connected(&adapter),
            "should be connected after connect()"
        );

        DbAdapter::disconnect(&mut adapter).await.unwrap();
        assert!(
            !DbAdapter::is_connected(&adapter),
            "should not be connected after disconnect()"
        );
    }

    // ── 4. database_type ─────────────────────────────────────────────────────

    #[test]
    fn test_mongodb_database_type_no_connection() {
        use arni_data::adapter::{ConnectionConfig, DatabaseType};
        use arni_data::adapters::mongodb::MongoDbAdapter;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "test-mongodb".to_string(),
            name: "Test MongoDB".to_string(),
            db_type: DatabaseType::MongoDB,
            host: Some("localhost".to_string()),
            port: Some(27017),
            database: "testdb".to_string(),
            username: None,
            use_ssl: false,
            parameters: HashMap::new(),
        };
        let adapter = MongoDbAdapter::new(config);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::MongoDB);
    }

    #[tokio::test]
    async fn test_mongodb_database_type_with_config_macro() {
        use arni_data::adapter::DatabaseType;
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::MongoDB);
    }

    // ── 5. list_databases ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_list_databases_after_connect() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        let databases = DbAdapter::list_databases(&adapter)
            .await
            .expect("list_databases should succeed");

        // A fresh MongoDB instance always has admin, config, local
        assert!(
            !databases.is_empty(),
            "list_databases should return at least one database"
        );

        // At minimum one of the system databases should be present
        let has_system_db = databases
            .iter()
            .any(|db| matches!(db.as_str(), "admin" | "config" | "local"));
        assert!(
            has_system_db,
            "list_databases should include at least one of admin/config/local; got: {:?}",
            databases
        );
    }

    #[tokio::test]
    async fn test_mongodb_list_databases_before_connect_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        let result = DbAdapter::list_databases(&adapter).await;
        assert!(
            result.is_err(),
            "list_databases without connect should return an error"
        );
    }

    // ── 6. list_tables (collections) ────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_list_tables_returns_collections() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // list_tables() returns collection names for MongoDB; may be empty on a fresh DB
        let collections = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed for MongoDB");
        // Result is a Vec<String>; no error is the key assertion here.
        let _ = collections;
    }

    #[tokio::test]
    async fn test_mongodb_list_tables_before_connect_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        let result = DbAdapter::list_tables(&adapter, None).await;
        assert!(
            result.is_err(),
            "list_tables without connect should return an error"
        );
    }

    #[tokio::test]
    async fn test_mongodb_list_tables_schema_param_ignored() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // MongoDB ignores the schema parameter (uses current_database instead)
        let result_no_schema = DbAdapter::list_tables(&adapter, None).await;
        let result_with_schema = DbAdapter::list_tables(&adapter, Some("ignored_schema")).await;

        assert!(result_no_schema.is_ok(), "list_tables(None) should succeed");
        assert!(
            result_with_schema.is_ok(),
            "list_tables(Some(...)) should succeed"
        );
    }

    // ── 7. execute_query ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_execute_query_before_connect_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        // Any query without connect must return an error
        let result =
            DbAdapter::execute_query(&adapter, r#"{"collection": "test", "filter": {}}"#).await;
        assert!(
            result.is_err(),
            "execute_query without connect should return an error"
        );
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_invalid_json_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Plain SQL is not valid JSON and must be rejected with a clear error
        let result = DbAdapter::execute_query(&adapter, "SELECT 1").await;
        assert!(
            result.is_err(),
            "non-JSON query string should return an error"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Invalid MongoDB query format")
                || err_msg.contains("invalid")
                || err_msg.contains("JSON"),
            "error message should describe the format problem; got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_missing_collection_field_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Valid JSON but missing the required "collection" field
        let result = DbAdapter::execute_query(&adapter, r#"{"filter": {"x": 1}}"#).await;
        assert!(
            result.is_err(),
            "JSON without 'collection' field should return an error"
        );
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_empty_collection_returns_empty_result() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Query a collection that almost certainly does not exist -> 0 rows, no error
        let result = DbAdapter::execute_query(
            &adapter,
            r#"{"collection": "arni_test_nonexistent_xyz_99", "filter": {}}"#,
        )
        .await
        .expect("query against non-existent collection should succeed with 0 rows");

        assert!(
            result.rows.is_empty(),
            "non-existent collection should return 0 rows"
        );
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_with_filter() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // A filter query against a non-existent collection should succeed with 0 rows
        let result = DbAdapter::execute_query(
            &adapter,
            r#"{"collection": "arni_test_nonexistent_xyz_99", "filter": {"x": 42}}"#,
        )
        .await
        .expect("filtered query against non-existent collection should succeed");

        assert!(
            result.rows.is_empty(),
            "should return 0 rows for empty collection"
        );
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_with_limit() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Limit field is supported in the query document
        let result = DbAdapter::execute_query(
            &adapter,
            r#"{"collection": "arni_test_nonexistent_xyz_99", "filter": {}, "limit": 5}"#,
        )
        .await
        .expect("query with limit field should succeed");

        // Non-existent collection always returns 0; limit doesn't cause an error
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn test_mongodb_execute_query_system_collection_invalid_returns_error() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // system.* collection names are rejected by validate_collection_name
        let result =
            DbAdapter::execute_query(&adapter, r#"{"collection": "system.users", "filter": {}}"#)
                .await;
        assert!(
            result.is_err(),
            "system.* collection name should be rejected"
        );
    }

    // ── 8. Unsupported metadata ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_get_views_returns_empty_vec() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // MongoDB adapter returns an empty Vec for views (not NotSupported)
        let views = DbAdapter::get_views(&adapter, None)
            .await
            .expect("get_views should return Ok(Vec) for MongoDB");
        assert!(
            views.is_empty(),
            "MongoDB get_views should return an empty list"
        );
    }

    #[tokio::test]
    async fn test_mongodb_get_foreign_keys_returns_empty_vec() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // MongoDB has no foreign keys; adapter returns empty Vec
        let fks = DbAdapter::get_foreign_keys(&adapter, "any_collection", None)
            .await
            .expect("get_foreign_keys should return Ok(Vec) for MongoDB");
        assert!(
            fks.is_empty(),
            "MongoDB get_foreign_keys should return an empty list"
        );
    }

    #[tokio::test]
    async fn test_mongodb_list_stored_procedures_returns_empty_vec() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // MongoDB has no traditional stored procedures; adapter returns empty Vec
        let procs = DbAdapter::list_stored_procedures(&adapter, None)
            .await
            .expect("list_stored_procedures should return Ok(Vec) for MongoDB");
        assert!(
            procs.is_empty(),
            "MongoDB list_stored_procedures should return an empty list"
        );
    }

    #[tokio::test]
    async fn test_mongodb_describe_table_on_empty_collection_returns_empty_columns() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // An empty collection produces a TableInfo with no columns (schema inference
        // requires at least one document to sample)
        let info = DbAdapter::describe_table(&adapter, "arni_test_nonexistent_xyz_99", None)
            .await
            .expect("describe_table on empty/nonexistent collection should succeed");

        assert_eq!(
            info.name, "arni_test_nonexistent_xyz_99",
            "TableInfo name should match the collection name"
        );
        // columns will be empty because there are no documents to sample
        assert!(
            info.columns.is_empty(),
            "describe_table on empty collection should return no columns"
        );
    }

    // ── 9. Error handling ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_execute_query_before_connect_error_message() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        let err = DbAdapter::execute_query(&adapter, r#"{"collection": "users", "filter": {}}"#)
            .await
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("Not connected")
                || msg.contains("not connected")
                || msg.contains("connect"),
            "error message should mention connection state; got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn test_mongodb_list_tables_before_connect_error_message() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);

        let err = DbAdapter::list_tables(&adapter, None).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Not connected")
                || msg.contains("not connected")
                || msg.contains("connect"),
            "error message should mention connection state; got: {}",
            msg
        );
    }

    // ── 10. Config accessor ──────────────────────────────────────────────────

    #[test]
    fn test_mongodb_config_accessor() {
        use arni_data::adapter::{ConnectionConfig, DatabaseType};
        use arni_data::adapters::mongodb::MongoDbAdapter;
        use std::collections::HashMap;

        let config = ConnectionConfig {
            id: "my-mongo".to_string(),
            name: "My MongoDB".to_string(),
            db_type: DatabaseType::MongoDB,
            host: Some("db.example.com".to_string()),
            port: Some(27017),
            database: "myapp".to_string(),
            username: Some("admin".to_string()),
            use_ssl: false,
            parameters: HashMap::new(),
        };
        let adapter = MongoDbAdapter::new(config.clone());

        let returned = ConnectionTrait::config(&adapter);
        assert_eq!(returned.id, "my-mongo");
        assert_eq!(returned.database, "myapp");
        assert_eq!(returned.host.as_deref(), Some("db.example.com"));
        assert_eq!(returned.port, Some(27017));
        assert_eq!(returned.db_type, DatabaseType::MongoDB);
    }

    #[tokio::test]
    async fn test_mongodb_config_preserved_after_connect() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let original_database = cfg.database.clone();
        let mut adapter = MongoDbAdapter::new(cfg.clone());

        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // Config should reflect the connected database
        let returned = ConnectionTrait::config(&adapter);
        assert_eq!(
            returned.database, original_database,
            "config database should be preserved after connect"
        );
        assert_eq!(
            returned.db_type,
            arni_data::adapter::DatabaseType::MongoDB,
            "config db_type should remain MongoDB"
        );
    }

    // ── Metadata: get_view_definition ────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_get_view_definition() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // MongoDB does not support SQL views; the adapter returns Ok(None) by design.
        let result = DbAdapter::get_view_definition(&adapter, "any_view", None)
            .await
            .expect("get_view_definition should return Ok for MongoDB");

        assert!(
            result.is_none(),
            "MongoDB get_view_definition should return None (not supported); got: {:?}",
            result
        );
    }
}
