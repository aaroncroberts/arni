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
            match common::load_test_config("mongo-dev") {
                Some(cfg) => cfg,
                None => {
                    println!(
                        "[SKIP] mongo-dev profile not found in ~/.arni/connections.yml or env"
                    );
                    return;
                }
            }
        }};
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

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
            .expect("health_check should succeed");
        assert!(healthy, "mongodb should be healthy after connect");
    }

    // ── Schema operations ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_list_tables_returns_collections() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let password = cfg.parameters.get("password").cloned();
        let mut adapter = MongoDbAdapter::new(cfg.clone());
        DbAdapter::connect(&mut adapter, &cfg, password.as_deref())
            .await
            .unwrap();

        // list_tables() returns collection names for MongoDB
        let collections = DbAdapter::list_tables(&adapter, None)
            .await
            .expect("list_tables should succeed for MongoDB");
        // list_tables returns collection names for MongoDB; may be empty
        let _ = collections;
    }

    // ── Error handling ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_mongodb_health_check_before_connect_returns_false() {
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);
        let healthy = ConnectionTrait::health_check(&adapter)
            .await
            .unwrap_or(false);
        assert!(!healthy, "health_check before connect should be false or error");
    }

    #[tokio::test]
    async fn test_mongodb_database_type() {
        use arni_data::adapter::DatabaseType;
        use arni_data::adapters::mongodb::MongoDbAdapter;

        let cfg = mongo_config!();
        let adapter = MongoDbAdapter::new(cfg);
        assert_eq!(DbAdapter::database_type(&adapter), DatabaseType::MongoDB);
    }
}
