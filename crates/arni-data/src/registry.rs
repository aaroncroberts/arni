//! Thread-safe connection registry for sharing adapter instances across async tasks.
//!
//! [`ConnectionRegistry`] maps string keys (typically profile names) to live
//! [`SharedAdapter`] instances. Adapters are created lazily and reused for
//! subsequent lookups, avoiding reconnection overhead on every command.
//!
//! # Concurrency guarantee
//!
//! If multiple tasks call [`get_or_connect`](ConnectionRegistry::get_or_connect)
//! for the same key simultaneously before that key is cached, only **one** factory
//! call is made — all other callers await its result. This prevents duplicate
//! connections ("thundering herd") under high concurrency.
//!
//! # Example
//!
//! ```ignore
//! use arni_data::registry::ConnectionRegistry;
//! use std::sync::Arc;
//!
//! let registry = Arc::new(ConnectionRegistry::new());
//!
//! // First call establishes the connection:
//! let adapter = registry.get_or_connect("prod", || async {
//!     let mut a = create_postgres_adapter(config);
//!     a.connect(...).await?;
//!     Ok(Arc::new(a) as SharedAdapter)
//! }).await?;
//!
//! // Subsequent calls return the cached Arc without reconnecting:
//! let same = registry.get_or_connect("prod", || async { unreachable!() }).await?;
//! assert!(Arc::ptr_eq(&adapter, &same));
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

use crate::{DataError, SharedAdapter};

/// One slot per registry key. `OnceCell` ensures at most one initialisation.
type Slot = Arc<OnceCell<SharedAdapter>>;

/// Thread-safe registry that maps profile keys to live, shared adapter instances.
///
/// Adapters are created lazily on first access and reused for subsequent requests.
/// See the [module documentation](self) for usage examples and concurrency details.
#[derive(Default)]
pub struct ConnectionRegistry {
    slots: Mutex<HashMap<String, Slot>>,
}

impl ConnectionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// Return the cached adapter for `key`, or call `factory` to create one.
    ///
    /// If multiple tasks call this method for the same `key` concurrently before
    /// it is cached, only the first `factory` invocation runs. All others wait
    /// for that result and then receive a clone of the same [`SharedAdapter`].
    ///
    /// # Errors
    ///
    /// Propagates any error returned by `factory`.
    pub async fn get_or_connect<F, Fut>(
        &self,
        key: &str,
        factory: F,
    ) -> Result<SharedAdapter, DataError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<SharedAdapter, DataError>>,
    {
        // Lock only long enough to get or create the OnceCell for this key.
        // This is a std::sync::Mutex (not async), so it must never be held
        // across an `.await`.
        let slot: Slot = {
            let mut slots = self.slots.lock().expect("registry mutex poisoned");
            Arc::clone(
                slots
                    .entry(key.to_string())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };

        // OnceCell::get_or_try_init ensures that even if many tasks race here
        // for the same slot, `factory` is called exactly once.
        slot.get_or_try_init(factory).await.map(Arc::clone)
    }

    /// Remove `key` from the registry.
    ///
    /// The underlying adapter (and its connection pool) is closed when all
    /// outstanding [`SharedAdapter`] clones are dropped.
    ///
    /// After eviction, the next call to [`get_or_connect`] for `key` will
    /// establish a fresh connection.
    pub fn evict(&self, key: &str) {
        let _ = self
            .slots
            .lock()
            .expect("registry mutex poisoned")
            .remove(key);
    }

    /// Return a snapshot of keys whose adapters have been successfully initialised.
    pub fn active_profiles(&self) -> Vec<String> {
        self.slots
            .lock()
            .expect("registry mutex poisoned")
            .iter()
            .filter(|(_, slot)| slot.initialized())
            .map(|(k, _)| k.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    /// Minimal adapter stub — only implements required methods; uses defaults for the rest.
    struct StubAdapter {
        connects: StdArc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::adapter::Connection for StubAdapter {
        async fn connect(&mut self) -> Result<(), DataError> {
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), DataError> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            true
        }
        async fn health_check(&self) -> Result<bool, DataError> {
            Ok(true)
        }
        fn config(&self) -> &crate::adapter::ConnectionConfig {
            unimplemented!()
        }
    }

    #[async_trait::async_trait]
    impl crate::DbAdapter for StubAdapter {
        async fn connect(
            &mut self,
            _c: &crate::ConnectionConfig,
            _p: Option<&str>,
        ) -> Result<(), DataError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), DataError> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            true
        }
        async fn test_connection(
            &self,
            _c: &crate::ConnectionConfig,
            _p: Option<&str>,
        ) -> Result<bool, DataError> {
            Ok(true)
        }
        fn database_type(&self) -> crate::DatabaseType {
            crate::DatabaseType::SQLite
        }
        fn metadata(&self) -> crate::adapter::AdapterMetadata<'_> {
            unimplemented!()
        }
        async fn export_dataframe(
            &self,
            _df: &polars::prelude::DataFrame,
            _t: &str,
            _s: Option<&str>,
            _r: bool,
        ) -> Result<u64, DataError> {
            unimplemented!()
        }
        async fn execute_query(&self, _q: &str) -> Result<crate::QueryResult, DataError> {
            unimplemented!()
        }
        async fn list_databases(&self) -> Result<Vec<String>, DataError> {
            Ok(vec![])
        }
        async fn list_tables(&self, _s: Option<&str>) -> Result<Vec<String>, DataError> {
            Ok(vec![])
        }
        async fn describe_table(
            &self,
            _t: &str,
            _s: Option<&str>,
        ) -> Result<crate::TableInfo, DataError> {
            unimplemented!()
        }
    }

    fn stub_config() -> crate::ConnectionConfig {
        crate::ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: crate::DatabaseType::SQLite,
            host: None,
            port: None,
            database: ":memory:".into(),
            username: None,
            use_ssl: false,
            parameters: Default::default(),
            pool_config: None,
        }
    }

    async fn make_shared(counter: StdArc<AtomicUsize>) -> Result<SharedAdapter, DataError> {
        let mut a = StubAdapter { connects: counter };
        crate::DbAdapter::connect(&mut a, &stub_config(), None).await?;
        Ok(Arc::new(a) as SharedAdapter)
    }

    #[tokio::test]
    async fn cached_on_second_call() {
        let reg = ConnectionRegistry::new();
        let counter = StdArc::new(AtomicUsize::new(0));

        let c1 = counter.clone();
        let a1 = reg
            .get_or_connect("test", || make_shared(c1))
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1, "factory called once");

        let a2 = reg
            .get_or_connect("test", || async {
                panic!("must not call factory on cache hit")
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&a1, &a2), "same Arc returned");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "still only 1 connect");
    }

    #[tokio::test]
    async fn concurrent_get_or_connect_calls_factory_once() {
        let reg = StdArc::new(ConnectionRegistry::new());
        let counter = StdArc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let reg = reg.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    reg.get_or_connect("shared", move || {
                        let c = counter.clone();
                        async move {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            make_shared(c).await
                        }
                    })
                    .await
                    .unwrap()
                })
            })
            .collect();

        let results: Vec<_> = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "only 1 connection made across 20 tasks"
        );
        let first = &results[0];
        for a in &results[1..] {
            assert!(
                Arc::ptr_eq(first, a),
                "all tasks share the same adapter Arc"
            );
        }
    }

    #[tokio::test]
    async fn evict_removes_profile() {
        let reg = ConnectionRegistry::new();
        let counter = StdArc::new(AtomicUsize::new(0));

        let c1 = counter.clone();
        reg.get_or_connect("key", || make_shared(c1)).await.unwrap();
        assert_eq!(reg.active_profiles(), vec!["key"]);

        reg.evict("key");
        assert!(reg.active_profiles().is_empty());

        let c2 = counter.clone();
        reg.get_or_connect("key", || make_shared(c2)).await.unwrap();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "re-connected after eviction"
        );
    }
}
