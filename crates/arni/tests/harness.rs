//! Smoke tests that verify the integration test harness itself compiles
//! and that its public helpers behave correctly.
//!
//! Run with: `cargo test -p arni-data --test harness`

mod common;

/// Verify the public harness API is callable end-to-end.
#[test]
fn harness_smoke_test() {
    // Availability guard: returns false for an unknown db type.
    assert!(!common::is_adapter_available("__smoke_test_db__"));

    // Skip guard: returns true (skip) when unavailable.
    assert!(common::skip_if_unavailable("__smoke_test_db__"));

    // Config loader: returns None for a profile that is in neither
    // ~/.arni/connections.yml nor environment variables.
    assert!(common::load_test_config("__smoke_test_profile__").is_none());
}
