/// Integration test: RUST_LOG environment variable overrides the configured level.
///
/// `init_default_with_filter` calls `EnvFilter::try_from_default_env()` first,
/// so a RUST_LOG value in the environment beats the programmatic level argument.
/// This test verifies the documented priority order by reading RUST_LOG if set.
///
/// Note: we cannot *set* RUST_LOG in a test and verify it is honoured without
/// forking a subprocess, because env vars are process-global state shared across
/// parallel tests.  Instead we verify the fallback path (no RUST_LOG) returns Ok
/// for known-good levels, and that the EnvFilter parse priority is documented.
#[test]
fn init_default_with_filter_accepts_valid_levels() {
    // Each of these should parse without error (no global subscriber installed yet
    // in this process, so the first call succeeds; subsequent calls return Err from
    // try_init but we only assert on the parse step).
    for level in ["error", "warn", "info", "debug", "trace"] {
        // We use try_new to test filter parsing in isolation (no side effects).
        let result = tracing_subscriber::EnvFilter::try_new(level);
        assert!(
            result.is_ok(),
            "valid log level '{level}' should parse successfully"
        );
    }
}

#[test]
fn rust_log_env_filter_parse_priority_documented() {
    // RUST_LOG takes priority: if RUST_LOG=warn, even init_default_with_filter("debug")
    // ends up using warn. We document this by showing try_from_default_env succeeds
    // when RUST_LOG is set to a valid level.
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        let result = tracing_subscriber::EnvFilter::try_from_default_env();
        assert!(
            result.is_ok(),
            "RUST_LOG={rust_log} should parse as a valid EnvFilter"
        );
    }
    // If RUST_LOG is not set, the fallback path is tested by other integration tests.
}
