/// Integration test: an invalid filter string returns Err, not a panic.
///
/// `init_arni_logging` and `init_default_with_filter` both parse the filter
/// string through `EnvFilter::try_new`. Invalid directives must produce a
/// descriptive `Err` so callers can handle them gracefully.
#[test]
fn invalid_filter_returns_err_not_panic() {
    let dir = tempfile::tempdir().expect("tempdir");

    // "!!!" is not a valid tracing filter directive.
    let result =
        arni_logging::init_arni_logging(dir.path(), "!!!", arni_logging::RotationPolicy::Never);

    assert!(
        result.is_err(),
        "expected Err for invalid filter '!!!', got Ok"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        !err_msg.is_empty(),
        "error message should not be empty for invalid filter"
    );
}

#[test]
fn init_default_with_filter_invalid_returns_err() {
    // This may succeed or fail depending on RUST_LOG; we care only that it doesn't panic.
    // If RUST_LOG is set to a valid level this returns Ok (RUST_LOG wins), which is fine.
    // If "!!!" is actually parsed (no RUST_LOG), it returns Err.
    let _ = arni_logging::init_default_with_filter("!!!");
    // Reaching here without a panic is the assertion.
}
