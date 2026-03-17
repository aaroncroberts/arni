/// Integration test: init_arni_logging() creates a log file in the target directory.
///
/// Each integration test file runs in its own process, which is necessary because
/// `tracing-subscriber` only allows one global subscriber per process. Separate
/// files guarantee isolation.
use arni_logging::RotationPolicy;

#[test]
fn init_arni_logging_creates_log_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = arni_logging::init_arni_logging(dir.path(), "info", RotationPolicy::Never)
        .expect("init_arni_logging should succeed");

    tracing::info!("hello from file_creates_log test");

    // Drop the guard to flush the async writer before inspecting the directory.
    drop(_guard);

    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .collect();

    assert!(
        !entries.is_empty(),
        "expected at least one log file in {:?}, found none",
        dir.path()
    );

    // The file should have the "arni" prefix configured in init_arni_logging.
    let has_arni_file = entries
        .iter()
        .any(|e| e.file_name().to_string_lossy().starts_with("arni"));
    assert!(
        has_arni_file,
        "expected a file starting with 'arni', got: {:?}",
        entries.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );
}
