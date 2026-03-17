/// Integration test: dropping the WorkerGuard flushes pending log lines to disk.
///
/// The non-blocking writer queues log events on an async channel and a background
/// thread drains it. Without dropping the guard (which joins the background thread),
/// log lines may still be buffered when we read the file. This test confirms the
/// drop-then-read pattern works reliably.
use arni_logging::RotationPolicy;

#[test]
fn dropping_guard_flushes_log_to_disk() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let guard = arni_logging::init_arni_logging(dir.path(), "debug", RotationPolicy::Never)
            .expect("init_arni_logging");

        tracing::debug!("flush-marker-debug");
        tracing::info!("flush-marker-info");

        // Explicit drop: this joins the background writer thread and flushes.
        drop(guard);
    }

    // After the guard is dropped, all buffered events must be on disk.
    let log_path = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("arni"))
        .expect("log file should exist after guard drop")
        .path();

    let content = std::fs::read_to_string(&log_path).expect("read log file");

    assert!(
        content.contains("flush-marker-debug"),
        "debug message should be flushed to disk; got:\n{content}"
    );
    assert!(
        content.contains("flush-marker-info"),
        "info message should be flushed to disk; got:\n{content}"
    );
}
