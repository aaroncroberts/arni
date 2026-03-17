/// Integration test: messages logged via tracing::info! appear in the log file
/// in human-readable text format (timestamp + level prefix, no raw JSON).
use arni_logging::RotationPolicy;

#[test]
fn logged_messages_appear_in_text_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let guard = arni_logging::init_arni_logging(dir.path(), "info", RotationPolicy::Never)
        .expect("init_arni_logging");

    tracing::info!("integration-marker-text-format");
    tracing::warn!(key = "value", "structured field test");

    // Flush by dropping the guard before reading the file.
    drop(guard);

    let log_path = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("arni"))
        .expect("log file")
        .path();

    let content = std::fs::read_to_string(&log_path).expect("read log file");

    // Text format includes the message verbatim.
    assert!(
        content.contains("integration-marker-text-format"),
        "log file should contain the info message; got:\n{content}"
    );

    // Text format includes the WARN level label.
    assert!(
        content.contains("WARN") || content.contains("warn"),
        "log file should contain WARN level; got:\n{content}"
    );

    // Text format is NOT raw JSON — it should not start every line with '{'.
    let first_non_empty = content.lines().find(|l| !l.trim().is_empty());
    assert!(
        first_non_empty
            .map(|l| !l.trim_start().starts_with('{'))
            .unwrap_or(true),
        "text format should not produce JSON lines; got:\n{content}"
    );
}
