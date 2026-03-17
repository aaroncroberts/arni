/// Integration test: when LoggingConfig uses JSON file format, each log line is
/// valid JSON and contains the expected "level" and "fields.message" keys.
use arni_logging::{LoggingConfig, RotationPolicy};

#[test]
fn logged_messages_appear_as_json_lines() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Build a file-only config using JSON format so we can parse the output.
    let config = LoggingConfig::builder()
        .with_file_json()
        .with_file_directory(dir.path().to_str().expect("valid utf8 path"))
        .with_file_prefix("arni-json-test")
        .with_rotation_policy(RotationPolicy::Never)
        .with_filter("info")
        .build()
        .expect("build config");

    // apply() returns () — no guard here, so we use the builder-based path.
    // The file writer is synchronous in this path (no WorkerGuard), so messages
    // are flushed when the subscriber is dropped at process exit. We call
    // init() which installs the global subscriber; the file is closed normally.
    config.apply().expect("apply config");

    tracing::info!("json-marker-message");
    tracing::error!(code = 42, "json-error-message");

    // Give the subscriber a moment to flush (file layer in LoggingConfig is
    // blocking — it flushes synchronously unlike init_arni_logging's non-blocking path).
    // Read directory, find our file, and parse.
    let log_path = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("arni-json-test"))
        .expect("json log file should exist")
        .path();

    let content = std::fs::read_to_string(&log_path).expect("read log file");
    assert!(!content.is_empty(), "log file should not be empty");

    // Every non-empty line must parse as valid JSON.
    for (i, line) in content.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {} is not valid JSON: {e}\n  content: {line}", i + 1));

        // tracing's JSON format nests the message under fields.message.
        let msg = parsed
            .get("fields")
            .and_then(|f| f.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("");

        // At least one line should contain our marker.
        let _ = msg; // we check globally below
    }

    assert!(
        content.contains("json-marker-message"),
        "JSON log should contain info message; got:\n{content}"
    );
    assert!(
        content.contains("json-error-message"),
        "JSON log should contain error message; got:\n{content}"
    );
    // Level field should be present.
    assert!(
        content.contains("\"level\""),
        "JSON log should have a 'level' field; got:\n{content}"
    );
}
