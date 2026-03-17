/// File logging with rotation example
///
/// Run with: cargo run --example file_rotation
use arni_logging::{LoggingConfig, RotationPolicy};
use tracing::{debug, info};

fn main() {
    // Configure daily-rotated file logging
    LoggingConfig::builder()
        .without_console() // File only
        .with_file_text()
        .with_file_directory("./example-logs")
        .with_file_prefix("rotation-demo")
        .with_rotation_policy(RotationPolicy::Daily)
        .build()
        .expect("Invalid configuration")
        .apply()
        .expect("Failed to initialize logging");

    info!("This message goes to example-logs/rotation-demo.log");
    debug!("Debug messages won't show (default level is INFO)");

    // With custom filter to show debug
    std::env::set_var("RUST_LOG", "debug");
    info!("Set RUST_LOG=debug to see debug messages");

    println!("✓ Logs written to ./example-logs/rotation-demo.log");
}
