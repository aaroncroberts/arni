/// Dual output example (console + file)
///
/// Run with: cargo run --example dual_output
use arni_logging::LoggingConfig;
use tracing::{debug, info, warn};

fn main() {
    // Configure both console (INFO) and file (DEBUG)
    LoggingConfig::builder()
        .with_console_filter("info")
        .with_file_filter("debug")
        .with_console_compact()
        .with_file_json()
        .with_file_directory("./example-logs")
        .with_file_prefix("dual-demo")
        .build()
        .expect("Invalid configuration")
        .apply()
        .expect("Failed to initialize logging");

    debug!("This only goes to file (DEBUG < INFO for console)");
    info!("This goes to both console and file");
    warn!("This also goes to both");

    // Structured data is preserved in JSON file
    info!(
        user_id = 789,
        action = "purchase",
        amount = 99.99,
        "User transaction"
    );

    println!("\n✓ Console output shown above");
    println!("✓ Full logs in ./example-logs/dual-demo.log (JSON format)");
}
