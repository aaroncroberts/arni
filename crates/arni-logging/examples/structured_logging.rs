/// Structured logging with JSON output example
///
/// Run with: cargo run --example structured_logging
use arni_logging::LoggingConfig;
use tracing::{info, instrument};

#[instrument]
fn process_order(order_id: u64, user_id: u64, amount: f64) {
    info!(status = "processing", "Order received");

    // Simulate processing
    std::thread::sleep(std::time::Duration::from_millis(100));

    info!(status = "completed", "Order processed");
}

fn main() {
    // JSON Lines format for log aggregation tools
    LoggingConfig::builder()
        .with_file_json()
        .with_file_directory("./example-logs")
        .with_file_prefix("structured-demo")
        .build()
        .expect("Invalid configuration")
        .apply()
        .expect("Failed to initialize logging");

    info!(
        app = "rusty-app",
        version = env!("CARGO_PKG_VERSION"),
        "Application started"
    );

    process_order(12345, 789, 99.99);
    process_order(12346, 790, 149.50);

    info!("Application shutdown");

    println!("✓ Structured logs written to ./example-logs/structured-demo.log");
    println!("  Each line is valid JSON for easy parsing");
}
