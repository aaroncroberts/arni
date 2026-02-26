/// Basic console logging example
///
/// Run with: cargo run --example console_basic
use arni_logging;
use tracing::{debug, error, info, warn};

fn main() {
    // Initialize with default settings (pretty console, INFO level)
    arni_logging::init_default();

    info!("Application started");
    warn!("This is a warning");
    error!("This is an error");
    debug!("This won't show (default level is INFO)");

    // Structured logging with fields
    info!(user_id = 123, action = "login", "User logged in");

    // With spans for context
    let _span = tracing::info_span!("request", request_id = 456).entered();
    info!("Processing request");
    warn!("Request took longer than expected");
}
