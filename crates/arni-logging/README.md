# arni-logging

Centralized logging infrastructure for arni using the `tracing` ecosystem.

## Features

- **Structured Logging**: Key-value fields, spans, and events for rich context
- **Multiple Output Formats**:
  - Console: Pretty (colorized, development) or Compact (minimal, production)
  - File: Text (.log) or JSON Lines (.jsonl for structured logging)
- **Dual Output**: Simultaneous console + file logging with independent configuration
- **Independent Filtering**: Different log levels for console vs. file (e.g., INFO to console, DEBUG to file)
- **File Rotation**: Daily, hourly, minutely, or never
- **Environment Config**: Honors `RUST_LOG` environment variable
- **Zero-Cost Abstractions**: Built on `tracing`, no runtime overhead when disabled

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
arni-logging = { workspace = true }
```

### Simple Console Logging

```rust
use arni_logging;

fn main() {
    // Initialize with defaults (pretty console, INFO level)
    arni_logging::init_default();

    tracing::info!("Application started");
    tracing::debug!("This won't show (default is INFO)");
}
```

### Custom Console Format

```rust
use arni_logging::LoggingConfig;

fn main() {
    // Compact format for production
    LoggingConfig::builder()
        .with_console_compact()
        .build()
        .unwrap()
        .apply()
        .expect("Failed to initialize logging");

    tracing::info!("Compact output");
}
```

### File Logging with Rotation

```rust
use arni_logging::{LoggingConfig, RotationPolicy};

fn main() {
    // Daily-rotated text logs in ./logs directory
    LoggingConfig::builder()
        .with_file_text()
        .with_file_directory("./logs")
        .with_file_prefix("myapp")
        .with_rotation_policy(RotationPolicy::Daily)
        .build()
        .unwrap()
        .apply()
        .expect("Failed to initialize logging");

    tracing::info!("This goes to logs/myapp.log");
}
```

### JSON Lines for Structured Logging

```rust
use arni_logging::LoggingConfig;

fn main() {
    // JSON Lines format for log aggregation tools
    LoggingConfig::builder()
        .with_file_json()
        .with_file_directory("./logs")
        .build()
        .unwrap()
        .apply()
        .expect("Failed to initialize logging");

    tracing::info!(user_id = 123, action = "login", "User action");
    // Output: {"timestamp":"...","level":"INFO","fields":{"user_id":123,"action":"login"},"target":"...","message":"User action"}
}
```

### Dual Output (Console + File)

```rust
use arni_logging::LoggingConfig;

fn main() {
    // Pretty console + JSON file
    LoggingConfig::builder()
        .with_console_pretty()
        .with_file_json()
        .with_file_directory("./logs")
        .build()
        .unwrap()
        .apply()
        .expect("Failed to initialize logging");

    tracing::info!("Logged to both console and file");
}
```

### Independent Filtering

```rust
use arni_logging::LoggingConfig;

fn main() {
    // INFO to console (less noise), DEBUG to file (full details)
    LoggingConfig::builder()
        .with_console_filter("info")
        .with_file_filter("debug")
        .with_console_pretty()
        .with_file_text()
        .build()
        .unwrap()
        .apply()
        .expect("Failed to initialize logging");

    tracing::debug!("File only: detailed debugging info");
    tracing::info!("Both: important application event");
}
```

## Configuration Options

### Console Output

| Method | Description |
|--------|-------------|
| `with_console_pretty()` | Colorized, detailed output for development |
| `with_console_compact()` | Minimal output for production |
| `with_console_stdout()` | Write to stdout (default: stderr) |
| `with_console_stderr()` | Write to stderr (default) |
| `without_console()` | Disable console output |

### File Output

| Method | Description |
|--------|-------------|
| `with_file_text()` | Human-readable .log format |
| `with_file_json()` | JSON Lines .jsonl format |
| `with_file_directory(path)` | Log directory (default: "logs") |
| `with_file_prefix(prefix)` | File name prefix (default: "app") |
| `with_rotation_policy(policy)` | Daily, Hourly, Minutely, or Never |
| `without_file()` | Disable file output |

### Filtering

| Method | Description |
|--------|-------------|
| `with_filter(level)` | Global filter (e.g., "debug", "info") |
| `with_console_filter(level)` | Console-specific filter |
| `with_file_filter(level)` | File-specific filter |

Log levels (most to least verbose): `trace`, `debug`, `info`, `warn`, `error`

### Rotation Policies

```rust
use arni_logging::RotationPolicy;

// Daily rotation at midnight
RotationPolicy::Daily

// Hourly rotation
RotationPolicy::Hourly

// Minutely (for testing)
RotationPolicy::Minutely

// Single file, no rotation
RotationPolicy::Never
```

## Usage with `tracing`

Once initialized, use the `tracing` macros throughout your code:

```rust
use tracing::{trace, debug, info, warn, error};

info!("Simple message");
warn!(error_code = 404, "Request failed");

// Spans for request tracking
use tracing::instrument;

#[instrument]
fn process_request(request_id: u64) {
    info!("Processing request");
    // Logs will include request_id automatically
}
```

## Environment Variable Override

Set `RUST_LOG` to override configured filters:

```bash
RUST_LOG=debug cargo run
RUST_LOG=arni_data=trace,info cargo run  # Trace arni_data, INFO for rest
```

## Integration Example (arni-data)

```rust
// In your arni-data library or application
use arni_logging::LoggingConfig;

pub fn init_logging() -> Result<(), Box<dyn std::error::Error>> {
    LoggingConfig::builder()
        .with_console_compact()
        .with_console_filter("info")
        .with_file_json()
        .with_file_filter("debug")
        .with_file_directory("./data-logs")
        .with_file_prefix("arni-data")
        .build()?
        .apply()?;

    Ok(())
}

// In your code
fn connect_to_database() {
    tracing::info!("Connecting to database");
    tracing::debug!(connection_string = "...", "Connection details");
}
```

## Testing

Run tests:

```bash
cargo test --package arni-logging
```

Generate documentation:

```bash
cargo doc --package arni-logging --open
```

## Architecture

- Built on `tracing` and `tracing-subscriber` for zero-cost abstractions
- Uses `tracing-appender` for file rotation
- Supports per-layer filtering for independent console/file log levels
- Builder pattern for ergonomic configuration

## License

MIT
