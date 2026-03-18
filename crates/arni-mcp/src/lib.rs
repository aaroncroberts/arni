//! `arni-mcp` — MCP server that exposes all arni database operations as
//! AI-callable tools via the [rmcp](https://crates.io/crates/rmcp) 0.12 SDK.
//!
//! # Architecture
//!
//! ```text
//! MCP client (Claude / agent)
//!        │  JSON-RPC 2.0 over stdio
//!        ▼
//!  ArniMcpServer   ← #[tool_router] / #[tool_handler]
//!        │
//!  ConnectionRegistry  ← lazy-connects on first tool call
//!        │
//!  DbAdapter (Postgres / MySQL / SQLite / …)
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use arni_mcp::serve;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     serve().await
//! }
//! ```

pub mod db;
pub mod filter;
pub mod resources;
pub mod server;
pub mod types;

use std::sync::Arc;

use arni::{ArniConfig, ConnectionRegistry};
use rmcp::ServiceExt;
use tracing::info;

pub use server::ArniMcpServer;

/// Start the MCP server on stdin/stdout.
///
/// Loads `ArniConfig` from the default search paths (`~/.arni/config.yaml`,
/// `./arni.yaml`, etc.) then serves indefinitely, processing JSON-RPC 2.0
/// requests from the MCP client.
pub async fn serve() -> anyhow::Result<()> {
    arni_logging::init_default();

    let config = ArniConfig::load_from_default_paths()
        .unwrap_or_else(|_| ArniConfig::default());

    let registry = Arc::new(ConnectionRegistry::new());
    let server = ArniMcpServer::new(registry, Arc::new(config));

    info!(event = "mcp_server_start", transport = "stdio");

    server
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await
        .map_err(|e| anyhow::anyhow!("MCP server error: {}", e))?;

    Ok(())
}
