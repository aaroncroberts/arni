# arni-mcp

MCP (Model Context Protocol) server for arni — exposes all 14 database operations as AI-callable tools over JSON-RPC 2.0 on stdin/stdout.

## Quick start

Register with **Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "arni": {
      "command": "arni",
      "args": ["mcp"]
    }
  }
}
```

Register with **Claude Code** (`.mcp.json` in project root, or `~/.claude.json` globally):

```json
{
  "mcpServers": {
    "arni": {
      "command": "arni",
      "args": ["mcp"]
    }
  }
}
```

Configure a connection in `~/.arni/config.yaml` and restart the agent — arni tools appear automatically.

## Tools exposed

| Tool | Description |
|------|-------------|
| `query` | Execute SQL SELECT; returns rows as JSON |
| `execute` | Execute DML/DDL; returns rows affected |
| `tables` | List all tables |
| `describe_table` | Column names, types, PK flag, row count |
| `list_databases` | Databases/schemas visible to the current user |
| `get_indexes` | Indexes for a table |
| `get_foreign_keys` | Foreign key constraints for a table |
| `get_views` | All views with SQL definitions |
| `get_server_info` | Database server type and version |
| `list_stored_procedures` | Stored procedures and functions |
| `find_tables` | Search tables by name pattern |
| `bulk_insert` | Insert multiple rows in one operation |
| `bulk_update` | Update rows matching a Filter DSL expression |
| `bulk_delete` | Delete rows matching a Filter DSL expression |

## Configuration

Connection profiles are read from `~/.arni/config.yaml` (or `./arni.yaml`). See [docs/configuration.md](../../docs/configuration.md) for the full schema.

```yaml
default_profile: dev

profiles:
  dev:
    connections:
      - id: dev-pg
        name: Dev Postgres
        db_type: postgres
        host: localhost
        port: 5432
        database: myapp
        username: postgres
        use_ssl: false
        parameters:
          password: "${POSTGRES_PASSWORD}"
```

## Full documentation

See [docs/mcp.md](../../docs/mcp.md) for:
- Architecture diagram
- Filter DSL reference (`bulk_update` / `bulk_delete`)
- MCP resource listing (`arni://profiles/{name}`)
- Logging and debugging
- Limitations vs the CLI

## Using as a library

```rust
use arni_mcp::ArniMcpServer;
use arni::{ArniConfig, ConnectionRegistry};
use std::sync::Arc;

let config = Arc::new(ArniConfig::load_from_default_paths()?);
let registry = Arc::new(ConnectionRegistry::new());
let server = ArniMcpServer::new(registry, config);

// Serve over stdin/stdout (blocks until the client disconnects)
server.serve_stdio().await?;
```

## Tests

```bash
# Unit + in-memory integration tests (no database required)
cargo test -p arni-mcp

# Live-database tool tests (requires running containers)
TEST_POSTGRES_AVAILABLE=true \
TEST_MYSQL_AVAILABLE=true \
cargo test -p arni-mcp --test live_db_tests
```

See [docs/testing.md](../../docs/testing.md) for the full test strategy.
