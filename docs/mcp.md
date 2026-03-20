# arni MCP Server

arni ships a first-class [Model Context Protocol](https://modelcontextprotocol.io) (MCP)
server so AI agents (Claude, GPT-4, etc.) can query and manage any configured database
profile without custom client code.

## Architecture

```
Claude / AI agent
     │  JSON-RPC 2.0 over stdio
     ▼
arni mcp  (arni-cli subcommand → arni-mcp crate)
     │
ConnectionRegistry  (lazy-connects on first tool call)
     │
DbAdapter  (Postgres / MySQL / SQLite / MongoDB / SQL Server / Oracle / DuckDB)
```

Communication uses the MCP wire format (JSON-RPC 2.0 over stdin/stdout).
The server process lives for the duration of the agent session and reuses
open database connections across tool calls.

## Quick start

### 1. Configure a connection profile

Profiles are stored in `~/.arni/config.yaml` (or `./arni.yaml`):

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
        database: myapp_dev
        username: postgres
        use_ssl: false
        parameters:
          password: "${POSTGRES_PASSWORD}"
```

See [configuration.md](configuration.md) for all options and supported databases.

### 2. Register with Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

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

Restart Claude Desktop. The arni tools appear automatically under the 🔌 icon.

### 3. Use from Claude

```
Can you list the tables in my dev database?
```

Claude will call `arni.tables(profile="dev")` and display the results.

---

## Available tools

| Tool | Description |
|------|-------------|
| `query` | Execute a SQL SELECT; returns columns + rows as JSON |
| `execute` | Execute DML/DDL (INSERT, UPDATE, DELETE, CREATE, …); returns rows affected |
| `tables` | List all tables in the database |
| `describe_table` | Column names, types, nullability, PK flag, row count |
| `list_databases` | Databases/schemas visible to the current user |
| `get_indexes` | All indexes for a table |
| `get_foreign_keys` | Foreign key constraints for a table |
| `get_views` | All views with SQL definitions |
| `get_server_info` | Database server type and version string |
| `list_stored_procedures` | Stored procedures and functions in a schema |
| `find_tables` | Search for tables by name pattern (contains/starts/ends) |
| `bulk_insert` | Insert multiple rows in a single batched operation |
| `bulk_update` | Update rows matching a Filter DSL expression |
| `bulk_delete` | Delete rows matching a Filter DSL expression |

---

## Filter DSL

`bulk_update` and `bulk_delete` accept a JSON filter expression:

| Shape | SQL equivalent |
|-------|---------------|
| `{"col": {"eq": value}}` | `col = value` |
| `{"col": {"ne": value}}` | `col <> value` |
| `{"col": {"gt": value}}` | `col > value` |
| `{"col": {"gte": value}}` | `col >= value` |
| `{"col": {"lt": value}}` | `col < value` |
| `{"col": {"lte": value}}` | `col <= value` |
| `{"col": {"in": [v1, v2]}}` | `col IN (v1, v2)` |
| `{"col": "is_null"}` | `col IS NULL` |
| `{"col": "is_not_null"}` | `col IS NOT NULL` |
| `{"and": [expr, …]}` | `(expr AND …)` |
| `{"or":  [expr, …]}` | `(expr OR …)` |
| `{"not": expr}` | `NOT expr` |

Examples:

```json
// Delete users with id in [1, 2, 3]
{"id": {"in": [1, 2, 3]}}

// Update active users older than 30
{"and": [{"active": {"eq": true}}, {"age": {"gt": 30}}]}
```

---

## MCP resources

Active connection profiles are also exposed as **MCP resources** under
`arni://profiles/{name}`. After a first successful tool call, the profile appears
in the resource list so agents can enumerate live connections without calling a tool.

---

## Running the server manually

```bash
# Start the MCP server on stdin/stdout
arni mcp

# Or directly via cargo (dev workflow)
cargo run -p arni-cli -- mcp
```

The server loads `~/.arni/config.yaml` (or `./arni.yaml`) at startup.
If no config is found it starts in an unconfigured state — tools that reference
a profile will return a clear error until a config file is created.

### Logging

The server writes structured `tracing` events to stderr. Set `RUST_LOG` to control
verbosity:

```bash
RUST_LOG=arni_mcp=debug arni mcp
```

Each tool call emits:

```
INFO arni_mcp::server  tool=query profile=dev duration_ms=12 rows=42
```

---

## Passwords and secrets

The MCP server runs non-interactively — there is no TTY for password prompts.
Store passwords in the connection config or inject via environment variables:

```yaml
parameters:
  password: "${DB_PASSWORD}"   # substituted at startup
```

Do **not** hard-code passwords in `config.yaml` if the file is committed to version
control. Use environment variable substitution (`${VAR}`) instead.

---

## Real-world examples

These examples show what an arni-connected Claude session actually looks like — the
user prompt, the tool calls Claude makes, and the outcome.

### Schema-aware code generation

> **You:** Write me a Rust struct and sqlx query to fetch all active users with their most
> recent order date.

Claude calls `describe_table("users")`, `describe_table("orders")`, and
`get_foreign_keys("orders")` before writing a single line. It sees the real column names,
types, nullable flags, and the FK relationship — so the struct fields, the JOIN condition,
and the `Option<>` wrappers match your actual schema rather than a generic template.

**Tools invoked:** `describe_table`, `get_foreign_keys`

---

### Incident investigation

> **You:** We're seeing failed checkouts in staging. Can you look at the orders table and
> find patterns in rows where status = 'failed' in the last 6 hours?

Claude calls `query` with a grouping query over `orders`, notices a cluster of failures
tied to a specific `payment_provider` value, then follows up with a second query correlating
those rows against `users` to check whether it's isolated to a particular account tier.
It surfaces the pattern and suggests the next diagnostic step — without you writing a
single SQL statement.

**Tools invoked:** `query` (multiple), `describe_table`, `get_foreign_keys`

---

### Migration drafting

> **You:** I want to add soft-delete to the users table. What migration do I need and what
> queries in the codebase should I update?

Claude calls `describe_table("users")` to see existing columns, `get_indexes("users")` to
check what indexes exist, and `get_foreign_keys("users")` to find dependent tables. It
produces a migration that adds `deleted_at TIMESTAMPTZ`, a partial index on `WHERE deleted_at IS NULL`,
and notes which FK-referencing tables may need `WHERE deleted_at IS NULL` guards added to
their queries — all grounded in your real schema.

**Tools invoked:** `describe_table`, `get_indexes`, `get_foreign_keys`

---

### Cross-database comparison

> **You:** Are there any tables in staging that don't exist in dev? Do the schemas match
> for the users and orders tables?

With two profiles registered (`dev` and `staging`), Claude calls `tables(profile="dev")`
and `tables(profile="staging")`, diffs the lists, then calls `describe_table` for each
divergent or shared table to surface column-level drift — missing columns, type mismatches,
nullable differences. Useful before a deploy or after an out-of-band schema change.

**Tools invoked:** `tables` (×2 profiles), `describe_table` (×multiple)

---

### Bulk data cleanup

> **You:** Remove all sessions that expired more than 30 days ago. Show me what the filter
> looks like before you run it.

Claude constructs the `bulk_delete` filter expression, explains it in plain English, and
waits for confirmation before executing. After deletion it reports the row count.

**Tools invoked:** `describe_table`, `bulk_delete`

---

See [use-cases.md](use-cases.md) for the full use-case reference across the library, MCP, and CLI.

---

## Limitations vs the CLI

| Feature | CLI | MCP |
|---------|-----|-----|
| DataFrame export (CSV/Parquet/JSON) | ✅ | via `query` + agent processing |
| Password prompting (TTY) | ✅ | ❌ — use config parameters |
| Progress bars / rich output | ✅ | ❌ — JSON only |
| Streaming large result sets | ❌ | ❌ — full result in memory |
| Bulk operations | ✅ | ✅ |
| Schema introspection | ✅ | ✅ |
