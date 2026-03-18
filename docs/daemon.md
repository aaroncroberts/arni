# arni Daemon — Protocol Specification & Setup Guide

The arni daemon is a long-running process that accepts newline-delimited JSON (NDJSON) commands over a Unix domain socket. It maintains a shared pool of database connections so that any number of clients can query any configured profile without re-establishing a connection on every request.

## Table of Contents

1. [Why a daemon?](#why-a-daemon)
2. [Starting the daemon](#starting-the-daemon)
3. [Protocol format](#protocol-format)
4. [Command reference](#command-reference)
   - [Core commands](#core-commands)
   - [Metadata commands](#metadata-commands)
   - [Bulk operation commands](#bulk-operation-commands)
   - [Utility commands](#utility-commands)
5. [Error handling](#error-handling)
6. [Connection lifecycle](#connection-lifecycle)
7. [Security considerations](#security-considerations)
8. [Logging](#logging)

---

## Why a daemon?

Opening a new database connection is expensive — typically 20–200 ms for TCP-based databases like PostgreSQL or MySQL. If every CLI invocation had to open a fresh connection, interactive workflows and scripts calling `arni` in a loop would be impractically slow.

The daemon solves this with a **connection registry**: the first client to use a profile pays the connection cost; every subsequent client reuses the live connection with zero overhead. The daemon also lets non-Rust processes (Node.js scripts, Cloudflare Workers via a bridge, Python tools) talk to any supported database without importing a Rust library.

```text
┌─────────────────────────────────────────────────────────────┐
│                       arni daemon                           │
│                                                             │
│   Unix socket ◄──── NDJSON ────► ConnectionRegistry        │
│   /tmp/arni.sock                  profile → SharedAdapter   │
│                                       │                     │
│                        ┌─────────────┼─────────────┐        │
│                        ▼             ▼             ▼        │
│                   PostgreSQL     DuckDB         SQLite      │
└─────────────────────────────────────────────────────────────┘
        ▲                ▲
        │                │
  Node.js client    arni CLI (arni query --profile mydb ...)
```

---

## Starting the daemon

```bash
# Start on the default socket path (/tmp/arni.sock)
arni daemon

# Start on a custom socket path
arni daemon --socket /var/run/arni/mydb.sock
```

The daemon prints the socket path to stdout and then stays in the foreground:

```
/tmp/arni.sock
```

Capture the path to use it from scripts:

```bash
SOCKET=$(arni daemon --socket /tmp/arni.sock &)
# or simply rely on the known default
```

### Stopping the daemon

Send a `shutdown` command (see below), or send `SIGTERM`/`SIGINT` to the process:

```bash
kill $(pgrep -f "arni daemon")
# or press Ctrl-C in the terminal running the daemon
```

On exit the daemon removes the socket file.

---

## Protocol format

Every message is a single-line JSON object terminated by `\n` (newline). The daemon responds with a single-line JSON object terminated by `\n`.

```
→ {"cmd":"query","profile":"mydb","sql":"SELECT 1"}\n
← {"ok":true,"columns":["?column?"],"rows":[[1]]}\n
```

**Rules:**
- One request → one response; responses are in strict request order per connection.
- Multiple clients may connect simultaneously; each gets independent request/response ordering.
- Empty lines from the client are ignored.
- If the daemon cannot parse a message, it responds `{"ok":false,"error":"..."}` and continues.

### Response envelope

All responses include `"ok": true | false` as the first field.

| Field | Type | Meaning |
|-------|------|---------|
| `ok` | `bool` | `true` on success, `false` on error |
| `error` | `string` | Present only when `ok` is `false` |

---

## Command reference

### Core commands

#### `connect`

Explicitly pre-warm a connection for a profile. Optional — all other commands connect lazily.

```json
{"cmd":"connect","profile":"mydb"}
```

Response:
```json
{"ok":true}
```

#### `disconnect`

Evict a profile's connection from the registry. The next command for this profile will reconnect.

```json
{"cmd":"disconnect","profile":"mydb"}
```

Response:
```json
{"ok":true}
```

#### `query`

Execute a SQL statement and return all rows.

```json
{"cmd":"query","profile":"mydb","sql":"SELECT id, name FROM users LIMIT 5"}
```

Response:
```json
{
  "ok": true,
  "columns": ["id", "name"],
  "rows": [[1, "Alice"], [2, "Bob"]]
}
```

Row values are JSON scalars: `null`, `true`/`false`, numbers, strings, or arrays of integers for binary columns.

#### `tables`

List all tables in the database (or the default schema).

```json
{"cmd":"tables","profile":"mydb"}
```

Response:
```json
{"ok":true,"tables":["users","orders","products"]}
```

#### `shutdown`

Stop the daemon. All connections are closed and the socket file is removed.

```json
{"cmd":"shutdown"}
```

Response:
```json
{"ok":true}
```

---

### Metadata commands

#### `describe_table`

Return column definitions and statistics for a table.

```json
{"cmd":"describe_table","profile":"mydb","table":"users","schema":null}
```

Response:
```json
{
  "ok": true,
  "table": "users",
  "columns": [
    {"name":"id","data_type":"int4","nullable":false,"default_value":null,"is_primary_key":true},
    {"name":"email","data_type":"varchar","nullable":false,"default_value":null,"is_primary_key":false}
  ],
  "row_count": 42000,
  "size_bytes": 1048576,
  "created_at": null
}
```

`schema` defaults to the database's default schema when `null`.

#### `list_databases`

List all databases or schemas visible to the connected user.

```json
{"cmd":"list_databases","profile":"mydb"}
```

Response:
```json
{"ok":true,"databases":["public","analytics","audit"]}
```

#### `get_indexes`

Return all indexes for a table.

```json
{"cmd":"get_indexes","profile":"mydb","table":"users","schema":null}
```

Response:
```json
{
  "ok": true,
  "table": "users",
  "indexes": [
    {
      "name": "users_pkey",
      "table_name": "users",
      "schema": "public",
      "columns": ["id"],
      "is_unique": true,
      "is_primary": true,
      "index_type": "btree"
    },
    {
      "name": "users_email_idx",
      "table_name": "users",
      "schema": "public",
      "columns": ["email"],
      "is_unique": true,
      "is_primary": false,
      "index_type": "btree"
    }
  ]
}
```

#### `get_foreign_keys`

Return all foreign keys defined on a table.

```json
{"cmd":"get_foreign_keys","profile":"mydb","table":"orders","schema":null}
```

Response:
```json
{
  "ok": true,
  "table": "orders",
  "foreign_keys": [
    {
      "name": "orders_user_id_fkey",
      "table_name": "orders",
      "schema": "public",
      "columns": ["user_id"],
      "referenced_table": "users",
      "referenced_schema": "public",
      "referenced_columns": ["id"],
      "on_delete": "CASCADE",
      "on_update": null
    }
  ]
}
```

#### `get_views`

List all views in a schema.

```json
{"cmd":"get_views","profile":"mydb","schema":null}
```

Response:
```json
{
  "ok": true,
  "views": [
    {"name":"active_users","schema":"public","definition":"SELECT * FROM users WHERE active = true"}
  ]
}
```

#### `get_server_info`

Return database server version and type.

```json
{"cmd":"get_server_info","profile":"mydb"}
```

Response:
```json
{
  "ok": true,
  "server": {
    "version": "16.1",
    "server_type": "PostgreSQL",
    "extra_info": {}
  }
}
```

#### `list_stored_procedures`

List stored procedures and functions in a schema.

```json
{"cmd":"list_stored_procedures","profile":"mydb","schema":null}
```

Response:
```json
{
  "ok": true,
  "procedures": [
    {"name":"get_user_stats","schema":"public","return_type":"TABLE","language":"plpgsql"}
  ]
}
```

#### `find_tables`

Search for tables whose names match a pattern.

```json
{"cmd":"find_tables","profile":"mydb","pattern":"user","mode":"contains","schema":null}
```

`mode` is one of `"contains"` (default), `"starts"`, `"ends"`.

Response:
```json
{
  "ok": true,
  "pattern": "user",
  "mode": "contains",
  "tables": ["users","user_roles","user_preferences"]
}
```

---

### Bulk operation commands

#### `bulk_insert`

Insert multiple rows into a table in a single batched operation.

```json
{
  "cmd": "bulk_insert",
  "profile": "mydb",
  "table": "users",
  "columns": ["name", "email", "active"],
  "rows": [
    ["Alice", "alice@example.com", true],
    ["Bob",   "bob@example.com",   true]
  ],
  "schema": null
}
```

Row values may be JSON `null`, booleans, numbers, or strings. They are mapped to `QueryValue` in the same order as `columns`.

Response:
```json
{"ok":true,"rows_affected":2}
```

#### `bulk_update`

Update rows matching a filter expression.

```json
{
  "cmd": "bulk_update",
  "profile": "mydb",
  "table": "users",
  "filter": {"active": {"eq": false}},
  "values": {"status": "inactive", "updated_at": "2026-01-01"},
  "schema": null
}
```

`filter` uses the [filter DSL](#filter-dsl). `values` is a flat JSON object of column → new value.

Response:
```json
{"ok":true,"rows_affected":7}
```

#### `bulk_delete`

Delete rows matching a filter expression.

```json
{
  "cmd": "bulk_delete",
  "profile": "mydb",
  "table": "users",
  "filter": {"id": {"in": [10, 11, 12]}},
  "schema": null
}
```

Response:
```json
{"ok":true,"rows_affected":3}
```

---

### Utility commands

#### `version`

Return the daemon protocol version and arni build version.

```json
{"cmd":"version"}
```

Response:
```json
{"ok":true,"protocol":"1.0","arni_version":"0.1.0"}
```

Use this as a health-check and compatibility probe before sending other commands.

---

## Filter DSL

The `filter` field in `bulk_update` and `bulk_delete` accepts a recursive JSON filter expression:

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
| `{"and": [expr, ...]}` | `(expr AND ...)` |
| `{"or": [expr, ...]}` | `(expr OR ...)` |
| `{"not": expr}` | `NOT expr` |

Filters compose arbitrarily:

```json
{
  "and": [
    {"active": {"eq": true}},
    {"or": [
      {"role": {"eq": "admin"}},
      {"score": {"gte": 100}}
    ]}
  ]
}
```

---

## Error handling

All errors return `{"ok":false,"error":"<message>"}`. Common error patterns:

| Scenario | Error message |
|----------|--------------|
| Unknown command | `"Invalid command: unknown variant \"xyz\""` |
| Profile not found | `"Profile 'xyz' not found. Available: ..."` |
| Connection failure | `"Connection failed: ..."` |
| SQL error | `"db error: ERROR: relation \"xyz\" does not exist"` |
| Filter parse error | `"Filter error: Unknown op 'xyz' for column 'id'"` |
| Row conversion error | `"Row conversion error: Cannot convert JSON value ..."` |

Clients should always check `ok` before reading other fields.

---

## Connection lifecycle

Connections are established lazily: the first command that names a profile causes `get_or_connect` to be called, which opens the connection and stores it in the registry. Subsequent commands for the same profile reuse the live connection with no I/O overhead.

```
First request for "mydb":
  get_or_connect("mydb") → opens connection → stores in registry → returns SharedAdapter

Subsequent requests for "mydb":
  get_or_connect("mydb") → finds existing SharedAdapter → returns it immediately
```

The `connect` command can be used to pre-warm a connection (e.g., at script startup) before any queries are sent. The `disconnect` command removes the entry from the registry; the next command will reconnect.

The daemon manages all connections across all clients. If client A and client B both use `"mydb"`, they share the same `SharedAdapter` — there is exactly one live connection per profile regardless of how many clients are connected.

---

## Security considerations

### Socket permissions

The Unix domain socket inherits the umask of the process that created it. By default this is typically `0600` (owner read/write only) on most systems. To restrict or relax access:

```bash
# Restrict to owner only (default on most systems)
umask 077 && arni daemon

# Allow group access (for shared-user setups)
umask 007 && arni daemon --socket /var/run/arni/arni.sock
```

Keep the socket in a directory with appropriate permissions. Do not place it in `/tmp` on multi-user systems if the queries or results are sensitive.

### Authentication

The daemon does not implement its own authentication layer — it delegates to the database's own authentication mechanism. Credentials are stored in `~/.arni/connections.yml` (or a custom config directory) and never transmitted over the socket. Any process that can reach the socket can issue any command for any configured profile.

In production, enforce access control at the filesystem level:

```bash
# Run daemon as a dedicated service user
sudo -u arni-svc arni daemon --socket /var/run/arni/arni.sock

# Only arni-svc and members of the arni group can connect
chown arni-svc:arni /var/run/arni/arni.sock
chmod 660 /var/run/arni/arni.sock
```

### Network exposure

The Unix socket is not network-accessible by default. To expose the daemon over the network (e.g., for Cloudflare Workers), use a dedicated HTTP bridge process that validates requests before forwarding to the socket. See [cloudflare.md](cloudflare.md) for a complete example.

Never bind the socket to a TCP port directly — the NDJSON protocol has no authentication or TLS.

### SQL injection

The `query` command passes SQL directly to the database driver. It is the caller's responsibility to sanitize inputs. For bulk operations (`bulk_insert`, `bulk_update`, `bulk_delete`), values are passed as structured data and quoted by the adapter — those paths are safe against injection.

---

## Logging

The daemon emits structured `tracing` events to the arni log file (default: `~/.arni/logs/arni.<date>`). Each command execution records:

```
INFO cmd=query profile=mydb duration_ms=3
INFO cmd=describe_table profile=mydb duration_ms=12
INFO cmd=bulk_insert profile=mydb duration_ms=45
```

The log level can be controlled with the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug arni daemon   # verbose: includes connection pool events
RUST_LOG=warn  arni daemon   # quiet: only warnings and errors
```

See [configuration.md](configuration.md) for log rotation and file location settings.
