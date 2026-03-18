# Configuration Reference

Arni loads database connections from a YAML or TOML configuration file. The file uses a
**two-level hierarchy**: top-level *profiles* group related connections (e.g. dev, staging,
production), and each profile holds one or more *connections*.

---

## File locations

Arni searches the following paths in order and loads the first one it finds:

| Priority | Path |
| :---: | :--- |
| 1 | `~/.arni/config.yaml` |
| 2 | `~/.arni/config.toml` |
| 3 | `./arni.yaml` |
| 4 | `./arni.toml` |
| 5 | `./.arni.yaml` |
| 6 | `./.arni.toml` |

The CLI and MCP server both call `ArniConfig::load_from_default_paths()` at startup. If no
file is found the process starts in an unconfigured state — tool calls that reference a
profile will return a clear error.

You can also point to an explicit file:

```bash
ARNI_CONFIG=/etc/arni/prod.yaml arni query prod-pg --sql "SELECT 1"
```

---

## Schema

### Top-level structure

```yaml
# Optional. Which profile to use when none is specified.
# Defaults to "default".
default_profile: dev

profiles:
  dev:        # profile name — use any string
    connections:
      - ...   # one or more ConnectionConfig entries
  prod:
    connections:
      - ...
```

```toml
default_profile = "dev"

[profiles.dev]
[[profiles.dev.connections]]
# ConnectionConfig fields …
```

### `ConnectionConfig` fields

Every entry in a `connections` array is a `ConnectionConfig`:

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `id` | string | ✅ | Unique identifier within the profile. Used to reference this connection from the CLI and MCP tools. |
| `name` | string | ✅ | Human-readable label shown in listings. |
| `db_type` | string | ✅ | Database type. See [supported values](#db_type-values). |
| `host` | string | server DBs | Hostname or IP. Required for PostgreSQL, MySQL, MongoDB, SQL Server, Oracle. |
| `port` | integer | server DBs | Port number (1–65535). Required for server-based databases. |
| `database` | string | ✅ | Database name. For SQLite/DuckDB this is the file path (use `":memory:"` for in-memory). |
| `username` | string | varies | Login username. Optional for SQLite/DuckDB. |
| `use_ssl` | bool | — | Enable TLS/SSL. Default `false`. |
| `parameters` | map | — | Driver-specific key/value extras (see per-adapter notes). Use `password` key here for credentials. |
| `pool_config` | object | — | Optional connection pool settings (see [pool_config](#pool_config)). |

#### `db_type` values

| Value | Database |
| :--- | :--- |
| `postgres` | PostgreSQL |
| `mysql` | MySQL / MariaDB |
| `sqlite` | SQLite |
| `mongodb` | MongoDB |
| `mssql` | SQL Server / Azure SQL |
| `oracle` | Oracle Database |
| `duckdb` | DuckDB |

#### `pool_config`

```yaml
pool_config:
  max_connections: 10      # maximum pool size (default: driver default)
  min_connections: 1       # minimum idle connections
  connect_timeout_secs: 30 # seconds before giving up on a new connection
  idle_timeout_secs: 600   # seconds before closing an idle connection
```

---

## Environment variable substitution

Any string value in a `ConnectionConfig` supports `${VAR}` or `$VAR` syntax.
Variables are resolved from the process environment at the time `substitute_env_vars()` is
called (immediately after loading in the CLI and MCP server).

```yaml
profiles:
  prod:
    connections:
      - id: prod-pg
        name: Production PostgreSQL
        db_type: postgres
        host: ${DB_HOST}
        port: 5432
        database: ${DB_NAME}
        username: ${DB_USER}
        use_ssl: true
        parameters:
          password: ${DB_PASSWORD}
```

If a referenced variable is missing, loading returns a descriptive error:

```
Environment variable 'DB_PASSWORD' not found (referenced in '${DB_PASSWORD}')
```

> **Tip for MCP / non-interactive use**: the MCP server runs without a TTY, so password
> prompts are not possible. Always store credentials in `parameters.password` or via an
> environment variable.

---

## Examples by database

### PostgreSQL

```yaml
default_profile: dev

profiles:
  dev:
    connections:
      - id: dev-pg
        name: Dev PostgreSQL
        db_type: postgres
        host: localhost
        port: 5432
        database: myapp_dev
        username: postgres
        use_ssl: false
        parameters:
          password: "${POSTGRES_PASSWORD}"
```

### MySQL / MariaDB

```yaml
profiles:
  dev:
    connections:
      - id: dev-mysql
        name: Dev MySQL
        db_type: mysql
        host: localhost
        port: 3306
        database: myapp_dev
        username: root
        use_ssl: false
        parameters:
          password: "${MYSQL_PASSWORD}"
```

### SQLite

```yaml
profiles:
  local:
    connections:
      - id: app-sqlite
        name: Application SQLite
        db_type: sqlite
        database: ./data/app.db   # relative to the working directory
        use_ssl: false

      - id: in-memory
        name: In-memory SQLite
        db_type: sqlite
        database: ":memory:"
        use_ssl: false
```

### MongoDB

```yaml
profiles:
  dev:
    connections:
      - id: dev-mongo
        name: Dev MongoDB
        db_type: mongodb
        host: localhost
        port: 27017
        database: myapp_dev
        use_ssl: false
        parameters:
          password: "${MONGO_PASSWORD}"
          # authSource: admin  # override auth database if needed
```

### SQL Server

```yaml
profiles:
  dev:
    connections:
      - id: dev-mssql
        name: Dev SQL Server
        db_type: mssql
        host: localhost
        port: 1433
        database: myapp_dev
        username: sa
        use_ssl: false
        parameters:
          password: "${MSSQL_PASSWORD}"
```

### Oracle

```yaml
profiles:
  dev:
    connections:
      - id: dev-oracle
        name: Dev Oracle
        db_type: oracle
        host: localhost
        port: 1521
        database: FREEPDB1          # service name or SID
        username: system
        use_ssl: false
        parameters:
          password: "${ORACLE_PASSWORD}"
```

### DuckDB

```yaml
profiles:
  analytics:
    connections:
      - id: analytics-file
        name: Analytics DuckDB
        db_type: duckdb
        database: ./data/analytics.duckdb
        use_ssl: false

      - id: analytics-memory
        name: In-memory DuckDB
        db_type: duckdb
        database: ":memory:"
        use_ssl: false
```

---

## Multi-connection profile

A single profile can hold connections to multiple databases — useful for ETL workflows:

```yaml
default_profile: etl

profiles:
  etl:
    connections:
      - id: source-pg
        name: Source PostgreSQL
        db_type: postgres
        host: source.example.com
        port: 5432
        database: warehouse
        username: reader
        use_ssl: true
        parameters:
          password: "${SOURCE_PG_PASSWORD}"

      - id: sink-duckdb
        name: Sink DuckDB
        db_type: duckdb
        database: ./output/warehouse.duckdb
        use_ssl: false
```

---

## TOML equivalent

All examples above also work as TOML using the array-of-tables syntax:

```toml
default_profile = "dev"

[profiles.dev]
[[profiles.dev.connections]]
id = "dev-pg"
name = "Dev PostgreSQL"
db_type = "postgres"
host = "localhost"
port = 5432
database = "myapp_dev"
username = "postgres"
use_ssl = false

[profiles.dev.connections.parameters]
password = "${POSTGRES_PASSWORD}"
```

---

## Validation rules

Configuration is validated when loaded. The following conditions cause an error:

- `id` is empty
- `database` is empty
- Server-based databases (`postgres`, `mysql`, `mongodb`, `mssql`, `oracle`) missing `host` or `port`
- `port` is 0
- Duplicate `id` values within a profile
- `default_profile` value does not match any profile name

---

## Rust API

```rust
use arni::{ArniConfig, ConfigProfile};

// Load from default search paths
let config = ArniConfig::load_from_default_paths()?;

// Load from an explicit path
let config = ArniConfig::load_from_file("/path/to/config.yaml")?;

// Apply environment variable substitution (done automatically by CLI/MCP)
let config = config.substitute_env_vars()?;

// Access a specific connection
if let Some(conn) = config.get_connection("dev", "dev-pg") {
    println!("Host: {:?}", conn.host);
}

// Iterate all connections in a profile
if let Some(profile) = config.get_profile("dev") {
    for conn in &profile.connections {
        println!("{} ({})", conn.name, conn.db_type);
    }
}
```
