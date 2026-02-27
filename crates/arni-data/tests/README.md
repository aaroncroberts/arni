# arni-data Integration Tests

Integration tests for the arni-data adapter layer. Each test connects to a real
database instance, so they are gated behind environment variables and must be
opted into explicitly.

---

## Prerequisites

- A running database for the adapter(s) you want to test
- A connection profile in `~/.arni/connections.yml` **or** the equivalent env vars
- `TEST_<DB>_AVAILABLE=true` set in your environment

The quickest path is to use the dev containers in `compose.yml` at the repo root:

```bash
podman-compose -f compose.yml up -d postgres mysql
```

---

## Configuration

### Option 1 — `~/.arni/connections.yml` (recommended for local dev)

Add a named profile to your config file:

```yaml
# ~/.arni/connections.yml

pg-dev:
  type: postgres
  host: localhost
  port: 5432
  database: arni_test
  username: arni
  password: arni

mysql-dev:
  type: mysql
  host: localhost
  port: 3306
  database: arni_test
  username: arni
  password: arni

sqlite-mem:
  type: sqlite
  database: ":memory:"

duckdb-mem:
  type: duckdb
  database: ":memory:"

mongo-dev:
  type: mongodb
  host: localhost
  port: 27017
  database: arni_test
  username: arni
  password: arni
```

Use `arni config add` to manage these entries through the CLI.

### Option 2 — Environment variables (useful for CI)

Set `TEST_<PREFIX>_TYPE` and `TEST_<PREFIX>_DATABASE` at minimum, where the
prefix is the profile name uppercased with `-` and `.` replaced by `_`:

| Profile name | Env prefix         |
|--------------|--------------------|
| `pg-dev`     | `TEST_PG_DEV`      |
| `mysql.ci`   | `TEST_MYSQL_CI`    |
| `sqlite-mem` | `TEST_SQLITE_MEM`  |

Available variables:

| Variable            | Required | Description                          |
|---------------------|----------|--------------------------------------|
| `TEST_<P>_TYPE`     | Yes      | Database type (see aliases below)    |
| `TEST_<P>_DATABASE` | Yes      | Database / schema name               |
| `TEST_<P>_HOST`     | No       | Hostname or IP address               |
| `TEST_<P>_PORT`     | No       | Port (falls back to default)         |
| `TEST_<P>_USER`     | No       | Username                             |
| `TEST_<P>_PASSWORD` | No       | Password (never stored in YAML keys) |
| `TEST_<P>_SSL`      | No       | `true` or `1` to enable SSL/TLS      |

**Accepted type strings:**

| Canonical   | Aliases                  |
|-------------|--------------------------|
| `postgres`  | `postgresql`             |
| `mysql`     | —                        |
| `sqlite`    | —                        |
| `mongodb`   | `mongo`                  |
| `sqlserver` | `mssql`                  |
| `oracle`    | —                        |
| `duckdb`    | —                        |

---

## Running Tests

### Enable an adapter and run its tests

```bash
# Declare the adapter available
export TEST_POSTGRES_AVAILABLE=true

# Run all postgres tests (requires the postgres feature flag)
cargo test -p arni-data --features postgres --test postgres

# Or run everything for all enabled adapters
cargo test -p arni-data --features all-databases
```

### Run only the harness self-tests (no DB required)

```bash
cargo test -p arni-data --test harness
```

### Run ignored tests (require full DB setup)

```bash
cargo test -p arni-data --features postgres -- --ignored
```

---

## Writing Adapter Tests

Use the helpers from `tests/common/mod.rs` in every adapter test file.

```rust
// tests/postgres.rs
mod common;

#[test]
fn test_postgres_connect() {
    if common::skip_if_unavailable("postgres") {
        return;
    }
    let cfg = common::load_test_config("pg-dev")
        .expect("pg-dev profile required for postgres tests");

    // ... test body using cfg
}
```

### Profile name conventions

| Adapter    | Recommended profile | Availability var            |
|------------|---------------------|-----------------------------|
| PostgreSQL | `pg-dev`            | `TEST_POSTGRES_AVAILABLE`   |
| MySQL      | `mysql-dev`         | `TEST_MYSQL_AVAILABLE`      |
| SQLite     | `sqlite-mem`        | `TEST_SQLITE_AVAILABLE`     |
| DuckDB     | `duckdb-mem`        | `TEST_DUCKDB_AVAILABLE`     |
| MongoDB    | `mongo-dev`         | `TEST_MONGODB_AVAILABLE`    |
| SQL Server | `mssql-dev`         | `TEST_SQLSERVER_AVAILABLE`  |
| Oracle     | `oracle-dev`        | `TEST_ORACLE_AVAILABLE`     |

---

## CI Integration

Set these in your CI environment (GitHub Actions, etc.) and point each
`TEST_<P>_*` variable at the matching service container:

```yaml
env:
  TEST_POSTGRES_AVAILABLE: "true"
  TEST_PG_DEV_TYPE: postgres
  TEST_PG_DEV_HOST: localhost
  TEST_PG_DEV_PORT: 5432
  TEST_PG_DEV_DATABASE: arni_test
  TEST_PG_DEV_USER: arni
  TEST_PG_DEV_PASSWORD: ${{ secrets.POSTGRES_PASSWORD }}
```
