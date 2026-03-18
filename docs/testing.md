# Testing Guide

This document describes how arni's test suite is organised, how to run it, and what each layer tests.

---

## Test Layers

```text
crates/arni/
├── src/**/*.rs               ← 1. Unit tests  (inline, #[cfg(test)])
├── tests/
│   ├── common/               ← shared harness (config loading, container helpers)
│   ├── duckdb.rs             ← 2. In-memory integration tests (always run)
│   ├── sqlite.rs             ← 2. In-memory integration tests (always run)
│   ├── postgres.rs           ← 3. Live-server tests (opt-in via env var)
│   ├── mysql.rs
│   ├── mssql.rs
│   ├── mongodb.rs
│   └── oracle.rs

crates/arni-mcp/
├── tests/
│   ├── server_tests.rs            ← 1. Unit tests  (construction, filter DSL, resources)
│   ├── tool_integration_tests.rs  ← 2. In-memory integration (DuckDB, always run)
│   └── live_db_tests.rs           ← 3. Live-server MCP tool tests (opt-in via env var)
```

### 1 — Unit tests

Inline tests inside `src/` modules. Test pure functions — connection string builders, SQL literal formatting, config validation, filter DSL parsing.

```bash
# Fast; no database required
cargo test --lib --features "duckdb sqlite"
```

### 2 — In-memory integration tests

Use DuckDB (`:memory:`) or SQLite (`:memory:`). Require no running server. Verify the full adapter lifecycle: connect → write → query → introspect → disconnect.

```bash
# No server needed
cargo test --features "duckdb sqlite" -p arni
cargo test -p arni-mcp        # includes tool_integration_tests
```

### 3 — Live-server integration tests

Run against real database containers. Each test checks an availability environment variable and returns early (silently passes) if the flag is not set.

```bash
# Start containers first
podman-compose up -d

# Enable the adapters you want to test
export TEST_POSTGRES_AVAILABLE=true
export TEST_MYSQL_AVAILABLE=true
export TEST_MSSQL_AVAILABLE=true
export TEST_MONGODB_AVAILABLE=true
# export TEST_ORACLE_AVAILABLE=true  # local-only; see docs/local-databases.md

# Run adapter integration tests
cargo test --features postgres -p arni --test postgres
cargo test --features mysql   -p arni --test mysql
cargo test --features mssql   -p arni --test mssql
cargo test --features mongodb -p arni --test mongodb

# Run MCP live-DB tool tests
cargo test -p arni-mcp --test live_db_tests
```

See [`docs/local-databases.md`](local-databases.md) for container setup, credentials, and troubleshooting.

---

## Availability guards

Every live-server test begins with:

```rust
#[tokio::test]
async fn test_postgres_connect() {
    if common::skip_if_unavailable("postgres") { return; }
    // ... real test body
}
```

`skip_if_unavailable` checks `TEST_POSTGRES_AVAILABLE` (any truthy value: `true`, `1`, `yes`). The test passes silently in CI where no database is present.

---

## Test harness helpers

`crates/arni/tests/common/` provides:

| Helper | Purpose |
| :--- | :--- |
| `is_adapter_available(db)` | Returns `true` when `TEST_<DB>_AVAILABLE` is set |
| `skip_if_unavailable(db)` | Prints skip notice and returns `true` when unavailable |
| `load_test_config(profile)` | Loads `ConnectionConfig` from `~/.arni/connections.yml` or env vars |
| `containers::postgres_config()` | Pre-built config matching the dev-container defaults |
| `containers::mysql_config()` | Same for MySQL |
| `containers::mssql_config()` | Same for SQL Server |
| `containers::mongodb_config()` | Same for MongoDB |
| `containers::oracle_config()` | Same for Oracle |
| `containers::duckdb_memory_config()` | In-memory DuckDB (no container needed) |
| `containers::sqlite_memory_config()` | In-memory SQLite (no container needed) |

Config loading order for `load_test_config`:
1. `~/.arni/connections.yml` — entry matching `profile_name`
2. Environment variables `TEST_<PROFILE>_*` (see `common/mod.rs` for the full list)

---

## CI behaviour

The CI workflow (`ci.yml`) runs:

```bash
cargo test --workspace --lib
```

This compiles and runs **only the `#[cfg(test)]` unit tests** inside `src/` modules. Integration tests in `tests/` are never compiled by `--lib`, so live-server tests are structurally invisible to CI — no `#[ignore]` attributes are needed.

---

## Coverage

Target: ≥ 80 % line coverage (excluding integration test files and examples).

```bash
# Install once
cargo install cargo-tarpaulin

# Generate HTML report
make coverage
# or
./scripts/coverage.sh

# View
open target/tarpaulin/html/index.html
```

---

## Quick reference

| Goal | Command |
| :--- | :--- |
| Fast unit feedback | `cargo test --lib --features "duckdb sqlite"` |
| All tests (no server) | `cargo test --features "duckdb sqlite"` |
| MCP unit + integration | `cargo test -p arni-mcp` |
| Postgres live tests | `TEST_POSTGRES_AVAILABLE=true cargo test --features postgres -p arni --test postgres` |
| All live-DB MCP tests | `TEST_POSTGRES_AVAILABLE=true … cargo test -p arni-mcp --test live_db_tests` |
| Coverage report | `make coverage` |
| Full quality gates | `make pre-commit` |
