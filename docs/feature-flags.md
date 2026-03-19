# Feature Flags

Arni uses Cargo feature flags to keep compile times and binary sizes small. By default, the `arni` crate compiles with **no database drivers and no polars dependency** — you opt in to exactly what you need.

## Why feature flags?

Without feature flags, building `arni` would pull in every database driver (PostgreSQL, MySQL, SQLite, MongoDB, SQL Server, Oracle, DuckDB) plus Polars — a combination that previously produced 21 GB+ of build artifacts. With feature flags:

- A minimal SQLite-only build compiles in ~30 seconds
- A full build (all databases + polars) still requires ~10 minutes but is an explicit opt-in
- CI catches regressions across all flag combinations via the [feature matrix job](.github/workflows/ci.yml)

## arni crate features

| Feature | Description | Adds |
|---------|-------------|------|
| `polars` | DataFrame API: `read_table_df`, `query_df`, `export_dataframe`, `export` module | ~1.2 GB rlibs |
| `dataframe` | Alias for `polars` | (same) |
| `postgres` | PostgreSQL adapter via sqlx | sqlx + native-tls |
| `mysql` | MySQL adapter via sqlx | sqlx + native-tls |
| `sqlite` | SQLite adapter via sqlx | sqlx (small) |
| `mongodb` | MongoDB adapter | mongo driver |
| `mssql` | SQL Server adapter via tiberius + bb8 | tiberius, bb8 |
| `oracle` | Oracle adapter (requires OCI client) | oracle crate |
| `duckdb` | DuckDB adapter — links against **system** DuckDB via pkg-config | ~5 MB headers |
| `duckdb-bundled` | DuckDB adapter — builds DuckDB **from source** | ~500 MB source + compile |
| `all-databases` | All DB adapters (uses system DuckDB) | all of the above |
| `full` | `all-databases` + `polars` | everything |

### Choosing between `duckdb` and `duckdb-bundled`

- **`duckdb`** (default): requires DuckDB installed on the system (e.g. via Homebrew: `brew install duckdb`). Links at build time using pkg-config. Fast compile.
- **`duckdb-bundled`**: builds DuckDB from C++ source. No system install needed, but adds ~500 MB and 10+ minutes to compile. Use only in CI or Docker images where you can't install system packages.

## arni-cli features

The `arni-cli` binary uses pass-through features. By default it compiles with `postgres + sqlite + polars`.

| Feature | Effect |
|---------|--------|
| `default` | `postgres`, `sqlite`, `polars` |
| `postgres` | Enable PostgreSQL |
| `mysql` | Enable MySQL |
| `sqlite` | Enable SQLite |
| `mongodb` | Enable MongoDB |
| `mssql` | Enable SQL Server |
| `oracle` | Enable Oracle |
| `duckdb` | Enable DuckDB (system install) |
| `duckdb-bundled` | Enable DuckDB (built from source) |
| `polars` | Enable DataFrame output (query, export commands) |
| `all-databases` | All DB adapters |
| `full` | All databases + polars |

### Install examples

```bash
# Default (postgres + sqlite + polars)
cargo install arni

# PostgreSQL only, no polars
cargo install arni --no-default-features --features postgres

# All databases, no polars (lighter output — QueryResult only)
cargo install arni --no-default-features --features all-databases

# Everything
cargo install arni --no-default-features --features full

# DuckDB from source (no system install needed)
cargo install arni --no-default-features --features postgres,sqlite,polars,duckdb-bundled
```

## arni-mcp features

The MCP server uses the same pass-through pattern. Default: `postgres + sqlite`.

```bash
# Default
cargo build -p arni-mcp

# All databases + DataFrame support
cargo build -p arni-mcp --features full
```

## Makefile presets

```bash
make build-cli          # postgres + sqlite + polars (default)
make build-cli-all      # all databases + polars + duckdb-bundled
make build-cli-minimal  # no DB drivers (test feature gating)
make build-mcp          # postgres + sqlite
make build-mcp-all      # all databases + polars
```

## Using arni as a library

When adding `arni` as a Cargo dependency, specify only the features you need:

```toml
[dependencies]
# Minimal: QueryResult-only API, no polars, no DB drivers
arni = { version = "0.3", default-features = false }

# PostgreSQL + lightweight QueryResult rows
arni = { version = "0.3", default-features = false, features = ["postgres"] }

# PostgreSQL + DataFrame API
arni = { version = "0.3", default-features = false, features = ["postgres", "polars"] }

# Everything
arni = { version = "0.3", features = ["full"] }
```

### API without polars

When the `polars` feature is **not** enabled, the adapter trait provides:

```rust
// Always available — returns lightweight Vec<Vec<QueryValue>>
let result: QueryResult = adapter.execute_query("SELECT * FROM users").await?;
let result: QueryResult = adapter.read_table("users", None).await?;
```

When the `polars` feature **is** enabled, additional methods become available:

```rust
// Only with --features polars
let df: DataFrame = adapter.read_table_df("users", None).await?;
let df: DataFrame = adapter.query_df("SELECT * FROM users").await?;
adapter.export_dataframe(&df, "users", None, false).await?;
```

### Re-exported polars

When the `polars` feature is enabled, arni re-exports the `polars` crate so downstream users don't need a direct dependency:

```rust
// In your Cargo.toml: arni = { features = ["polars"] }
// No need for a direct polars dep:
use arni::polars::prelude::DataFrame;
```

## System DuckDB setup

For the `duckdb` feature (system-linked mode):

**macOS (Homebrew):**
```bash
brew install duckdb
# pkg-config picks it up automatically
```

**Linux (Ubuntu/Debian):**
```bash
# Download from https://github.com/duckdb/duckdb/releases
wget https://github.com/duckdb/duckdb/releases/download/v1.4.4/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip -d /tmp/duckdb
sudo install -m755 /tmp/duckdb/libduckdb.so /usr/local/lib/
sudo install -m644 /tmp/duckdb/duckdb.h /tmp/duckdb/duckdb.hpp /usr/local/include/
sudo ldconfig
```

**CI (GitHub Actions):** See `.github/workflows/ci.yml` — the `feature-matrix` job installs system DuckDB automatically for combinations that need it.
