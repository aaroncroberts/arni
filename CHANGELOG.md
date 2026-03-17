# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] — 2026-03-17

### Added
- **`arni-data`** — core library with a unified `DbAdapter` trait and adapters for
  PostgreSQL, MySQL, SQLite, MongoDB, Microsoft SQL Server, Oracle, and DuckDB
- **`arni`** — public re-export facade; downstream users write `use arni::DbAdapter`
  while all implementation lives in `arni-data`
- **`arni-cli`** — command-line tool (`arni connect`, `arni query`, `arni metadata`,
  `arni export`, `arni config`, `arni daemon`) with Unix socket daemon mode
- **`arni-logging`** — structured `tracing`-based logging with file rotation,
  configurable via `~/.arni/logging.yml`
- Polars `DataFrame` as the primary data interchange format across all adapters
- Multi-format export: CSV, JSON, Parquet, Arrow IPC, Excel (`.xlsx`)
- `ConnectionRegistry` and `SharedAdapter` for runtime adapter management
- `ArniConfig` with YAML/TOML config file discovery (`~/.arni/config.yaml`)
- Feature-gated database drivers — enable only what you need:
  `postgres`, `mysql`, `sqlite`, `mongodb`, `mssql`, `oracle`, `duckdb`, `all-databases`
- Integration test suite with Docker/Podman container harness (per-adapter)
- CI pipeline: format check, Clippy (`-D warnings`), unit tests (Ubuntu stable)

---

[Unreleased]: https://github.com/aaroncroberts/arni/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aaroncroberts/arni/releases/tag/v0.1.0
