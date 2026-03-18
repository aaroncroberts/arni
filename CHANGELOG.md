# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.3.0] — 2026-03-18

### Added
- **`examples/axum-api`** — standalone Axum HTTP server example; uses SQLite in-memory with
  zero config; exposes `GET /tables`, `GET /query?sql=…`, `POST /bulk-insert`;
  `make_adapter()` is the single swap-point to redirect the whole API at any real database
- **`crates/arni-mcp/README.md`** — crate-level README with quick-start registration snippets
  for Claude Desktop and Claude Code, tool table, and links to full docs

### Fixed
- **CLI help**: Removed `global = true` from `--list-tools`, `--capabilities`, `--schema` so
  these discovery-only flags no longer appear in every subcommand's help output
- **CLI `--search-mode`**: Removed duplicate `[default: contains]` that appeared in both the
  help text string and clap's automatic default annotation
- **CLI `connect` / `mcp` help text**: Improved descriptions — `connect` now states it prints
  server info; `mcp` now lists the 14 tools, config file, and one-line registration snippet
- **Docs**: Fixed CLI usage examples across `README.md` and `docs/` to match actual `--profile`
  flag syntax (examples previously used an old positional-argument style that never existed)
- **Docs**: Bumped all Cargo.toml example version strings from `"0.1"` to `"0.2"`
- **Docs**: Added missing `pool_config` field to `ConnectionConfig` struct in
  `docs/architecture.md`

---

## [0.2.0] — 2026-03-18

### Added
- **`arni-mcp`** — first-class [Model Context Protocol](https://modelcontextprotocol.io) server
  that exposes all 14 `DbAdapter` operations as AI-callable tools via the rmcp 0.12 SDK:
  `query`, `execute`, `tables`, `describe_table`, `list_databases`, `get_indexes`,
  `get_foreign_keys`, `get_views`, `get_server_info`, `list_stored_procedures`,
  `find_tables`, `bulk_insert`, `bulk_update`, `bulk_delete`
- **`arni mcp`** CLI subcommand — starts the MCP server on stdin/stdout; register with
  Claude Desktop, Claude Code, or any MCP-compatible agent in one line
- **Filter DSL** — JSON predicate language (`{"col": {"eq": value}}`, `and`, `or`, `not`, `in`,
  `is_null`, `is_not_null`) used by `bulk_update` and `bulk_delete` tool calls
- **MCP resource provider** — active connection profiles exposed as `arni://profiles/{name}`
  resources so agents can enumerate live connections without a tool call
- **`docs/mcp.md`** — full MCP guide: architecture, quick start, tool reference, Filter DSL,
  resource listing, logging, password handling, limitations vs CLI
- **`docs/examples/mcp.json`** — Claude Desktop / Claude Code registration snippet

### Changed
- **`arni-data` merged into `arni`** — implementation crate consolidated; all source now lives in
  `crates/arni/src/`. Import paths (`use arni::DbAdapter`) are unchanged.
- **All documentation updated** — `docs/configuration.md` fully rewritten to match the real
  `ArniConfig`/`ConfigProfile`/`ConnectionConfig` schema; all `arni-data` path references
  corrected across `README.md`, `CONTRIBUTING.md`, `docs/architecture.md`, `docs/getting-started.md`
- `docs/configuration.md` now documents environment variable substitution as **implemented**
  (was incorrectly described as "planned")

### Testing
- 19 unit tests in `crates/arni-mcp/tests/server_tests.rs` — server construction, `get_info`,
  resource helpers, and full Filter DSL coverage
- 19 integration tests in `crates/arni-mcp/tests/tool_integration_tests.rs` — every tool handler
  exercised end-to-end against an in-memory DuckDB database; no external server required
- 20 live-database MCP tool tests in `crates/arni-mcp/tests/live_db_tests.rs` — all 14 tool
  handlers verified against PostgreSQL, MySQL, SQL Server, and MongoDB containers; tests skip
  silently when containers are not running (opt-in via `TEST_<DB>_AVAILABLE=true`)
- `docs/testing.md` rewritten to accurately document the three-layer test strategy, CI behaviour,
  and all harness helpers

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

[Unreleased]: https://github.com/aaroncroberts/arni/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/aaroncroberts/arni/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aaroncroberts/arni/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/aaroncroberts/arni/releases/tag/v0.1.0
