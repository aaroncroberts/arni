# Arni Development Guide

> **🚨 MANDATORY WORKFLOW**: Before starting ANY work, read [`WORKFLOW.md`](../../WORKFLOW.md). All work MUST follow the two-phase approach: Planning ([`task-generate.md`](commands/task-generate.md)) → Execution ([`task-execute.md`](commands/task-execute.md)).

## Project Overview

**Arni** is a Rust library and CLI tool for unified database access. It provides a consistent interface for exporting DataFrames to multiple database systems (PostgreSQL, MongoDB, Oracle, SQL Server, DuckDB) with intelligent type inference and adapter patterns.

**Tech Stack:**
- **Language:** Rust (stable + nightly)
- **Data:** Polars DataFrames
- **Databases:** PostgreSQL, MongoDB, Oracle, SQL Server, DuckDB
- **Testing:** cargo test, tarpaulin (coverage)
- **CI/CD:** GitHub Actions

## Workflow (MUST READ)

**See [`WORKFLOW.md`](../../WORKFLOW.md) for complete details.**

### Quick Summary

1. **Planning Phase** (ALWAYS FIRST):
   - Use `.claude/commands/task-generate.md` to create WBS
   - Create Epic → Features → Tasks with dependencies
   - Sync with `bd sync`

2. **Execution Phase** (ONLY AFTER PLANNING):
   - Use `.claude/commands/task-execute.md` workflows
   - Follow type-specific execution patterns
   - Update state frequently with `bd update` and `bd sync`
   - Test, lint, format before closing

**Never skip planning. Never execute without context.**

## TDD Workflow

Arni follows strict Test-Driven Development practices:

1. **Write failing test** - Start with a test that defines behavior
2. **Run tests** - Confirm test fails (`cargo test`)
3. **Write minimal code** - Implement just enough to pass
4. **Run tests** - Confirm test passes
5. **Refactor** - Clean up code while keeping tests green
6. **Check coverage** - Maintain ≥80% coverage
7. **Lint & format** - Run clippy and rustfmt before commit

### Quick TDD Commands

```bash
# Start TDD session (auto-rebuild on changes)
./scripts/dev.sh --test

# Run all tests
cargo test
# or
make test

# Run unit tests only (fast feedback)
cargo test --lib
# or
make test-unit

# Run integration tests
cargo test --test '*'
# or
make test-integration

# Check coverage (target: ≥80%)
./scripts/coverage.sh
# or
make coverage

# Format code
cargo fmt
# or
make fmt

# Lint code (deny warnings)
cargo clippy -- -D warnings
# or
make clippy
```

## Test Organization

### Unit Tests
- **Location:** In-module tests (`src/*/mod.rs`)
- **Scope:** Single function/struct behavior
- **Speed:** Fast (<100ms)
- **Dependencies:** None (use mocks if needed)

```rust
// src/adapters/postgres.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_string_builder() {
        let config = PostgresConfig::new("localhost", 5432, "testdb");
        let conn_str = config.connection_string();
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("5432"));
    }
}
```

### Integration Tests
- **Location:** `tests/` directory (separate from src)
- **Scope:** End-to-end adapter behavior
- **Speed:** Slower (database I/O)
- **Dependencies:** Real or test databases

```rust
// tests/postgres.rs
#[test]
#[ignore] // Enable with database setup
fn test_postgres_export() {
    let df = sample_users_dataframe();
    let adapter = PostgresAdapter::new(test_config());
    let result = adapter.export(df, "users");
    assert!(result.is_ok());
}
```

**Conditional Execution:**
- Integration tests use environment variables (`TEST_POSTGRES_AVAILABLE=true`)
- Tests check availability before running: `if !is_postgres_available() { return; }`
- Use `#[ignore]` attribute for tests requiring database setup
- See [tests/README.md](../tests/README.md) for database setup

### Test Fixtures
- **Location:** `tests/fixtures/mod.rs`
- **Purpose:** Reusable test data (sample DataFrames, schemas)
- **Usage:** Import with `use crate::fixtures::*;`

```rust
// Available fixtures
let users = sample_users_dataframe();       // 5 sample users
let products = sample_products_dataframe(); // 3 sample products
let orders = sample_orders_dataframe();     // 3 sample orders
let empty = empty_dataframe();              // Edge case testing
```

## Code Quality Standards

### Coverage Requirements
- **Minimum:** 80% line coverage
- **Goal:** 90% line coverage
- **Exclude:** Integration tests, example code

```bash
# Generate coverage report
./scripts/coverage.sh

# View HTML report
open target/tarpaulin/html/index.html

# CI coverage check (enforce threshold)
./scripts/coverage.sh --threshold 80
```

### Formatting (rustfmt)
- **Style:** Default rustfmt settings
- **Check:** `cargo fmt -- --check` (CI mode)
- **Fix:** `cargo fmt` (auto-format)
- **Config:** See `rustfmt.toml` (if present)

```bash
# Format all code
./scripts/fmt.sh

# Check formatting (CI mode)
./scripts/fmt.sh --check
# or
make fmt-check
```

### Linting (clippy)
- **Mode:** Deny warnings (`-D warnings`)
- **Targets:** All (lib, bins, tests, examples)
- **Auto-fix:** Available with `--fix` flag

```bash
# Run clippy (deny warnings)
./scripts/clippy.sh

# Auto-fix warnings
./scripts/clippy.sh --fix

# Check all targets
./scripts/clippy.sh --all
# or
make clippy
```

**Clippy Rules:**
- All warnings treated as errors in CI
- Use `#[allow(clippy::rule)]` sparingly (with justification)
- Fix pedantic warnings before merge

## Development Workflow

### Starting Work
```bash
# 1. Claim task
bd update <task-id> --status in_progress

# 2. Create feature branch
git checkout -b feature/my-feature

# 3. Start TDD session
./scripts/dev.sh --test
```

### Development Loop
```bash
# Write test → Write code → Test passes
# Repeat until feature complete

# Quick checks during development
cargo test --lib           # Fast unit tests
cargo check                # Type checking only
./scripts/check.sh         # Verify compilation
```

### Pre-Commit Checklist
```bash
# Run all quality gates
make pre-commit

# Or manually:
cargo fmt                           # Format code
cargo clippy -- -D warnings         # Lint code
cargo test                          # Run all tests
./scripts/coverage.sh --threshold 80 # Check coverage
```

### Committing Work
```bash
# Stage changes
git add -A

# Commit with conventional commit message
git commit -m "feat(adapters): Add PostgreSQL export support"

# Push and sync issue tracker
git push && bd sync
```

### Completing Task
```bash
# 1. Document deliverables
bd update <task-id> --notes="Implemented X, Y, Z"

# 2. Close task
bd close <task-id> --reason="Feature complete with tests"

# 3. Sync to remote
git push && bd sync
```

## Makefile Targets

All common workflows available via Makefile:

```bash
make help              # Show all available targets

# Development
make dev               # Watch and rebuild on changes
make dev-test          # Watch and run tests on changes
make check             # Fast compilation check
make build             # Build debug version
make build-release     # Build optimized version

# Testing
make test              # Run all tests
make test-unit         # Run unit tests only
make test-integration  # Run integration tests only
make test-ignored      # Run ignored tests (requires DB setup)
make coverage          # Generate coverage report

# Code Quality
make fmt               # Format code
make fmt-check         # Check formatting (no changes)
make clippy            # Lint with clippy
make fix               # Auto-fix clippy warnings
make pre-commit        # Run all quality gates

# CI/CD
make ci-check          # Run full CI pipeline locally

# Utilities
make clean             # Remove build artifacts
make clean-all         # Deep clean (includes Cargo.lock)
make info              # Show project information
make install           # Install dev dependencies
```

See [scripts/README.md](../scripts/README.md) for detailed script documentation.

## CI/CD Integration

GitHub Actions runs on every push/PR to `main` or `develop`:

### CI Pipeline Jobs
1. **Format Check** - Verify `cargo fmt` compliance
2. **Clippy** - Lint with `-D warnings`
3. **Test Matrix** - Test on 6 combinations:
   - OS: Ubuntu, macOS, Windows
   - Rust: stable, nightly
4. **Build Release** - Optimized build verification
5. **Minimal Versions** - Check minimum dependency versions
6. **CI Success** - Summary job for branch protection

### Running CI Locally
```bash
# Run full CI pipeline
make ci-check

# Or individual components
make fmt-check
make clippy
cargo test --workspace
cargo build --release
```

### CI Optimization
- **Caching:** Cargo registry, git dependencies, build artifacts
- **Incremental:** Only changed crates rebuild
- **Parallel:** Multiple matrix combinations run simultaneously

See [.github/workflows/README.md](../.github/workflows/README.md) for CI details.

## Best Practices

### Testing
- **Test first:** Write test before implementation
- **One assertion:** Focus tests on single behavior
- **Clear names:** Use descriptive test function names
- **Arrange-Act-Assert:** Structure tests consistently
- **Fast feedback:** Keep unit tests fast (<100ms)
- **Integration separate:** Use `#[ignore]` for slow tests
- **Mock external:** Use mocks for external dependencies
- **Edge cases:** Test empty, null, boundary conditions

### Code Organization
- **Small functions:** <50 lines per function
- **Single responsibility:** One clear purpose per function
- **Error handling:** Use `Result<T, E>` with descriptive errors
- **Documentation:** Public API has doc comments (`///`)
- **Examples:** Include `# Examples` in doc comments
- **Type safety:** Use newtypes for domain concepts

### Git Workflow
- **Branch naming:** `feature/`, `fix/`, `refactor/`, `docs/`
- **Commit messages:** Conventional commits (`feat:`, `fix:`, `docs:`, etc.)
- **Small commits:** Logical, atomic changes
- **Issue tracking:** Reference issue IDs in commits
- **Sync frequently:** `git push && bd sync` after each task

## Troubleshooting

### Tests Failing
```bash
# Run with full output
cargo test -- --nocapture

# Run with verbose logging
RUST_LOG=debug cargo test

# Run specific test
cargo test test_name -- --exact --nocapture
```

### Integration Tests Skipped
```bash
# Check environment variables
cat .env.test

# Enable PostgreSQL tests
export TEST_POSTGRES_AVAILABLE=true
export TEST_POSTGRES_HOST=localhost
export TEST_POSTGRES_PORT=5432

# Start test database
docker run --name arni-test-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=arni_test \
  -p 5432:5432 \
  -d postgres:15

# Run with ignored tests
cargo test --test '*' -- --ignored
```

### Coverage Not Generated
```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Run with verbose output
./scripts/coverage.sh -v

# Check for compilation errors
cargo build --tests
```

### Clippy Warnings
```bash
# Show detailed explanation
cargo clippy -- -D warnings --verbose

# Auto-fix safe warnings
cargo clippy --fix

# Allow specific warning (use sparingly)
#[allow(clippy::rule_name)]
```

## Resources

- **Project Docs:** [docs/](../docs/)
- **Examples:** [examples/](../examples/)
- **Scripts:** [scripts/README.md](../scripts/README.md)
- **Integration Tests:** [tests/README.md](../tests/README.md)
- **CI/CD:** [.github/workflows/README.md](../.github/workflows/README.md)
- **Issue Tracking:** See [docs/issue-tracking.md](../docs/issue-tracking.md) — issues synced to [aaroncroberts/arni on DoltHub](https://www.dolthub.com/repositories/aaroncroberts/arni)
- **Release Process:** See [docs/release-process.md](../docs/release-process.md) — open draft PR first, commit continuously, CI gates the merge, then tag. Never delete `aaron/agentic-coder`.
- **Draft PR Workflow (portable):** See [docs/draft-pr-workflow.md](../docs/draft-pr-workflow.md) — generic, project-agnostic version of the same workflow.
- **Rust Book:** https://doc.rust-lang.org/book/
- **Clippy Lints:** https://rust-lang.github.io/rust-clippy/

## Quick Reference Card

```bash
# Start TDD session
./scripts/dev.sh --test         # Auto-test on save

# TDD loop
cargo test --lib                # Fast unit tests
cargo test                      # All tests
./scripts/coverage.sh           # Check coverage

# Quality gates (pre-commit)
make pre-commit                 # Run all checks

# Or individually
cargo fmt                       # Format
cargo clippy -- -D warnings     # Lint
cargo test                      # Test
./scripts/coverage.sh --threshold 80  # Coverage

# Task workflow
bd update <id> --status in_progress   # Claim
git commit -m "feat: Message"         # Commit
bd close <id> --reason="Done"         # Complete
git push && bd sync                   # Sync
```

---

**Remember:** Tests first, coverage ≥80%, clippy clean, format before commit, sync after push.
