# Contributing to Arni

Thank you for taking the time to contribute! Arni is a welcoming project and every pull request, bug report, and documentation improvement matters.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting the Code](#getting-the-code)
3. [Development Setup](#development-setup)
4. [TDD Workflow](#tdd-workflow)
5. [Running the Test Suite](#running-the-test-suite)
6. [Code Quality Gates](#code-quality-gates)
7. [Submitting a Pull Request](#submitting-a-pull-request)
8. [Adding a New Adapter](#adding-a-new-adapter)

---

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold it.

---

## Getting the Code

```bash
git clone https://github.com/acroberts16/arni.git
cd arni
```

Rust ≥ 1.75 (stable) is required. Install via [rustup](https://rustup.rs).

---

## Development Setup

```bash
# Check that the workspace compiles
cargo check --workspace

# Install coverage tooling (optional, needed for make coverage)
cargo install cargo-tarpaulin

# Install cargo-watch for live test feedback (optional)
cargo install cargo-watch

# Start local databases for integration tests (requires Docker or Podman)
podman-compose up -d
```

See [`docs/local-databases.md`](docs/local-databases.md) for database setup details.

---

## TDD Workflow

All changes to library code follow **Red → Green → Refactor**:

### 1. Red — write a failing test

Start with a test that defines the desired behaviour. Confirm it fails before writing any implementation:

```bash
cargo test --lib --features duckdb 2>&1 | grep FAILED
```

### 2. Green — minimal implementation

Write the smallest amount of code that makes the test pass. Run tests frequently:

```bash
# Unit tests only — fast feedback loop
cargo test --lib --features "duckdb sqlite"

# Or watch mode (auto-runs on file save)
cargo watch -x "test --lib --features duckdb sqlite"
```

### 3. Refactor

Clean up the implementation while keeping tests green. Then add integration tests in `tests/<adapter>.rs`.

---

## Running the Test Suite

### Unit tests (no database required)

```bash
cargo test --lib --features "duckdb sqlite"
```

### Integration tests (in-memory databases, no server needed)

```bash
cargo test --features "duckdb sqlite" -p arni
```

### Integration tests (server-based databases)

Start the local databases first (`podman-compose up -d`), then:

```bash
export TEST_POSTGRES_AVAILABLE=true
export TEST_POSTGRES_HOST=localhost TEST_POSTGRES_PORT=5432
export TEST_POSTGRES_DATABASE=test_db
export TEST_POSTGRES_USERNAME=test_user TEST_POSTGRES_PASSWORD=test_password

cargo test --features postgres -p arni --test postgres -- --include-ignored
```

Replace `postgres` with `mysql`, `mssql`, `mongodb`, or `oracle` for other adapters. See [`docs/architecture.md`](docs/architecture.md#testing-strategy) for the complete variable list.

### All tests + examples (CI mode)

```bash
make ci-check
```

### Coverage

Target: ≥ 80 % line coverage.

```bash
make coverage   # generates HTML report in target/tarpaulin/html/
```

---

## Code Quality Gates

Every pull request must pass all four gates before merge:

| Gate | Command | Requirement |
| :--- | :--- | :--- |
| Formatting | `cargo fmt -- --check` | No unstaged changes |
| Linting | `cargo clippy -- -D warnings` | Zero warnings |
| Tests | `cargo test --features "duckdb sqlite"` | All pass |
| Coverage | `make coverage` | ≥ 80 % line |

Run them all at once:

```bash
make pre-commit
```

---

## Submitting a Pull Request

1. **Fork** the repository and create a descriptive branch:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make your changes** following the TDD workflow above.

3. **Run the pre-commit checks**:
   ```bash
   make pre-commit
   ```

4. **Push** to your fork and open a PR against `main`.

5. Fill in the pull request template — describe *what* changed and *why*.

GitHub CI will run the full test matrix (Ubuntu + macOS, stable + nightly Rust) automatically. Address any failures before requesting review.

---

## Adding a New Adapter

The architecture is designed to make new adapters straightforward. See the step-by-step guide in [`docs/architecture.md`](docs/architecture.md#implementing-a-new-adapter):

1. Add the driver as an optional Cargo dependency
2. Create `crates/arni/src/adapters/<dbname>.rs` using the provided skeleton
3. Register the module in `adapters/mod.rs` and `lib.rs`
4. Add integration tests in `tests/<dbname>.rs`

Open a draft PR early — maintainers are happy to give feedback before the implementation is complete.

---

## Questions?

- Open a [GitHub Discussion](https://github.com/acroberts16/arni/discussions) for questions and ideas
- Open an [Issue](https://github.com/acroberts16/arni/issues) for bugs and feature requests
