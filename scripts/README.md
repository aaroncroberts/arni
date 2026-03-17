# Build Scripts

This directory contains build automation scripts for the Arni project.

## Available Scripts

### build.sh

Build the Arni project in debug or release mode.

**Usage:**
```bash
./scripts/build.sh [OPTIONS]
```

**Options:**
- `-r, --release` - Build in release mode (optimized)
- `-d, --debug` - Build in debug mode (default)
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/build.sh              # Debug build
./scripts/build.sh --release    # Release build
./scripts/build.sh -r -v        # Release build with verbose output
```

**Output:**
- Debug binaries: `target/debug/`
- Release binaries: `target/release/`

### clean.sh

Remove build artifacts and clean the project.

**Usage:**
```bash
./scripts/clean.sh [OPTIONS]
```

**Options:**
- `-a, --all` - Remove all cargo caches and Cargo.lock (deep clean)
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/clean.sh              # Remove target/ directory
./scripts/clean.sh --all        # Deep clean (includes Cargo.lock)
```

**What gets cleaned:**
- Always: `target/` directory
- With `--all`: `Cargo.lock`, cargo cache

### check.sh

Verify that code compiles without building binaries. Fast way to check for compilation errors.

**Usage:**
```bash
./scripts/check.sh [OPTIONS]
```

**Options:**
- `--lib` - Check only the library
- `--bins` - Check only binaries
- `--tests` - Check tests
- `--all` - Check all targets (default)
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/check.sh              # Check all targets
./scripts/check.sh --lib        # Check only library
./scripts/check.sh --tests      # Check tests compile
```

### dev.sh

Watch for file changes and automatically rebuild or test using cargo-watch.

**Usage:**
```bash
./scripts/dev.sh [OPTIONS]
```

**Options:**
- `-t, --test` - Watch and run tests on change
- `-c, --check` - Watch and run cargo check only
- `-r, --run` - Watch and run the application
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/dev.sh                # Watch and build on changes
./scripts/dev.sh --test         # Watch and run tests
./scripts/dev.sh --check        # Watch and check only
./scripts/dev.sh --run          # Watch and run app
```

**Requirements:**
- Requires `cargo-watch`: `cargo install cargo-watch`

**Notes:**
- Press Ctrl+C to stop watching
- Automatically rebuilds when files change
- Great for TDD workflow

### fmt.sh

Format all Rust code using rustfmt.

**Usage:**
```bash
./scripts/fmt.sh [OPTIONS]
```

**Options:**
- `-c, --check` - Check formatting without modifying files
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/fmt.sh                # Format all code
./scripts/fmt.sh --check        # Check formatting only
./scripts/fmt.sh -v             # Format with verbose output
```

**Notes:**
- Formats all Rust files in the workspace
- Create `rustfmt.toml` in project root for custom settings
- Check mode (--check) is useful in CI/CD pipelines

### clippy.sh

Run Clippy linter with deny warnings to catch code issues.

**Usage:**
```bash
./scripts/clippy.sh [OPTIONS]
```

**Options:**
- `-f, --fix` - Automatically fix warnings when possible
- `-a, --all` - Check all targets (lib, bins, tests, examples)
- `-v, --verbose` - Verbose output
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/clippy.sh             # Run clippy with deny warnings
./scripts/clippy.sh --fix       # Fix warnings automatically
./scripts/clippy.sh --all       # Check all targets
./scripts/clippy.sh -f -v       # Fix with verbose output
```

**Notes:**
- Warnings are treated as errors (-D warnings)
- Use --fix to automatically apply suggested fixes
- Some fixes may require manual intervention

### test.sh

Run unit tests, integration tests, or all tests with various options.

**Usage:**
```bash
./scripts/test.sh [OPTIONS]
```

**Options:**
- `-u, --unit` - Run unit tests only
- `-i, --integration` - Run integration tests only
- `-a, --all` - Run all tests (default)
- `-v, --verbose` - Verbose output
- `-q, --quiet` - Quiet output (only show failures)
- `--nocapture` - Show all output (don't capture stdout/stderr)
- `--release` - Run tests in release mode
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/test.sh               # Run all tests
./scripts/test.sh --unit        # Run unit tests only
./scripts/test.sh --integration # Run integration tests only
./scripts/test.sh -v --nocapture # Verbose with all output
./scripts/test.sh --release     # Test release build
```

**Notes:**
- Unit tests are in each crate's src/ directory
- Integration tests are in tests/ directory
- CI environment is automatically detected
- Use --nocapture to see println! output

### coverage.sh

Generate code coverage reports using cargo-tarpaulin.

**Usage:**
```bash
./scripts/coverage.sh [OPTIONS]
```

**Options:**
- `-o, --output FORMAT` - Output format: html, xml, lcov, json (default: html)
- `-t, --threshold NUM` - Minimum coverage threshold percentage (0-100)
- `-v, --verbose` - Verbose output
- `--open` - Open HTML report in browser after generation
- `-h, --help` - Show help message

**Examples:**
```bash
./scripts/coverage.sh                  # Generate HTML report
./scripts/coverage.sh --threshold 80   # Require 80% coverage
./scripts/coverage.sh --output xml     # Generate XML report
./scripts/coverage.sh --open           # Generate and open in browser
./scripts/coverage.sh -o html -o xml   # Generate both formats
```

**Requirements:**
- Requires `cargo-tarpaulin`: `cargo install cargo-tarpaulin`
- On macOS, may require Docker or have limitations

**Notes:**
- Reports are generated in target/coverage/
- Multiple output formats can be specified
- Threshold option useful for CI/CD pipelines
- Script exits with code 1 if coverage below threshold

## Platform Support

All scripts are designed to work on:
- **macOS** (Darwin)
- **Linux**

The scripts automatically detect the platform and adjust behavior accordingly.

## Error Handling

All scripts use `set -euo pipefail` for robust error handling:
- `-e`: Exit on error
- `-u`: Exit on undefined variable
- `-o pipefail`: Exit on pipeline failures

Non-zero exit codes indicate failures.

## Requirements

- **Rust toolchain** (cargo) must be installed
- **Bash** version 3.2+ (default on macOS and Linux)

## Common Workflows

### Clean build from scratch
```bash
./scripts/clean.sh && ./scripts/build.sh --release
```

### Quick validation
```bash
./scripts/check.sh              # Fast compile check
./scripts/fmt.sh --check        # Check formatting
./scripts/clippy.sh             # Lint code
./scripts/test.sh               # Run all tests
```

### Development cycle (manual)
```bash
# Make changes...
./scripts/check.sh              # Fast compile check
./scripts/build.sh              # Full debug build
./scripts/test.sh               # Run tests
```

### Development cycle (watch mode)
```bash
./scripts/dev.sh --test         # Auto-run tests on changes
```

### Testing workflows
```bash
./scripts/test.sh --unit        # Run unit tests only
./scripts/test.sh --integration # Run integration tests only
./scripts/test.sh -v --nocapture # Verbose with output
./scripts/coverage.sh           # Generate coverage report
./scripts/coverage.sh --open    # Generate and view coverage
```

### Pre-commit checks
```bash
./scripts/fmt.sh                # Format code
./scripts/clippy.sh --fix       # Fix linting issues
./scripts/check.sh              # Verify compilation
./scripts/test.sh               # Run tests
```

### CI/CD validation
```bash
./scripts/fmt.sh --check        # Check formatting
./scripts/clippy.sh --all       # Lint all targets
./scripts/build.sh --release    # Build optimized
./scripts/test.sh               # Run test suite
./scripts/coverage.sh --threshold 80 --output xml  # Coverage with threshold
```

## Exit Codes

- `0` - Success
- `1` - Error (compilation failure, missing dependencies, etc.)

## Adding New Scripts

When adding new scripts to this directory:

1. Start with the shebang: `#!/usr/bin/env bash`
2. Add `set -euo pipefail` for error handling
3. Include platform detection if needed
4. Add color output for better UX
5. Include `--help` option with usage documentation
6. Make executable: `chmod +x scripts/your-script.sh`
7. Update this README with usage instructions

## See Also

- [Main README](../README.md) - Project overview
- [Testing Guide](../docs/testing.md) - Testing patterns
