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
```

### Development cycle (manual)
```bash
# Make changes...
./scripts/check.sh              # Fast compile check
./scripts/build.sh              # Full debug build
cargo test                      # Run tests
```

### Development cycle (watch mode)
```bash
./scripts/dev.sh --test         # Auto-run tests on changes
```

### Pre-commit checks
```bash
./scripts/fmt.sh                # Format code
./scripts/clippy.sh --fix       # Fix linting issues
./scripts/check.sh              # Verify compilation
cargo test                      # Run tests
```

### CI/CD validation
```bash
./scripts/fmt.sh --check        # Check formatting
./scripts/clippy.sh --all       # Lint all targets
./scripts/build.sh --release    # Build optimized
cargo test                      # Run test suite
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
