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
./scripts/check.sh
```

### Development cycle
```bash
# Make changes...
./scripts/check.sh              # Fast compile check
./scripts/build.sh              # Full debug build
cargo test                      # Run tests
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
