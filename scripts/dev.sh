#!/usr/bin/env bash
# Development watch script for Arni
# Uses cargo-watch for live reloading and automatic recompilation

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Platform detection
OS="$(uname -s)"
case "${OS}" in
    Linux*)     PLATFORM=Linux;;
    Darwin*)    PLATFORM=macOS;;
    *)          PLATFORM="UNKNOWN:${OS}"
esac

# Print colored message
print_msg() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Print usage
usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Development watch script with live reloading using cargo-watch.

OPTIONS:
    -t, --test          Watch and run tests on change
    -c, --check         Watch and run cargo check only
    -r, --run           Watch and run the application
    -v, --verbose       Verbose output
    -h, --help          Show this help message

EXAMPLES:
    $(basename "$0")                     # Watch and build on changes
    $(basename "$0") --test              # Watch and run tests
    $(basename "$0") --check             # Watch and check only
    $(basename "$0") --run               # Watch and run application

NOTES:
    - Requires cargo-watch to be installed
    - Install with: cargo install cargo-watch
    - Press Ctrl+C to stop watching

EOF
    exit 0
}

# ── Read ~/.arni/config.yml and export ORACLE_LIB_DIR / DUCKDB_LIB_DIR ────────
# Shell env always wins (force=false semantics matching AppConfig::apply_lib_paths).
load_arni_config() {
    local config_file="${HOME}/.arni/config.yml"
    [[ -f "$config_file" ]] || return 0

    # Extract a scalar value for a given top-level YAML key.
    # Handles: key: value   and   key: ~/path (tilde not special in grep)
    yaml_scalar() {
        local key="$1"
        grep -E "^${key}:" "$config_file" \
            | head -1 \
            | sed -E "s/^${key}:[[:space:]]*//" \
            | sed 's/[[:space:]]*#.*$//' \
            | sed "s/^['\"]//;s/['\"]$//" \
            | xargs  # trim leading/trailing whitespace
    }

    # Expand a leading ~ to $HOME (POSIX-safe)
    expand_tilde_val() {
        local val="$1"
        case "$val" in
            "~"|"~/"|"~/"*)  echo "${HOME}${val#\~}" ;;
            *)                echo "$val" ;;
        esac
    }

    local oracle_dir
    oracle_dir="$(yaml_scalar oracle_lib_dir)"
    if [[ -n "$oracle_dir" && -z "${ORACLE_LIB_DIR:-}" ]]; then
        oracle_dir="$(expand_tilde_val "$oracle_dir")"
        export ORACLE_LIB_DIR="$oracle_dir"
    fi

    local duckdb_dir
    duckdb_dir="$(yaml_scalar duckdb_lib_dir)"
    if [[ -n "$duckdb_dir" && -z "${DUCKDB_LIB_DIR:-}" ]]; then
        duckdb_dir="$(expand_tilde_val "$duckdb_dir")"
        export DUCKDB_LIB_DIR="$duckdb_dir"
    fi
}

# Check if cargo-watch is installed
check_cargo_watch() {
    if ! command -v cargo-watch &> /dev/null; then
        print_msg "$RED" "Error: cargo-watch is not installed"
        print_msg "$YELLOW" "Install with: cargo install cargo-watch"
        exit 1
    fi
}

# Check that system libduckdb is available (required — we no longer use bundled)
check_duckdb() {
    # Honour explicit override first
    local lib_dir="${DUCKDB_LIB_DIR:-}"
    if [[ -n "$lib_dir" ]]; then
        if [[ -f "$lib_dir/libduckdb.dylib" || -f "$lib_dir/libduckdb.so" ]]; then
            return 0
        fi
        print_msg "$RED" "Error: DUCKDB_LIB_DIR='$lib_dir' but no libduckdb found there"
        exit 1
    fi

    # Apple Silicon Homebrew default
    if [[ -f "/opt/homebrew/lib/libduckdb.dylib" ]]; then
        return 0
    fi
    # Intel Mac / Linux Homebrew / system default
    if [[ -f "/usr/local/lib/libduckdb.dylib" || -f "/usr/local/lib/libduckdb.so" ]]; then
        return 0
    fi

    print_msg "$RED" "Error: system libduckdb not found."
    print_msg "$YELLOW" "Install it first:"
    if [[ "$PLATFORM" == "macOS" ]]; then
        print_msg "$YELLOW" "  brew install duckdb"
    else
        print_msg "$YELLOW" "  See https://duckdb.org/docs/installation (choose 'C/C++ API')"
    fi
    print_msg "$YELLOW" "Or set DUCKDB_LIB_DIR=/path/to/your/lib before running."
    exit 1
}

# Check that Oracle Instant Client is available (needed by oracle crate at runtime).
# Uses ORACLE_LIB_DIR (set in your shell profile) as the canonical location.
# Exports DYLD_LIBRARY_PATH (macOS) / LD_LIBRARY_PATH (Linux) so that
# cargo test processes can dlopen libclntsh.dylib at runtime.
check_oracle_client() {
    local lib_dir="${ORACLE_LIB_DIR:-}"

    # Fall back to already-set DYLD_LIBRARY_PATH / LD_LIBRARY_PATH
    if [[ -z "$lib_dir" ]]; then
        lib_dir="${DYLD_LIBRARY_PATH:-${LD_LIBRARY_PATH:-}}"
    fi

    if [[ -n "$lib_dir" ]]; then
        if [[ -f "$lib_dir/libclntsh.dylib" || -f "$lib_dir/libclntsh.so" ]]; then
            # Ensure cargo-spawned test processes can find the library
            export DYLD_LIBRARY_PATH="$lib_dir"
            export LD_LIBRARY_PATH="$lib_dir"
            return 0
        fi
        print_msg "$YELLOW" "Warning: ORACLE_LIB_DIR='$lib_dir' set but no libclntsh found there."
        print_msg "$YELLOW" "Oracle adapter tests will be skipped."
        return 0
    fi

    print_msg "$YELLOW" "Warning: ORACLE_LIB_DIR is not set — Oracle adapter tests will be skipped."
    print_msg "$YELLOW" "To enable Oracle tests, add to your shell profile (~/.zshrc or ~/.bashrc):"
    if [[ "$PLATFORM" == "macOS" ]]; then
        print_msg "$YELLOW" "  export ORACLE_LIB_DIR=~/Oracle/instantclient_23_3"
        print_msg "$YELLOW" "  export DYLD_LIBRARY_PATH=\$ORACLE_LIB_DIR"
        print_msg "$YELLOW" "Download: https://www.oracle.com/database/technologies/instant-client/macos-arm64-downloads.html"
    else
        print_msg "$YELLOW" "  export ORACLE_LIB_DIR=/opt/oracle/instantclient"
        print_msg "$YELLOW" "  export LD_LIBRARY_PATH=\$ORACLE_LIB_DIR"
        print_msg "$YELLOW" "Download: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
    fi
    # Not fatal — non-Oracle adapters still work without Instant Client
}

# Default options
WATCH_MODE="build"
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            WATCH_MODE="test"
            shift
            ;;
        -c|--check)
            WATCH_MODE="check"
            shift
            ;;
        -r|--run)
            WATCH_MODE="run"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            print_msg "$RED" "Unknown option: $1"
            print_msg "$YELLOW" "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Load user config first so env vars are available for dependency checks
load_arni_config

# Check dependencies
check_cargo_watch
check_duckdb
check_oracle_client

# Print platform info
print_msg "$GREEN" "Platform: $PLATFORM"
print_msg "$GREEN" "Watch mode: $WATCH_MODE"

# Prepare cargo-watch command
WATCH_CMD="cargo watch"
if [[ "$VERBOSE" == "true" ]]; then
    WATCH_CMD="$WATCH_CMD --verbose"
fi

case $WATCH_MODE in
    build)
        WATCH_CMD="$WATCH_CMD -x build"
        print_msg "$YELLOW" "Watching for changes and building..."
        ;;
    test)
        WATCH_CMD="$WATCH_CMD -x test"
        print_msg "$YELLOW" "Watching for changes and running tests..."
        ;;
    check)
        WATCH_CMD="$WATCH_CMD -x check"
        print_msg "$YELLOW" "Watching for changes and checking..."
        ;;
    run)
        WATCH_CMD="$WATCH_CMD -x run"
        print_msg "$YELLOW" "Watching for changes and running..."
        ;;
esac

# Run cargo-watch
print_msg "$GREEN" "Starting cargo-watch (Press Ctrl+C to stop)..."
echo ""

if eval "$WATCH_CMD"; then
    print_msg "$GREEN" "Watch stopped successfully"
    exit 0
else
    print_msg "$RED" "Watch failed!"
    exit 1
fi
