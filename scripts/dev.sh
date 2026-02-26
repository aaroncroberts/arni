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

# Check if cargo-watch is installed
check_cargo_watch() {
    if ! command -v cargo-watch &> /dev/null; then
        print_msg "$RED" "Error: cargo-watch is not installed"
        print_msg "$YELLOW" "Install with: cargo install cargo-watch"
        exit 1
    fi
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

# Check dependencies
check_cargo_watch

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
