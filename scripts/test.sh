#!/usr/bin/env bash
# Test script for Arni
# Run unit tests, integration tests, or all tests with various options

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

# Detect CI environment
is_ci() {
    [[ -n "${CI:-}" ]] || [[ -n "${GITHUB_ACTIONS:-}" ]] || [[ -n "${JENKINS_HOME:-}" ]]
}

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

Run tests for the Arni project.

OPTIONS:
    -u, --unit          Run unit tests only
    -i, --integration   Run integration tests only
    -a, --all           Run all tests (default)
    -v, --verbose       Verbose output
    -q, --quiet         Quiet output (only show failures)
    --nocapture         Show all output (don't capture stdout/stderr)
    --release           Run tests in release mode
    -h, --help          Show this help message

EXAMPLES:
    $(basename "$0")                     # Run all tests
    $(basename "$0") --unit              # Run unit tests only
    $(basename "$0") --integration       # Run integration tests only
    $(basename "$0") -v --nocapture      # Verbose with all output
    $(basename "$0") --release           # Test release build

NOTES:
    - Unit tests are in each crate's src/ directory
    - Integration tests are in tests/ directory
    - CI environment is automatically detected
    - Use --nocapture to see println! output

EXIT CODES:
    0 - All tests passed
    1 - Test failures or errors

EOF
    exit 0
}

# Default options
TEST_TYPE="all"
VERBOSE=false
QUIET=false
NOCAPTURE=false
RELEASE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--unit)
            TEST_TYPE="unit"
            shift
            ;;
        -i|--integration)
            TEST_TYPE="integration"
            shift
            ;;
        -a|--all)
            TEST_TYPE="all"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        --nocapture)
            NOCAPTURE=true
            shift
            ;;
        --release)
            RELEASE=true
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

# Print environment info
if [[ "$VERBOSE" == "true" ]]; then
    print_msg "$GREEN" "Platform: $PLATFORM"
    if is_ci; then
        print_msg "$GREEN" "Environment: CI"
    else
        print_msg "$GREEN" "Environment: Local"
    fi
    print_msg "$GREEN" "Test type: $TEST_TYPE"
fi

# Prepare cargo test command
TEST_CMD="cargo test"

case $TEST_TYPE in
    unit)
        TEST_CMD="$TEST_CMD --lib"
        print_msg "$YELLOW" "Running unit tests..."
        ;;
    integration)
        TEST_CMD="$TEST_CMD --test '*'"
        print_msg "$YELLOW" "Running integration tests..."
        ;;
    all)
        print_msg "$YELLOW" "Running all tests..."
        ;;
esac

# Add flags
if [[ "$RELEASE" == "true" ]]; then
    TEST_CMD="$TEST_CMD --release"
fi

if [[ "$VERBOSE" == "true" ]]; then
    TEST_CMD="$TEST_CMD --verbose"
fi

if [[ "$QUIET" == "true" ]]; then
    TEST_CMD="$TEST_CMD --quiet"
fi

if [[ "$NOCAPTURE" == "true" ]]; then
    TEST_CMD="$TEST_CMD -- --nocapture"
fi

# Run tests
if eval "$TEST_CMD"; then
    print_msg "$GREEN" "✓ All tests passed"
    exit 0
else
    print_msg "$RED" "✗ Tests failed"
    exit 1
fi
