#!/usr/bin/env bash
# Linting script for Arni
# Runs Clippy with deny warnings to catch code issues

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

Run Clippy linter on the project with deny warnings.

OPTIONS:
    -f, --fix           Automatically fix warnings when possible
    -a, --all           Check all targets (lib, bins, tests, examples)
    -v, --verbose       Verbose output
    -h, --help          Show this help message

EXAMPLES:
    $(basename "$0")                     # Run clippy with deny warnings
    $(basename "$0") --fix               # Fix warnings automatically
    $(basename "$0") --all               # Check all targets
    $(basename "$0") -f -v               # Fix with verbose output

NOTES:
    - Warnings are treated as errors (deny warnings)
    - Use --fix to automatically apply suggested fixes
    - Some fixes may require manual intervention

EXIT CODES:
    0 - Success (no warnings or errors)
    1 - Warnings or errors found

EOF
    exit 0
}

# Default options
FIX=false
ALL_TARGETS=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--fix)
            FIX=true
            shift
            ;;
        -a|--all)
            ALL_TARGETS=true
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

# Print platform info
if [[ "$VERBOSE" == "true" ]]; then
    print_msg "$GREEN" "Platform: $PLATFORM"
fi

# Prepare clippy command
CLIPPY_CMD="cargo clippy"

if [[ "$ALL_TARGETS" == "true" ]]; then
    # Match CI exactly: --all-targets --all-features catches feature-gated code
    CLIPPY_CMD="$CLIPPY_CMD --all-targets --all-features"
fi

# Add deny warnings flag
CLIPPY_CMD="$CLIPPY_CMD -- -D warnings"

if [[ "$FIX" == "true" ]]; then
    # For fixes, use clippy-fix instead
    CLIPPY_CMD="cargo clippy --fix"
    if [[ "$ALL_TARGETS" == "true" ]]; then
        CLIPPY_CMD="$CLIPPY_CMD --all-targets --all-features"
    fi
    CLIPPY_CMD="$CLIPPY_CMD -- -D warnings"
    print_msg "$YELLOW" "Running clippy with automatic fixes..."
else
    print_msg "$YELLOW" "Running clippy..."
fi

if [[ "$VERBOSE" == "true" ]]; then
    CLIPPY_CMD="$CLIPPY_CMD --verbose"
fi

# Run clippy
if eval "$CLIPPY_CMD"; then
    if [[ "$FIX" == "true" ]]; then
        print_msg "$GREEN" "✓ Clippy fixes applied successfully"
    else
        print_msg "$GREEN" "✓ No clippy warnings found"
    fi
    exit 0
else
    if [[ "$FIX" == "true" ]]; then
        print_msg "$RED" "✗ Clippy fixes failed"
        print_msg "$YELLOW" "Some issues may require manual intervention"
    else
        print_msg "$RED" "✗ Clippy found warnings or errors"
        print_msg "$YELLOW" "Run with --fix to automatically apply fixes"
    fi
    exit 1
fi
