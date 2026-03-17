#!/usr/bin/env bash
# Formatting script for Arni
# Runs rustfmt on all Rust code in the project

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

Format all Rust code using rustfmt.

OPTIONS:
    -c, --check         Check formatting without modifying files
    -v, --verbose       Verbose output
    -h, --help          Show this help message

EXAMPLES:
    $(basename "$0")                     # Format all code
    $(basename "$0") --check             # Check formatting only
    $(basename "$0") -v                  # Format with verbose output

NOTES:
    - Uses rustfmt with default configuration
    - Create rustfmt.toml in project root for custom settings
    - All Rust files in the workspace will be formatted

EXIT CODES:
    0 - Success (or all files already formatted in check mode)
    1 - Error (or formatting needed in check mode)

EOF
    exit 0
}

# Default options
CHECK_ONLY=false
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--check)
            CHECK_ONLY=true
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

# Prepare rustfmt command
FMT_CMD="cargo fmt --all"
if [[ "$CHECK_ONLY" == "true" ]]; then
    FMT_CMD="$FMT_CMD -- --check"
    print_msg "$YELLOW" "Checking code formatting..."
else
    print_msg "$YELLOW" "Formatting code..."
fi

if [[ "$VERBOSE" == "true" ]]; then
    FMT_CMD="$FMT_CMD --verbose"
fi

# Run formatting
if eval "$FMT_CMD"; then
    if [[ "$CHECK_ONLY" == "true" ]]; then
        print_msg "$GREEN" "✓ All code is properly formatted"
    else
        print_msg "$GREEN" "✓ Code formatted successfully"
    fi
    exit 0
else
    if [[ "$CHECK_ONLY" == "true" ]]; then
        print_msg "$RED" "✗ Some files need formatting"
        print_msg "$YELLOW" "Run $(basename "$0") to format the code"
    else
        print_msg "$RED" "✗ Formatting failed"
    fi
    exit 1
fi
