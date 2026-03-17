#!/usr/bin/env bash
# Check script for Arni
# Runs cargo check to verify code compiles without building

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print colored message
print_msg() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Print usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Check that the Arni project compiles without building binaries.

OPTIONS:
    --lib               Check only the library
    --bins              Check only binaries
    --tests             Check tests
    --all               Check all targets (default)
    -h, --help          Show this help message
    -v, --verbose       Verbose output

EXAMPLES:
    $0                  # Check all targets
    $0 --lib            # Check only library
    $0 --tests          # Check tests

EOF
}

# Parse arguments
TARGET="--all-targets"
VERBOSE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --lib)
            TARGET="--lib"
            shift
            ;;
        --bins)
            TARGET="--bins"
            shift
            ;;
        --tests)
            TARGET="--tests"
            shift
            ;;
        --all)
            TARGET="--all-targets"
            shift
            ;;
        -v|--verbose)
            VERBOSE="--verbose"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_msg "$RED" "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main check function
main() {
    print_msg "$GREEN" "========================================="
    print_msg "$GREEN" "  Arni Check Script"
    print_msg "$GREEN" "========================================="
    echo ""

    # Check if cargo is available
    if ! command -v cargo &> /dev/null; then
        print_msg "$RED" "Error: cargo not found. Please install Rust toolchain."
        exit 1
    fi

    # Show Rust version
    print_msg "$YELLOW" "Rust version:"
    cargo --version
    echo ""

    print_msg "$GREEN" "Running cargo check $TARGET..."
    echo ""

    # Build check command
    local CHECK_CMD="cargo check $TARGET"
    
    if [[ -n "$VERBOSE" ]]; then
        CHECK_CMD="$CHECK_CMD $VERBOSE"
    fi

    # Execute check
    if eval "$CHECK_CMD"; then
        echo ""
        print_msg "$GREEN" "========================================="
        print_msg "$GREEN" "  Check passed! ✓"
        print_msg "$GREEN" "========================================="
        echo ""
        print_msg "$YELLOW" "All code compiles successfully."
        exit 0
    else
        echo ""
        print_msg "$RED" "========================================="
        print_msg "$RED" "  Check failed! ✗"
        print_msg "$RED" "========================================="
        echo ""
        print_msg "$YELLOW" "Fix compilation errors and try again."
        exit 1
    fi
}

main
