#!/usr/bin/env bash
# Build script for Arni
# Supports debug and release builds with cross-platform compatibility

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
Usage: $0 [OPTIONS]

Build the Arni Rust project.

OPTIONS:
    -r, --release       Build in release mode (optimized)
    -d, --debug         Build in debug mode (default)
    -h, --help          Show this help message
    -v, --verbose       Verbose output

EXAMPLES:
    $0                  # Build in debug mode
    $0 --release        # Build in release mode
    $0 -r -v            # Build in release with verbose output

EOF
}

# Parse arguments
MODE="debug"
VERBOSE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--release)
            MODE="release"
            shift
            ;;
        -d|--debug)
            MODE="debug"
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

# Main build function
main() {
    print_msg "$GREEN" "========================================="
    print_msg "$GREEN" "  Arni Build Script"
    print_msg "$GREEN" "========================================="
    echo ""
    print_msg "$YELLOW" "Platform: ${PLATFORM}"
    print_msg "$YELLOW" "Build Mode: ${MODE}"
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
    print_msg "$GREEN" "Starting build..."
    echo ""

    # Build command
    local BUILD_CMD="cargo build"
    
    if [[ "$MODE" == "release" ]]; then
        BUILD_CMD="$BUILD_CMD --release"
    fi

    if [[ -n "$VERBOSE" ]]; then
        BUILD_CMD="$BUILD_CMD $VERBOSE"
    fi

    # Execute build
    if eval "$BUILD_CMD"; then
        echo ""
        print_msg "$GREEN" "========================================="
        print_msg "$GREEN" "  Build successful! ($MODE)"
        print_msg "$GREEN" "========================================="
        echo ""
        
        # Show build artifacts location
        if [[ "$MODE" == "release" ]]; then
            print_msg "$YELLOW" "Binaries: target/release/"
        else
            print_msg "$YELLOW" "Binaries: target/debug/"
        fi
        
        exit 0
    else
        echo ""
        print_msg "$RED" "========================================="
        print_msg "$RED" "  Build failed!"
        print_msg "$RED" "========================================="
        exit 1
    fi
}

main
