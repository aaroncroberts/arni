#!/usr/bin/env bash
# Clean script for Arni
# Removes build artifacts and target directory

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

Clean build artifacts from the Arni project.

OPTIONS:
    -a, --all           Remove all cargo caches and locks (use with caution)
    -h, --help          Show this help message
    -v, --verbose       Verbose output

EXAMPLES:
    $0                  # Clean target/ directory
    $0 --all            # Clean everything including Cargo.lock

EOF
}

# Parse arguments
CLEAN_ALL=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -a|--all)
            CLEAN_ALL=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
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

# Main clean function
main() {
    print_msg "$GREEN" "========================================="
    print_msg "$GREEN" "  Arni Clean Script"
    print_msg "$GREEN" "========================================="
    echo ""

    # Check if target directory exists
    if [[ -d "target" ]]; then
        print_msg "$YELLOW" "Removing target/ directory..."
        
        if [[ "$VERBOSE" == true ]]; then
            du -sh target/ 2>/dev/null || echo "Unable to calculate size"
        fi
        
        rm -rf target/
        print_msg "$GREEN" "✓ target/ directory removed"
    else
        print_msg "$YELLOW" "target/ directory does not exist (already clean)"
    fi

    # Clean additional artifacts if --all flag is set
    if [[ "$CLEAN_ALL" == true ]]; then
        echo ""
        print_msg "$YELLOW" "Performing deep clean..."
        
        # Remove Cargo.lock (will be regenerated on next build)
        if [[ -f "Cargo.lock" ]]; then
            print_msg "$YELLOW" "Removing Cargo.lock..."
            rm -f Cargo.lock
            print_msg "$GREEN" "✓ Cargo.lock removed"
        fi
        
        # Clean cargo cache for this project
        if command -v cargo &> /dev/null; then
            print_msg "$YELLOW" "Running cargo clean..."
            cargo clean
            print_msg "$GREEN" "✓ Cargo cache cleaned"
        fi
    fi

    echo ""
    print_msg "$GREEN" "========================================="
    print_msg "$GREEN" "  Clean completed successfully!"
    print_msg "$GREEN" "========================================="
    
    exit 0
}

main
