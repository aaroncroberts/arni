#!/usr/bin/env bash
# Coverage script for Arni
# Generate code coverage reports using cargo-tarpaulin

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

Generate code coverage reports using cargo-tarpaulin.

OPTIONS:
    -o, --output FORMAT Output format: html, xml, lcov, json (default: html)
    -t, --threshold NUM Minimum coverage threshold percentage (0-100)
    -v, --verbose       Verbose output
    --open              Open HTML report in browser after generation
    -h, --help          Show this help message

EXAMPLES:
    $(basename "$0")                     # Generate HTML report
    $(basename "$0") --threshold 80      # Require 80% coverage
    $(basename "$0") --output xml        # Generate XML report
    $(basename "$0") --open              # Generate and open in browser
    $(basename "$0") -o html -o xml      # Generate both formats

NOTES:
    - Requires cargo-tarpaulin to be installed
    - Install with: cargo install cargo-tarpaulin
    - Reports are generated in target/coverage/
    - On macOS, tarpaulin requires Docker or may have limitations
    - Multiple output formats can be specified

COVERAGE THRESHOLDS:
    - Specify a minimum percentage with --threshold
    - Script will exit with code 1 if coverage is below threshold
    - Useful for CI/CD pipelines

EXIT CODES:
    0 - Success (coverage meets or exceeds threshold)
    1 - Error or coverage below threshold

EOF
    exit 0
}

# Check if cargo-tarpaulin is installed
check_tarpaulin() {
    if ! command -v cargo-tarpaulin &> /dev/null; then
        print_msg "$RED" "Error: cargo-tarpaulin is not installed"
        print_msg "$YELLOW" "Install with: cargo install cargo-tarpaulin"
        if [[ "$PLATFORM" == "macOS" ]]; then
            print_msg "$YELLOW" "Note: On macOS, tarpaulin has limitations and may require Docker"
            print_msg "$YELLOW" "Alternative: Use cargo-llvm-cov: cargo install cargo-llvm-cov"
        fi
        exit 1
    fi
}

# Default options
OUTPUT_FORMATS=()
THRESHOLD=""
VERBOSE=false
OPEN_REPORT=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_FORMATS+=("$2")
            shift 2
            ;;
        -t|--threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --open)
            OPEN_REPORT=true
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

# Set default output format if none specified
if [[ ${#OUTPUT_FORMATS[@]} -eq 0 ]]; then
    OUTPUT_FORMATS=("html")
fi

# Check dependencies
check_tarpaulin

# Print environment info
if [[ "$VERBOSE" == "true" ]]; then
    print_msg "$GREEN" "Platform: $PLATFORM"
    if is_ci; then
        print_msg "$GREEN" "Environment: CI"
    else
        print_msg "$GREEN" "Environment: Local"
    fi
    print_msg "$GREEN" "Output formats: ${OUTPUT_FORMATS[*]}"
    if [[ -n "$THRESHOLD" ]]; then
        print_msg "$GREEN" "Coverage threshold: ${THRESHOLD}%"
    fi
fi

# Create output directory
OUTPUT_DIR="target/coverage"
mkdir -p "$OUTPUT_DIR"

# Prepare cargo-tarpaulin command
TARPAULIN_CMD="cargo tarpaulin --out"

# Add output formats
for format in "${OUTPUT_FORMATS[@]}"; do
    case $format in
        html|xml|lcov|json)
            TARPAULIN_CMD="$TARPAULIN_CMD $format"
            ;;
        *)
            print_msg "$RED" "Unknown output format: $format"
            print_msg "$YELLOW" "Valid formats: html, xml, lcov, json"
            exit 1
            ;;
    esac
done

# Add output directory
TARPAULIN_CMD="$TARPAULIN_CMD --output-dir $OUTPUT_DIR"

# Add threshold if specified
if [[ -n "$THRESHOLD" ]]; then
    TARPAULIN_CMD="$TARPAULIN_CMD --fail-under $THRESHOLD"
fi

# Add verbose flag
if [[ "$VERBOSE" == "true" ]]; then
    TARPAULIN_CMD="$TARPAULIN_CMD --verbose"
fi

# Run coverage
print_msg "$YELLOW" "Generating coverage report..."
print_msg "$YELLOW" "This may take a few minutes..."

if eval "$TARPAULIN_CMD"; then
    print_msg "$GREEN" "✓ Coverage report generated successfully"
    
    # Show report locations
    print_msg "$GREEN" "\nReports generated in: $OUTPUT_DIR/"
    for format in "${OUTPUT_FORMATS[@]}"; do
        case $format in
            html)
                print_msg "$GREEN" "  - HTML: $OUTPUT_DIR/index.html"
                ;;
            xml)
                print_msg "$GREEN" "  - XML: $OUTPUT_DIR/cobertura.xml"
                ;;
            lcov)
                print_msg "$GREEN" "  - LCOV: $OUTPUT_DIR/lcov.info"
                ;;
            json)
                print_msg "$GREEN" "  - JSON: $OUTPUT_DIR/tarpaulin-report.json"
                ;;
        esac
    done
    
    # Open HTML report if requested
    if [[ "$OPEN_REPORT" == "true" ]]; then
        for format in "${OUTPUT_FORMATS[@]}"; do
            if [[ "$format" == "html" ]]; then
                HTML_PATH="$OUTPUT_DIR/index.html"
                if [[ -f "$HTML_PATH" ]]; then
                    print_msg "$GREEN" "\nOpening report in browser..."
                    if [[ "$PLATFORM" == "macOS" ]]; then
                        open "$HTML_PATH"
                    elif [[ "$PLATFORM" == "Linux" ]]; then
                        xdg-open "$HTML_PATH" &> /dev/null || print_msg "$YELLOW" "Could not open browser automatically"
                    fi
                fi
                break
            fi
        done
    fi
    
    exit 0
else
    print_msg "$RED" "✗ Coverage generation failed or below threshold"
    if [[ -n "$THRESHOLD" ]]; then
        print_msg "$YELLOW" "Coverage may be below the ${THRESHOLD}% threshold"
    fi
    exit 1
fi
