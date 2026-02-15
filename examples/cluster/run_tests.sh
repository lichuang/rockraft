#!/bin/bash
#
# run_tests.sh - Run all tests in the test directory using pytest
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/test"
START_SCRIPT="$SCRIPT_DIR/start.sh"

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo "============================================================"
    echo "$1"
    echo "============================================================"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help       Show this help message"
    echo "  -v, --verbose    Run with verbose output"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests"
    echo "  $0 -v                 # Run with verbose output"
}

# Function to check if pytest is available
check_pytest() {
    if ! command -v python3 &> /dev/null; then
        print_error "python3 not found"
        exit 1
    fi
    
    if ! python3 -c "import pytest" 2>/dev/null; then
        print_error "pytest not found. Please install it: pip install pytest"
        exit 1
    fi
}

# Main function
main() {
    local verbose=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--verbose)
                verbose="-v"
                shift
                ;;
            -*)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                shift
                ;;
        esac
    done
    
    print_header "Cluster Test Runner"
    
    # Check if test directory exists
    if [ ! -d "$TEST_DIR" ]; then
        print_error "Test directory not found: $TEST_DIR"
        exit 1
    fi
    
    # Check pytest
    check_pytest
    
    # Run pytest
    print_info "Running tests with pytest..."
    cd "$SCRIPT_DIR"
    
    if python3 -m pytest test/test_cluster_members.py $verbose; then
        print_success "All tests passed!"
        exit 0
    else
        print_error "Some tests failed!"
        exit 1
    fi
}

# Run main
main "$@"
