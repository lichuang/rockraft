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
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
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

print_test() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

print_section() {
    echo -e "${MAGENTA}[SECTION]${NC} $1"
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
    echo "  -v, --verbose    Run with verbose output (show test names and print statements)"
    echo "  -q, --quiet      Run with minimal output"
    echo "  -k, --keyword    Run only tests matching the given keyword expression"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests with default output"
    echo "  $0 -v                 # Run with verbose output (show print statements)"
    echo "  $0 -v -k scan_prefix  # Run only tests matching 'scan_prefix'"
    echo "  $0 -q                 # Run with minimal output"
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

# Function to run pytest with progress display
run_pytest() {
    local pytest_args=("$@")
    
    print_info "Starting test execution..."
    print_info "Test file: test/test_cluster.py"
    echo ""
    
    # Run pytest and capture output while displaying it
    cd "$SCRIPT_DIR"
    
    # Disable exit on error temporarily to capture pytest exit code
    set +e
    python3 -m pytest test/test_cluster.py "${pytest_args[@]}" 2>&1
    local exit_code=$?
    set -e
    
    return $exit_code
}

# Main function
main() {
    local verbose_flag=""
    local quiet_flag=""
    local keyword=""
    local pytest_args=()
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--verbose)
                verbose_flag="1"
                shift
                ;;
            -q|--quiet)
                quiet_flag="1"
                shift
                ;;
            -k|--keyword)
                if [[ -n "$2" && ! "$2" =~ ^- ]]; then
                    keyword="$2"
                    shift 2
                else
                    print_error "Option -k requires a keyword argument"
                    exit 1
                fi
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
    
    # Build pytest arguments
    if [ -n "$verbose_flag" ]; then
        # Verbose mode: show test names and print statements
        pytest_args+=("-v" "-s")
        print_info "Mode: Verbose (showing test names and print output)"
    elif [ -n "$quiet_flag" ]; then
        # Quiet mode
        pytest_args+=("-q")
        print_info "Mode: Quiet"
    else
        # Default mode
        pytest_args+=("-v")
        print_info "Mode: Default (showing test names)"
    fi
    
    # Add keyword filter if specified
    if [ -n "$keyword" ]; then
        pytest_args+=("-k" "$keyword")
        print_info "Filter: Running only tests matching '$keyword'"
    fi
    
    # Add traceback format
    pytest_args+=("--tb=short")
    
    # Show test collection info
    print_info "Discovering tests..."
    echo ""
    
    # Run pytest
    local exit_code=0
    run_pytest "${pytest_args[@]}" || exit_code=$?
    
    echo ""
    print_header "Test Summary"
    
    if [ $exit_code -eq 0 ]; then
        print_success "All tests passed!"
        print_info "Test file: test/test_cluster.py"
        if [ -n "$keyword" ]; then
            print_info "Filter: Tests matching '$keyword'"
        fi
        exit 0
    elif [ $exit_code -eq 5 ]; then
        # Exit code 5 means no tests were collected
        print_warning "No tests matched the given criteria"
        print_info "Test file: test/test_cluster.py"
        if [ -n "$keyword" ]; then
            print_info "Filter: Tests matching '$keyword'"
        fi
        exit 0
    else
        print_error "Some tests failed! (exit code: $exit_code)"
        print_info "Check the output above for details"
        exit $exit_code
    fi
}

# Run main
main "$@"
