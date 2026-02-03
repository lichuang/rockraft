#!/bin/bash
#
# start.sh - Start all nodes in the Raft cluster
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NODES=("node1" "node2" "node3")
PIDS=()
LOG_DIR="logs"
BIN="target/debug/cluster_example"
DEFAULT_LOG_LEVEL="info"

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

# Function to check if a node is already running
is_node_running() {
    local node=$1
    local pid_file="${LOG_DIR}/${node}.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        else
            rm -f "$pid_file"
            return 1
        fi
    fi
    return 1
}

# Function to start a single node
start_node() {
    local node=$1
    local conf="conf/${node}.toml"
    local log_level=$2

    if is_node_running "$node"; then
        print_warning "Node $node is already running (PID: $(cat ${LOG_DIR}/${node}.pid))"
        return 1
    fi

    if [ ! -f "$conf" ]; then
        print_error "Configuration file $conf not found"
        return 1
    fi

    if [ ! -f "$BIN" ]; then
        print_error "Binary $BIN not found. Please run 'cargo build' first"
        return 1
    fi

    print_info "Starting node $node (log level: $log_level)..."

    # Create log directory if it doesn't exist
    mkdir -p "$LOG_DIR"

    # Build RUST_LOG based on log level
    local rust_log
    case "$log_level" in
        debug|trace)
            rust_log="rockraft=${log_level},rockraft_cluster=${log_level},tower_http=info,axum=info"
            ;;
        info)
            rust_log="rockraft=info,rockraft_cluster=info,tower_http=info,axum=info"
            ;;
        warn|warning)
            rust_log="rockraft=warn,rockraft_cluster=warn,tower_http=warn,axum=warn"
            ;;
        error)
            rust_log="rockraft=error,rockraft_cluster=error,tower_http=error,axum=error"
            ;;
        *)
            rust_log="$log_level"
            ;;
    esac

    # Start the node in background, redirecting output to log file
    nohup env RUST_LOG="$rust_log" "$BIN" --conf "$conf" > "${LOG_DIR}/${node}.log" 2>&1 &
    local pid=$!

    # Save PID for later use
    echo $pid > "${LOG_DIR}/${node}.pid"

    # Wait a bit and check if the process is still running
    sleep 1
    if ps -p $pid > /dev/null 2>&1; then
        print_success "Node $node started successfully (PID: $pid)"
        print_info "  Log: ${LOG_DIR}/${node}.log"
        PIDS+=($pid)
        return 0
    else
        print_error "Failed to start node $node"
        rm -f "${LOG_DIR}/${node}.pid"
        return 1
    fi
}

# Function to stop all nodes
stop_all_nodes() {
    print_info "Stopping all nodes..."

    for node in "${NODES[@]}"; do
        local pid_file="${LOG_DIR}/${node}.pid"
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                print_info "Stopping $node (PID: $pid)..."
                kill $pid
                # Wait for process to terminate
                local count=0
                while ps -p $pid > /dev/null 2>&1 && [ $count -lt 10 ]; do
                    sleep 1
                    count=$((count + 1))
                done
                if ps -p $pid > /dev/null 2>&1; then
                    print_warning "Force killing $node (PID: $pid)"
                    kill -9 $pid
                fi
                print_success "Node $node stopped"
            fi
            rm -f "$pid_file"
        fi
    done
}

# Function to check status of all nodes
check_status() {
    print_info "Cluster status:"
    echo ""

    for node in "${NODES[@]}"; do
        local pid_file="${LOG_DIR}/${node}.pid"
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                local http_port=$(grep "http_addr" "conf/${node}.toml" | cut -d'"' -f2 | cut -d':' -f2)
                echo -e "  ${GREEN}●${NC} $node - Running (PID: $pid, HTTP: $http_port)"
            else
                echo -e "  ${RED}○${NC} $node - Stopped (stale PID file)"
                rm -f "$pid_file"
            fi
        else
            echo -e "  ${RED}○${NC} $node - Stopped"
        fi
    done
    echo ""
}

# Function to show logs for a specific node
show_logs() {
    local node=$1
    local log_file="${LOG_DIR}/${node}.log"

    if [ ! -f "$log_file" ]; then
        print_error "Log file for $node not found"
        return 1
    fi

    print_info "Showing logs for $node (Ctrl+C to exit)..."
    echo ""
    tail -f "$log_file"
}

# Function to clean up logs
cleanup_logs() {
    print_info "Cleaning up log files..."
    rm -rf "$LOG_DIR"
    print_success "Log files cleaned"
}

# Function to test the cluster with curl
test_cluster() {
    print_info "Testing cluster..."

    # Get health of all nodes
    for i in 1 2 3; do
        echo ""
        print_info "Checking node $i..."
        curl -s "http://127.0.0.1:800$i/health" | jq '.' 2>/dev/null || curl -s "http://127.0.0.1:800$i/health"
        sleep 0.5
    done

    echo ""
    print_info "Setting a test key..."
    curl -s -X POST "http://127.0.0.1:8001/set" \
        -H "Content-Type: application/json" \
        -d '{"key": "test:timestamp", "value": "'$(date)'"}' \
        | jq '.' 2>/dev/null || curl -s -X POST "http://127.0.0.1:8001/set" \
        -H "Content-Type: application/json" \
        -d '{"key": "test:timestamp", "value": "'$(date)'"}'

    echo ""
    print_info "Reading the test key from all nodes..."
    for i in 1 2 3; do
        echo -n "  Node $i: "
        curl -s "http://127.0.0.1:800$i/get?key=test:timestamp" | jq '.value' 2>/dev/null || \
        curl -s "http://127.0.0.1:800$i/get?key=test:timestamp"
    done
    echo ""
}

# Main script logic
case "$1" in
    start)
        local log_level="$2"
        if [ -z "$log_level" ]; then
            log_level="$DEFAULT_LOG_LEVEL"
        fi

        print_info "Starting all nodes in the cluster (log level: $log_level)..."
        echo ""

        # Start nodes sequentially with a small delay
        for node in "${NODES[@]}"; do
            start_node "$node" "$log_level"
            if [ $? -eq 0 ]; then
                # Wait a bit before starting the next node
                sleep 2
            fi
        done

        echo ""
        print_success "Cluster startup complete!"
        echo ""
        print_info "To view logs, run: $0 logs <node>"
        print_info "To check status, run: $0 status"
        print_info "To stop cluster, run: $0 stop"
        print_info "To test cluster, run: $0 test"
        echo ""
        check_status
        ;;

    stop)
        stop_all_nodes
        print_success "All nodes stopped"
        ;;

    restart)
        local log_level="$2"
        stop_all_nodes
        sleep 2
        if [ -n "$log_level" ]; then
            $0 start "$log_level"
        else
            $0 start
        fi
        ;;

    status)
        check_status
        ;;

    logs)
        if [ -z "$2" ]; then
            print_error "Please specify a node: $0 logs <node>"
            echo "Available nodes: ${NODES[@]}"
            exit 1
        fi

        if [[ ! " ${NODES[@]} " =~ " $2 " ]]; then
            print_error "Invalid node: $2"
            echo "Available nodes: ${NODES[@]}"
            exit 1
        fi

        show_logs "$2"
        ;;

    test)
        test_cluster
        ;;

    clean)
        cleanup_logs
        ;;

    *)
        echo "Usage: $0 {start|stop|restart|status|logs|test|clean}"
        echo ""
        echo "Commands:"
        echo "  start [level]  - Start all cluster nodes (log level: info|debug|trace|warn|error)"
        echo "  stop           - Stop all cluster nodes"
        echo "  restart [level]- Restart all cluster nodes"
        echo "  status         - Show status of all nodes"
        echo "  logs <node>   - Show logs for a specific node (tail -f)"
        echo "  test           - Test the cluster with curl"
        echo "  clean          - Clean up log files"
        echo ""
        echo "Examples:"
        echo "  $0 start              # Start all nodes with info level"
        echo "  $0 start debug        # Start all nodes with debug level"
        echo "  $0 start trace         # Start all nodes with trace level"
        echo "  $0 status             # Check cluster status"
        echo "  $0 logs node1         # View node1 logs"
        echo "  $0 test               # Test cluster"
        echo "  $0 stop               # Stop all nodes"
        exit 1
        ;;
esac
