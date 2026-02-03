# Cluster Example - HTTP API with Raft Cluster

This example demonstrates how to build a distributed key-value store with HTTP API using `rockraft` in cluster mode.

## Overview

This example shows:
- Creating a multi-node Raft cluster (3 nodes)
- Cluster mode with `join` configuration
- HTTP API for read/write operations
- Leader election and automatic failover
- Data replication across all nodes

## Architecture

```
                    HTTP Clients
                        |
                        v
+-----------------+  +-----------------+  +-----------------+
|   Node 1        |  |   Node 2        |  |   Node 3        |
| (HTTP: 8001)    |  | (HTTP: 8002)    |  | (HTTP: 8003)    |
| (Raft: 7001)    |  | (Raft: 7002)    |  | (Raft: 7003)    |
|                 |  |                 |  |                 |
| RocksDB         |  | RocksDB         |  | RocksDB         |
|                 |  |                 |  |                 |
+--------+--------+  +--------+--------+  +--------+--------+
         |                      |                      |
         +---------- Raft Consensus (gRPC) ---------+
```

## Project Structure

```
cluster/
├── Cargo.toml              # Project dependencies
├── start.sh                # Cluster management script
├── conf/                   # Configuration files
│   ├── node1.toml         # Node 1 configuration
│   ├── node2.toml         # Node 2 configuration
│   └── node3.toml         # Node 3 configuration
├── src/
│   └── main.rs            # HTTP server with Raft integration
├── logs/                   # Log directory (created by start.sh)
└── README.md              # This file
```

## HTTP API Endpoints

### GET /get?key={key}

Get a value by key.

**Example:**
```bash
curl "http://localhost:8001/get?key=mykey"
```

**Response:**
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```

If key doesn't exist:
```json
{
  "key": "mykey",
  "value": null
}
```

### POST /set

Set a key-value pair (only works on leader node).

**Example:**
```bash
curl -X POST http://localhost:8001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey", "value": "myvalue"}'
```

**Response (Success):**
```json
{
  "success": true,
  "message": "Key 'mykey' set successfully"
}
```

**Response (Not Leader):**
```json
{
  "error": "This node is not the leader. Current leader: 1"
}
```

### POST /delete

Delete a key (only works on leader node).

**Example:**
```bash
curl -X POST http://localhost:8001/delete \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey"}'
```

**Response:**
```json
{
  "success": true,
  "message": "Key 'mykey' deleted successfully"
}
```

### GET /health

Health check endpoint.

**Example:**
```bash
curl http://localhost:8001/health
```

**Response:**
```json
{
  "status": "ok",
  "node_id": 1,
  "is_leader": true,
  "current_leader": 1,
  "current_term": 1,
  "state": "Leader"
}
```

### GET /metrics

Get detailed cluster metrics.

**Example:**
```bash
curl http://localhost:8001/metrics
```

**Response:**
```json
{
  "node_id": 1,
  "current_leader": 1,
  "current_term": 1,
  "last_applied": { ... },
  "last_log_index": 2,
  "state": "Leader",
  "membership": { ... }
}
```

## Cluster Management Script

The `start.sh` script provides an easy way to manage the Raft cluster.

### Script Features

- **One-command startup**: Start all three nodes with `./start.sh start`
- **Status monitoring**: Check which nodes are running with `./start.sh status`
- **Log viewing**: View real-time logs for any node with `./start.sh logs <node>`
- **Cluster testing**: Automated test script to verify cluster functionality
- **Graceful shutdown**: Stop all nodes with proper cleanup
- **Automatic cleanup**: Clean up log files when needed

### Script Commands

| Command | Description |
|----------|-------------|
| `./start.sh start` | Start all three cluster nodes sequentially |
| `./start.sh stop` | Stop all running cluster nodes |
| `./start.sh restart` | Stop and restart all nodes |
| `./start.sh status` | Show status of all nodes (running/stopped) |
| `./start.sh logs <node>` | View real-time logs for a specific node |
| `./start.sh test` | Test cluster health and data replication |
| `./start.sh clean` | Remove all log files in `logs/` directory |

### Usage Examples

```bash
# Build the project first
cargo build

# Start the cluster
./start.sh start

# Check status after a few seconds
./start.sh status

# View logs from node 1
./start.sh logs node1

# Test the cluster (writes a key and reads from all nodes)
./start.sh test

# Stop all nodes when done
./start.sh stop
```

### Log Files

The script creates a `logs/` directory containing:

```
logs/
├── node1.log       # Node 1 output
├── node1.pid       # Node 1 process ID
├── node2.log       # Node 2 output
├── node2.pid       # Node 2 process ID
├── node3.log       # Node 3 output
└── node3.pid       # Node 3 process ID
```

## Configuration Files

Each node has its own TOML configuration file in the `conf/` directory.

### Node 1 (conf/node1.toml)

```toml
node_id = 1
raft_addr = "127.0.0.1:7001"
http_addr = "127.0.0.1:8001"
single = false
join = ["127.0.0.1:7002", "127.0.0.1:7003"]
data_path = "/tmp/rockraft_cluster/node1"
max_open_files = 10000
```

### Node 2 (conf/node2.toml)

```toml
node_id = 2
raft_addr = "127.0.0.1:7002"
http_addr = "127.0.0.1:8002"
single = false
join = ["127.0.0.1:7001"]
data_path = "/tmp/rockraft_cluster/node2"
max_open_files = 10000
```

### Node 3 (conf/node3.toml)

```toml
node_id = 3
raft_addr = "127.0.0.1:7003"
http_addr = "127.0.0.1:8003"
single = false
join = ["127.0.0.1:7001"]
data_path = "/tmp/rockraft_cluster/node3"
max_open_files = 10000
```

## How to Run

### Step 1: Build the example

From `examples/cluster` directory:

```bash
cargo build
```

### Step 2: Start the cluster

**Option A: Using the start script (recommended)**

The `start.sh` script provides an easy way to manage the cluster:

```bash
# Start all three nodes
./start.sh start

# Check cluster status
./start.sh status

# View logs for a specific node
./start.sh logs node1

# Test the cluster
./start.sh test

# Stop all nodes
./start.sh stop

# Restart all nodes
./start.sh restart

# Clean up log files
./start.sh clean
```

**Option B: Manually starting each node**

Open three separate terminals and start the nodes:

**Terminal 1 - Node 1:**
```bash
cargo run -- --conf conf/node1.toml
```

**Terminal 2 - Node 2:**
```bash
cargo run -- --conf conf/node2.toml
```

**Terminal 3 - Node 3:**
```bash
cargo run -- --conf conf/node3.toml
```

### Step 3: Test the cluster

#### Check health of all nodes:
```bash
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

#### Set a key-value pair (write to leader):
```bash
curl -X POST http://localhost:8001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "Alice"}'
```

#### Get the value (read from any node):
```bash
curl "http://localhost:8001/get?key=user:1"
curl "http://localhost:8002/get?key=user:1"
curl "http://localhost:8003/get?key=user:1"
```

#### Set more data:
```bash
curl -X POST http://localhost:8001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "user:2", "value": "Bob"}'

curl -X POST http://localhost:8001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "config:theme", "value": "dark"}'
```

#### Delete a key:
```bash
curl -X POST http://localhost:8001/delete \
  -H "Content-Type: application/json" \
  -d '{"key": "config:theme"}'
```

### Step 4: Stop the nodes

Press `Ctrl+C` in each terminal to gracefully shut down the nodes.

## Example Output

### Node 1 Startup Output

```
Configuration loaded:
  node_id: 1
  raft_addr: 127.0.0.1:7001
  http_addr: 127.0.0.1:8001
  data_path: /tmp/rockraft_cluster/node1
  single: false
  join: ["127.0.0.1:7002", "127.0.0.1:7003"]

Creating Raft node...
✓ Raft node created successfully!

Initializing 3-node cluster...
✓ Cluster initialized successfully!

Waiting for initial log to be committed...
✓ Initial log committed successfully!

HTTP server listening on http://127.0.0.1:8001

Available endpoints:
  GET  http://127.0.0.1:8001/get?key=<key>    - Get a value by key
  POST http://127.0.0.1:8001/set             - Set a key-value pair
  POST http://127.0.0.1:8001/delete          - Delete a key
  GET  http://127.0.0.1:8001/health          - Health check
  GET  http://127.0.0.1:8001/metrics         - Cluster metrics

Press Ctrl+C to shutdown...
```

### Node 2 Startup Output

```
Configuration loaded:
  node_id: 2
  raft_addr: 127.0.0.1:7002
  http_addr: 127.0.0.1:8002
  data_path: /tmp/rockraft_cluster/node2
  single: false
  join: ["127.0.0.1:7001"]

Creating Raft node...
✓ Raft node created successfully!

Waiting for cluster to be ready...
Waiting for initial log to be committed...
✓ Initial log committed successfully!

HTTP server listening on http://127.0.0.1:8002

Available endpoints:
  GET  http://127.0.0.1:8002/get?key=<key>    - Get a value by key
  POST http://127.0.0.1:8002/set             - Set a key-value pair
  POST http://127.0.0.1:8002/delete          - Delete a key
  GET  http://127.0.0.1:8002/health          - Health check
  GET  http://127.0.0.1:8002/metrics         - Cluster metrics

Press Ctrl+C to shutdown...
```

## Key Concepts

### Cluster Mode

- All nodes are configured with `single = false` and `join` list
- Node 1 initializes the cluster with all 3 members
- Node 2 and 3 join the cluster via the `join` configuration
- Data is automatically replicated across all nodes

### Leader Election

- Node 1 becomes the initial leader by initializing the cluster
- If Node 1 fails, Nodes 2 and 3 will elect a new leader
- Only the leader accepts write requests (`POST /set`, `POST /delete`)
- All nodes can serve read requests (`GET /get`)

### Data Consistency

- Write operations go through Raft consensus
- All nodes maintain identical data through log replication
- Reads return locally stored data (eventually consistent)

### Fault Tolerance

- The cluster can tolerate 1 node failure (3 nodes total)
- If leader fails, followers elect a new leader automatically
- Data is safely replicated across all nodes

## Implementation Details

### Write Path (POST /set)

1. Client sends POST request to any node
2. Node checks if it's the leader via `assume_leader()`
3. If leader: writes to Raft via `client_write()`
4. If not leader: returns error with leader information
5. Raft replicates the write to all followers
6. Write is committed once majority acknowledges
7. State machine applies the write to RocksDB

### Read Path (GET /get)

1. Client sends GET request to any node
2. Node reads directly from local RocksDB
3. Returns value or null if key doesn't exist
4. No Raft consensus required for reads

## Next Steps

To build a production-ready Raft cluster:

1. **Implement TLS**: Add HTTPS support for secure communication
2. **Authentication**: Add JWT or token-based authentication
3. **Load Balancing**: Use a load balancer to distribute HTTP requests
4. **Monitoring**: Add Prometheus metrics and Grafana dashboards
5. **Backup**: Implement automated backup of RocksDB data
6. **Cluster Management**: Add API endpoints for adding/removing nodes
7. **Read Consistency**: Implement linearizable reads if needed
8. **Rate Limiting**: Add rate limiting to prevent abuse

## Dependencies

- `rockraft`: The main Raft library
- `axum`: HTTP web framework
- `tower-http`: HTTP middleware (CORS, tracing)
- `tokio`: Async runtime
- `rocksdb`: Embedded database
- `serde` & `serde_json`: Serialization
- `toml`: Configuration file parsing
- `tracing`: Structured logging

## Troubleshooting

### Port Already in Use

If you see "port already in use" error:
- Stop all running nodes with Ctrl+C
- Check if ports are still in use: `lsof -i :7001 -i :8001`
- Restart the nodes

### Nodes Cannot Connect

If nodes cannot join the cluster:
- Ensure nodes are started in the correct order (node1 first)
- Check that ports 7001-7003 are not blocked by firewall
- Verify network connectivity between nodes
- Check the `join` addresses in configuration files

### Write Operations Fail

If `POST /set` fails with "not leader" error:
- The node you're writing to is not the leader
- Check `/health` endpoint to find the current leader
- Direct writes to the leader node's HTTP port
- Or implement client-side leader redirection

### Data Not Persisting

If data is not persisting:
- Check that `/tmp/rockraft_cluster` directory exists and is writable
- Ensure RocksDB has sufficient disk space
- Verify file permissions
- Check logs for errors

## Notes

- Data is stored in `/tmp/rockraft_cluster/node{N}` directories
- Raft communication uses ports 7001-7003 (gRPC)
- HTTP API uses ports 8001-8003
- All nodes communicate via gRPC for Raft consensus
- The cluster requires at least 2 nodes to maintain a quorum
- Node 1 initializes the cluster, others join via configuration
- **Use `start.sh` script** for easy cluster management:
  - Run `./start.sh start` to launch all nodes
  - Run `./start.sh status` to check cluster health
  - Run `./start.sh logs <node>` to view node logs
  - Run `./start.sh stop` to gracefully shut down all nodes
- If starting nodes manually, press Ctrl+C in each terminal to shut down
