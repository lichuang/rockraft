# Basic Rockraft Example - Multi-Node Cluster

This is a basic example demonstrating how to use `rockraft` library to create a multi-node Raft-based distributed key-value store.

## Overview

This example demonstrates:

- Creating multiple `RaftNode` instances with different configurations
- Initializing a 3-node Raft cluster
- Loading configuration from TOML files
- Running multiple Raft nodes simultaneously
- Writing data to cluster using Raft's consensus protocol
- Monitoring cluster metrics across nodes

## Prerequisites

- Rust 2021 edition or later
- Cargo package manager

## Project Structure

```
basic_example/
├── Cargo.toml                # Project dependencies
├── conf/                     # Configuration files directory
│   ├── node1.toml           # Node 1 configuration
│   ├── node2.toml           # Node 2 configuration
│   └── node3.toml           # Node 3 configuration
├── src/
│   └── main.rs             # Main example code
└── README.md               # This file
```

## Configuration Files

Each node has its own TOML configuration file in the `conf/` directory:

### Node 1 (conf/node1.toml)

```toml
[node]
node_id = 1
raft_addr = "127.0.0.1:6682"

[rocksdb]
data_path = "/tmp/rockraft/node1"
max_open_files = 10000
```

### Node 2 (conf/node2.toml)

```toml
[node]
node_id = 2
raft_addr = "127.0.0.1:6683"

[rocksdb]
data_path = "/tmp/rockraft/node2"
max_open_files = 10000
```

### Node 3 (conf/node3.toml)

```toml
[node]
node_id = 3
raft_addr = "127.0.0.1:6684"

[rocksdb]
data_path = "/tmp/rockraft/node3"
max_open_files = 10000
```

## How to Run

### Step 1: Build the example

From the `examples/basic` directory:

```bash
cargo build
```

### Step 2: Start Node 1 (Leader)

Open a terminal and start node 1:

```bash
cargo run -- --conf conf/node1.toml
```

Node 1 will:

- Initialize the 3-node cluster
- Become the leader
- Write test data to the cluster

### Step 3: Start Node 2

Open another terminal and start node 2:

```bash
cargo run -- --conf conf/node2.toml
```

Node 2 will:

- Connect to the cluster
- Replicate data from the leader
- Wait for commands

### Step 4: Start Node 3

Open a third terminal and start node 3:

```bash
cargo run -- --conf conf/node3.toml
```

Node 3 will:

- Connect to the cluster
- Replicate data from the leader
- Wait for commands

### Step 5: Stop the nodes

Press `Ctrl+C` in each terminal to gracefully shut down the nodes.

## Example Output

### Node 1 Output (Leader)

```
Creating Raft node with config:
  node_id: 1
  raft_addr: 127.0.0.1:6682
  data_path: /tmp/rockraft/node1

Creating Raft node...
✓ Raft node created and gRPC service started successfully!

Initializing 3-node cluster...
✓ Cluster initialized successfully!

Waiting for initial log to be committed (log index 0)...
✓ Initial log committed successfully!

Node 1 is the leader, writing data to cluster...
  key: my_key
  value: b"my_value"
✓ Write result: log_id=Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 1 })

Waiting for write to be applied (log index 1)...
✓ Write applied successfully!

Writing second piece of data:
  key: another_key
  value: b"another_value"
✓ Write result: log_id=Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 2 })

Current Raft Metrics:
  Current Leader: Some(1)
  Current Term: 1
  Last Applied Index: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 2 })
  Last Log Index: Some(2)
  State: Leader
  Membership: ...

Raft node 1 is running. Press Ctrl+C to shutdown.
```

### Node 2 Output (Follower)

```
Creating Raft node with config:
  node_id: 2
  raft_addr: 127.0.0.1:6683
  data_path: /tmp/rockraft/node2

Creating Raft node...
✓ Raft node created and gRPC service started successfully!

Waiting for cluster to be initialized by node 1...
Waiting for initial log to be committed (log index 0)...
✓ Initial log committed successfully!

Current Raft Metrics:
  Current Leader: Some(1)
  Current Term: 1
  Last Applied Index: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 2 })
  Last Log Index: Some(2)
  State: Follower
  Membership: ...

Raft node 2 is running. Press Ctrl+C to shutdown.
```

### Node 3 Output (Follower)

```
Creating Raft node with config:
  node_id: 3
  raft_addr: 127.0.0.1:6684
  data_path: /tmp/rockraft/node3

Creating Raft node...
✓ Raft node created and gRPC service started successfully!

Waiting for cluster to be initialized by node 1...
Waiting for initial log to be committed (log index 0)...
✓ Initial log committed successfully!

Current Raft Metrics:
  Current Leader: Some(1)
  Current Term: 1
  Last Applied Index: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 2 })
  Last Log Index: Some(2)
  State: Follower
  Membership: ...

Raft node 3 is running. Press Ctrl+C to shutdown.
```

## Architecture

### RaftNodeBuilder Pattern

The example uses the `RaftNodeBuilder` pattern to create nodes:

```rust
let raft_node = RaftNodeBuilder::build(&config).await?;
```

This builder:

1. Creates the Raft instance with RocksDB storage
2. Sets up network communication
3. Starts the gRPC service automatically
4. Returns an `Arc<RaftNode>` ready to use

### Multi-Node Setup

- **Node 1**: Initializes the cluster as the leader
- **Nodes 2 & 3**: Join as followers and replicate data
- All nodes communicate via gRPC on their assigned ports (6682, 6683, 6684)

### Graceful Shutdown

Each node:

1. Listens for `Ctrl+C` signal
2. Calls `raft_node.shutdown().await`
3. Waits for gRPC service to finish
4. Exits cleanly

## Code Walkthrough

### 1. Configuration Loading

Load configuration from TOML file:

```rust
let config = load_config(config_path)?;
```

The `load_config` function reads and parses the TOML file into a `Config` struct.

### 2. Node Creation

Create a Raft node using the builder pattern:

```rust
let raft_node = RaftNodeBuilder::build(&config).await?;
```

This automatically starts both the Raft instance and the gRPC service.

### 3. Cluster Initialization (Node 1 only)

Only the first node initializes the cluster:

```rust
if config.node_id == 1 {
    let mut nodes = BTreeMap::new();
    nodes.insert(1, RaftNodeInfo { node_id: 1, rpc_addr: "127.0.0.1:6682".to_string() });
    nodes.insert(2, RaftNodeInfo { node_id: 2, rpc_addr: "127.0.0.1:6683".to_string() });
    nodes.insert(3, RaftNodeInfo { node_id: 3, rpc_addr: "127.0.0.1:6684".to_string() });

    raft_node.raft().initialize(nodes).await?;
}
```

### 4. Data Replication

Node 1 writes data, which is replicated to all followers:

```rust
raft_node.raft().client_write(StorageData { key, value }).await?;
```

### 5. Signal Handling

Handle Ctrl+C for graceful shutdown:

```rust
tokio::signal::ctrl_c().await?;
raft_node.shutdown().await?;
```

## Key Concepts

### Leader Election

- Node 1 becomes the initial leader by initializing the cluster
- If Node 1 fails, other nodes will elect a new leader
- Only the leader accepts write requests

### Log Replication

- The leader appends entries to its log
- Followers receive and replicate these entries via gRPC
- All nodes eventually reach consensus

### Fault Tolerance

- The cluster can tolerate 1 node failure (3 nodes total)
- If leader fails, followers elect a new leader
- Data is safely replicated across all nodes

## Next Steps

To build a production-ready Raft cluster:

1. **Implement membership changes**: Add logic to dynamically add or remove nodes
2. **Add snapshotting**: Configure automatic snapshot creation to recover disk space
3. **Implement read operations**: Add methods to read data from the state machine
4. **Add authentication**: Implement TLS and authentication for secure communication
5. **Health monitoring**: Add health checks and metrics endpoints
6. **Dynamic configuration**: Support runtime configuration updates
7. **Proper error handling**: Implement retry logic for network failures

## Notes

- Data is stored in `/tmp/rockraft/node{N}` directories
- Each node listens on a different port (6682, 6683, 6684)
- All nodes communicate via gRPC
- The cluster requires at least 2 nodes to maintain a quorum
- Start nodes in order (1, then 2, then 3) for proper initialization
- Press Ctrl+C in each terminal to shut down nodes gracefully

## Dependencies

- `rockraft`: The main Raft library
- `serde`: Serialization/deserialization framework
- `tokio`: Async runtime
- `tracing` & `tracing-subscriber`: Structured logging
- `toml`: Configuration file parsing
- `bytes`: For handling byte data

## Troubleshooting

### Port Already in Use

If you see "port already in use" error:

- Stop all running nodes with Ctrl+C
- Wait a few seconds
- Restart the nodes

### Cluster Initialization Failed

If nodes cannot connect:

- Ensure all nodes are started in order (1, 2, 3)
- Check that ports 6682, 6683, 6684 are not blocked by firewall
- Verify network connectivity between nodes

### Data Persistence

If data is not persisting:

- Check that `/tmp/rockraft` directory exists and is writable
- Ensure RocksDB has sufficient disk space
- Verify file permissions
