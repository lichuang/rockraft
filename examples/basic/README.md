# Basic Rockraft Example

This is a basic example demonstrating how to use the `rockraft` library to create a Raft-based distributed key-value store.

## Overview

This example demonstrates:
- Creating a `RaftNode` instance with a custom configuration
- Initializing a single-node Raft cluster
- Writing data to the cluster using Raft's consensus protocol
- Monitoring cluster metrics

## Prerequisites

- Rust 2021 edition or later
- Cargo package manager

## Project Structure

```
basic_example/
├── Cargo.toml          # Project dependencies (references parent rockraft library)
├── src/
│   └── main.rs        # Main example code
└── README.md          # This file
```

## How to Run

From the `basic_example` directory:

```bash
# Build the example
cargo build

# Run the example
cargo run
```

Alternatively, from the root `rockraft` directory:

```bash
cargo run --example basic_example
```

## Example Output

When you run the example, you should see output similar to:

```
Creating Raft node with config:
  node_id: 1
  raft_addr: 127.0.0.1:6682
  data_path: /tmp/.tmpxxxxx/node1

Creating Raft node...
✓ Raft node created successfully!

Initializing single-node cluster with voters: {1}
✓ Cluster initialized successfully!

Waiting for initial log to be committed (log index 0)...
✓ Initial log committed successfully!

Writing data to cluster:
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
  Nodes: {1: RaftNode { node_id: 1, rpc_addr: "127.0.0.1:6682" }}

Example completed successfully!
Data directory: "/tmp/.tmpxxxxx/node1"
```

## Code Walkthrough

### 1. Configuration

The example creates a `Config` struct with:
- `node_id`: Unique identifier for this node (1)
- `raft_addr`: RPC address for this node
- `rocksdb`: Storage configuration

```rust
let config = Config {
    node_id: 1,
    raft_addr: "127.0.0.1:6682".to_string(),
    rocksdb: rockraft::config::RocksdbConfig { ... },
};
```

### 2. Node Creation

Create a new `RaftNode` instance using the configuration:

```rust
let raft_node = RaftNode::create(&config).await?;
```

### 3. Cluster Initialization

Initialize the cluster with a single voter node:

```rust
let mut voters = BTreeSet::new();
voters.insert(1);

raft_node.raft.initialize(voters).await?;
```

### 4. Writing Data

Write key-value pairs to the cluster using the Raft consensus protocol:

```rust
raft_node.raft.client_write(rockraft::raft::types::StorageData {
    key: key.clone(),
    value: value.clone(),
}).await?;
```

### 5. Monitoring

Check cluster metrics and status:

```rust
let metrics = raft_node.raft.metrics();
println!("Current Leader: {:?}", metrics.current_leader);
println!("State: {:?}", metrics.state);
```

## Next Steps

To build a production-ready Raft cluster:

1. **Run multiple nodes**: Create multiple `RaftNode` instances with different `node_id` and `raft_addr`
2. **Add gRPC server**: Implement a gRPC server to handle RPC requests from other nodes
3. **Implement reads**: Add methods to read data from the state machine
4. **Handle membership changes**: Implement logic to dynamically add or remove nodes from the cluster
5. **Add snapshotting**: Configure automatic snapshot creation to recover disk space
6. **Add proper error handling**: Implement retry logic and proper error handling for network issues

## Notes

- This example uses a temporary directory for data storage via `tempfile`
- The data directory is automatically cleaned up when the program exits
- In a real application, you would use a persistent data directory
- The example runs a single-node cluster for demonstration purposes
- For high availability, run at least 3 nodes in production

## Dependencies

- `rockraft`: The main Raft library (referenced via path from parent)
- `bytes`: For handling byte data
- `tokio`: Async runtime
- `tracing` & `tracing-subscriber`: For logging
- `tempfile`: For creating temporary directories during testing
