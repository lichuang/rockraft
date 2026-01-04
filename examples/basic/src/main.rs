use bytes::Bytes;
use rockraft::config::Config;
use rockraft::node::RaftNodeBuilder;
use rockraft::raft::types::RaftNode as RaftNodeInfo;
use std::collections::BTreeMap;
use tempfile::tempdir;

/// Basic usage example demonstrating how to create and use a RaftNode
///
/// This example shows:
/// 1. Creating a configuration
/// 2. Initializing a Raft node
/// 3. Setting up a single-node cluster
/// 4. Writing data to Raft cluster
/// 5. Reading cluster metrics
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Initialize tracing for logging
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();

  // Create a temporary directory for RocksDB data
  let temp_dir = tempdir()?;
  let data_path = temp_dir.path().join("node1");

  // Step 1: Create a configuration
  let config = Config {
    node_id: 1,
    raft_addr: "127.0.0.1:6682".to_string(),
    rocksdb: rockraft::config::RocksdbConfig {
      data_path: data_path.to_string_lossy().to_string(),
      max_open_files: 10000,
    },
  };

  println!("Creating Raft node with config:");
  println!("  node_id: {}", config.node_id);
  println!("  raft_addr: {}", config.raft_addr);
  println!("  data_path: {}", config.rocksdb.data_path);

  // Step 2: Create and initialize Raft node
  println!("\nCreating Raft node...");
  let raft_node = RaftNodeBuilder::build(&config).await?;
  println!("✓ Raft node created and gRPC service started successfully!");

  // Step 3: Initialize cluster with a single voter
  println!("\nInitializing single-node cluster...");

  // Create node configuration for initial cluster member
  let mut nodes = BTreeMap::new();
  nodes.insert(
    1,
    RaftNodeInfo {
      node_id: 1,
      rpc_addr: "127.0.0.1:6682".to_string(),
    },
  );

  // Initialize cluster with single voter node
  raft_node.raft().initialize(nodes).await?;
  println!("✓ Cluster initialized successfully!");

  // Wait for the initial log to be committed
  println!("\nWaiting for initial log to be committed (log index 0)...");
  let timeout = Some(std::time::Duration::from_secs(5));
  raft_node
    .raft()
    .wait(timeout)
    .applied_index(Some(0), "init")
    .await?;
  println!("✓ Initial log committed successfully!");

  // Step 4: Write some data to the cluster
  let key = "my_key".to_string();
  let value = Bytes::from("my_value");

  println!("\nWriting data to cluster:");
  println!("  key: {}", key);
  println!("  value: {:?}", value);

  let response = raft_node
    .raft()
    .client_write(rockraft::raft::types::StorageData {
      key: key.clone(),
      value: value.clone(),
    })
    .await?;

  println!("✓ Write result: log_id={:?}", response.log_id);

  // Wait for write to be applied
  let log_id = response.log_id;
  println!(
    "\nWaiting for write to be applied (log index {})...",
    log_id.index
  );
  raft_node
    .raft()
    .wait(timeout)
    .applied_index(Some(log_id.index), "write")
    .await?;
  println!("✓ Write applied successfully!");

  // Write another piece of data
  let key2 = "another_key".to_string();
  let value2 = Bytes::from("another_value");

  println!("\nWriting second piece of data:");
  println!("  key: {}", key2);
  println!("  value: {:?}", value2);

  let response2 = raft_node
    .raft()
    .client_write(rockraft::raft::types::StorageData {
      key: key2.clone(),
      value: value2.clone(),
    })
    .await?;

  println!("✓ Write result: log_id={:?}", response2.log_id);

  // Step 5: Print cluster statistics
  let metrics = raft_node.raft().metrics().borrow().clone();
  println!("\nCurrent Raft Metrics:");
  println!("  Current Leader: {:?}", metrics.current_leader);
  println!("  Current Term: {}", metrics.current_term);
  println!("  Last Applied Index: {:?}", metrics.last_applied);
  println!("  Last Log Index: {:?}", metrics.last_log_index);
  println!("  State: {:?}", metrics.running_state);
  println!("  Membership: {:?}", metrics.membership_config);

  // Additional info
  // Step 6: Shutdown the Raft node gracefully
  println!("\nShutting down Raft node...");
  raft_node.shutdown().await?;
  println!("✓ Raft node shutdown successfully!");

  // Additional info
  println!("\nExample completed successfully!");
  println!("Data directory: {:?}", data_path);
  println!("Note: Data directory will be cleaned up when tempdir goes out of scope");
  println!("\nNote: In a real application, you would:");
  println!("  - Run multiple nodes for high availability");
  println!("  - Add nodes using: raft.add_learner() and raft.change_membership()");
  println!("  - Call raft.shutdown() when stopping the application");
  println!("  - Implement read operations to query the state machine");

  Ok(())
}
