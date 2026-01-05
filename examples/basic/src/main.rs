use bytes::Bytes;
use rockraft::config::Config;
use rockraft::node::RaftNodeBuilder;
use rockraft::raft::types::RaftNode as RaftNodeInfo;
use std::collections::BTreeMap;
use std::env;
use std::fs;

/// Basic usage example demonstrating how to create and use a RaftNode
///
/// Usage:
///   cargo run -- --conf conf/node1.toml
///   cargo run -- --conf conf/node2.toml
///   cargo run -- --conf conf/node3.toml
///
/// This example shows:
/// 1. Loading configuration from TOML file
/// 2. Initializing a Raft node
/// 3. Setting up a multi-node cluster
/// 4. Writing data to Raft cluster
/// 5. Reading cluster metrics
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Initialize tracing for logging
  tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();

  // Parse command line arguments
  let args: Vec<String> = env::args().collect();

  let config_path = if args.len() > 2 && args[1] == "--conf" {
    &args[2]
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    std::process::exit(1);
  };

  // Load configuration from TOML file
  let config = load_config(config_path)?;

  println!("Creating Raft node with config:");
  println!("  node_id: {}", config.node_id);
  println!("  raft_addr: {}", config.raft_addr);
  println!("  data_path: {}", config.rocksdb.data_path);

  // Step 1: Create and initialize Raft node
  println!("\nCreating Raft node...");
  let raft_node = RaftNodeBuilder::build(&config).await?;
  println!("✓ Raft node created and gRPC service started successfully!");

  // Step 2: For node 1, initialize the cluster
  if config.node_id == 1 {
    println!("\nInitializing 3-node cluster...");

    // Create node configuration for initial cluster members
    let mut nodes = BTreeMap::new();

    // Node 1
    nodes.insert(
      1,
      RaftNodeInfo {
        node_id: 1,
        rpc_addr: "127.0.0.1:6682".to_string(),
      },
    );

    // Node 2
    nodes.insert(
      2,
      RaftNodeInfo {
        node_id: 2,
        rpc_addr: "127.0.0.1:6683".to_string(),
      },
    );

    // Node 3
    nodes.insert(
      3,
      RaftNodeInfo {
        node_id: 3,
        rpc_addr: "127.0.0.1:6684".to_string(),
      },
    );

    // Initialize cluster with three voter nodes
    raft_node.raft().initialize(nodes).await?;
    println!("✓ Cluster initialized successfully!");
  } else {
    // For other nodes, wait a bit for cluster to be initialized
    println!("\nWaiting for cluster to be initialized by node 1...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
  }

  // Step 3: Wait for the initial log to be committed
  println!("\nWaiting for initial log to be committed (log index 0)...");
  let timeout = Some(std::time::Duration::from_secs(10));
  raft_node
    .raft()
    .wait(timeout)
    .applied_index(Some(0), "init")
    .await?;
  println!("✓ Initial log committed successfully!");

  // Step 4: Only node 1 writes some data to the cluster
  if config.node_id == 1 {
    println!("\nNode 1 is the leader, writing data to cluster...");

    let key = "my_key".to_string();
    let value = Bytes::from("my_value");

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
  }

  // Step 5: Print cluster statistics
  let metrics = raft_node.raft().metrics().borrow().clone();
  println!("\nCurrent Raft Metrics:");
  println!("  Current Leader: {:?}", metrics.current_leader);
  println!("  Current Term: {}", metrics.current_term);
  println!("  Last Applied Index: {:?}", metrics.last_applied);
  println!("  Last Log Index: {:?}", metrics.last_log_index);
  println!("  State: {:?}", metrics.running_state);
  println!("  Membership: {:?}", metrics.membership_config);

  // Step 6: Keep the node running
  println!(
    "\nRaft node {} is running. Press Ctrl+C to shutdown.",
    config.node_id
  );

  // Set up Ctrl+C handler
  tokio::signal::ctrl_c().await?;

  // Step 7: Shutdown the Raft node gracefully
  println!("\nShutting down Raft node {}...", config.node_id);
  raft_node.shutdown().await?;
  println!("✓ Raft node shutdown successfully!");

  println!("\nExample completed successfully!");
  println!("\nNote: In a real application, you would:");
  println!("  - Run multiple nodes for high availability (as configured)");
  println!("  - Call raft.shutdown() when stopping the application");
  println!("  - Implement read operations to query the state machine");
  println!("  - Add error handling and retry logic for network failures");

  Ok(())
}

/// Load configuration from a TOML file
fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
  let config_str = fs::read_to_string(path)
    .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

  let config: Config = toml::from_str(&config_str)
    .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

  Ok(config)
}
