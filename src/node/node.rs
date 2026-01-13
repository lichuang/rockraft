use crate::error::Result;
use anyerror::AnyError;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

use openraft::Config as OpenRaftConfig;
use openraft::Raft;
use tonic::transport::Server;

use super::LeaderHandler;
use crate::config::Config;
use crate::engine::RocksDBEngine;
use crate::raft::grpc_client::ClientPool;
use crate::raft::network::NetworkFactory;
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::raft::store::column_family_list;
use crate::raft::store::RocksLogStore;
use crate::raft::store::RocksStateMachine;
use crate::raft::types::TypeConfig;
use crate::raft::types::{ForwardToLeader, NodeId};
use crate::service::RaftServiceImpl;

pub struct RaftNode {
  engine: Arc<RocksDBEngine>,
  raft: Arc<Raft<TypeConfig>>,

  state_machine: Arc<RocksStateMachine>,

  shutdown_tx: broadcast::Sender<()>,
  _shutdown_rx: broadcast::Receiver<()>,
  service_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RaftNode {
  /// Get a reference to the underlying Raft instance
  pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
    &self.raft
  }

  /// Shutdown raft thread
  pub async fn shutdown(&self) -> Result<()> {
    // Send shutdown signal
    let _ = self.shutdown_tx.send(());

    // Wait for the service task to finish if it exists
    let handle = self.service_handle.lock().unwrap().take();
    if let Some(h) = handle {
      h.await.ok();
    }

    Ok(())
  }

  pub async fn create(config: &Config) -> Result<Arc<Self>> {
    let engine = Arc::new(RocksDBEngine::new(
      &config.rocksdb.data_path,
      config.rocksdb.max_open_files,
      column_family_list(),
    ));

    let node_id = config.node_id;

    // Create log store
    let log_store = RocksLogStore::create(engine.db.clone())?;

    // Create state machine
    let data_dir = PathBuf::from(&config.rocksdb.data_path);
    let state_machine = RocksStateMachine::new(engine.db.clone(), data_dir).await?;

    // Create client pool for network connections
    let client_pool = Arc::new(ClientPool::new(10));

    // Create network factory
    let network = NetworkFactory::new(client_pool);

    // Configure Raft
    let raft_config = OpenRaftConfig::default();

    // Create Raft instance
    let raft = Arc::new(
      Raft::new(
        node_id,
        Arc::new(raft_config),
        network,
        log_store,
        state_machine.clone(),
      )
      .await?,
    );

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx_for_struct) = broadcast::channel(1);

    Ok(Arc::new(Self {
      engine,
      raft,
      state_machine: Arc::new(state_machine),
      shutdown_tx,
      _shutdown_rx: shutdown_rx_for_struct,
      service_handle: std::sync::Mutex::new(None),
    }))
  }

  /// Start the Raft gRPC service in a separate thread
  pub async fn start_raft_service(raft_node: Arc<Self>, config: &Config) -> Result<()> {
    let raft_addr = config.raft.addr.clone();

    // Subscribe to shutdown signal
    let mut shutdown_rx = raft_node.shutdown_tx.subscribe();

    // Clone raft_node for the spawned task
    let raft_node_for_service = raft_node.clone();

    // Spawn gRPC server in a separate thread/task
    let handle = tokio::task::spawn(async move {
      tracing::info!("Starting Raft gRPC service on {}", raft_addr);

      // Create gRPC service instance
      let raft_service = RaftServiceImpl::new(raft_node_for_service);

      // Create TCP listener
      let listener = match TcpListener::bind(&raft_addr).await {
        Ok(l) => l,
        Err(e) => {
          tracing::error!("Failed to bind gRPC server to {}: {}", raft_addr, e);
          return;
        }
      };

      tracing::info!("Raft gRPC service listening on {}", raft_addr);

      // Run the gRPC server with shutdown handling
      let server_future = Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

      tokio::select! {
        // Wait for shutdown signal
        _ = shutdown_rx.recv() => {
          tracing::info!("Raft gRPC service received shutdown signal, shutting down...");
        }

        // Wait for server to finish (if it errors)
        result = server_future => {
          match result {
            Ok(_) => tracing::info!("Raft gRPC service finished normally"),
            Err(e) => tracing::error!("Raft gRPC service error: {}", e),
          }
        }
      }

      tracing::info!("Raft gRPC service stopped");
    });

    // Store the handle in RaftNode
    *raft_node.service_handle.lock().unwrap() = Some(handle);
    tracing::info!("Raft gRPC service started");

    Ok(())
  }

  pub async fn get_leader(&self) -> Result<Option<NodeId>> {
    let deadline = Duration::from_millis(2000);
    let mut metrics_rx = self.raft.metrics();

    let result = timeout(deadline, async {
      loop {
        if let Some(leader) = metrics_rx.borrow().current_leader {
          return Ok(Some(leader));
        }
        if let Err(e) = metrics_rx.changed().await {
          // If changed() returns an error, the watch channel is closed
          // or receiver lagged. Return the error to the caller
          let error_msg = format!("Metrics watch error: {:?}", e);
          tracing::debug!("{}", error_msg);
          return Err(AnyError::error(error_msg).into());
        }
      }
    })
    .await;

    match result {
      Ok(inner_result) => inner_result,
      Err(_) => {
        // Timeout occurred
        Ok(None)
      }
    }
  }

  /// Assume's current node is a leader
  /// Returns Ok(LeaderHandler) if this node is the current leader
  /// Returns Err(ForwardToLeader) with the current leader information if this node is not a leader
  pub async fn assume_leader(&self) -> std::result::Result<LeaderHandler<'_>, ForwardToLeader> {
    let current_node_id = *self.raft.node_id();

    match self.get_leader().await {
      Ok(Some(leader_id)) => {
        if leader_id == current_node_id {
          Ok(LeaderHandler::new(self))
        } else {
          Err(ForwardToLeader {
            leader_id: Some(leader_id),
            leader_node: None,
          })
        }
      }
      Ok(None) => {
        // No leader found, return error without leader_id
        Err(ForwardToLeader {
          leader_id: None,
          leader_node: None,
        })
      }
      Err(_) => {
        // Error occurred while getting leader, assume we are not the leader
        Err(ForwardToLeader {
          leader_id: None,
          leader_node: None,
        })
      }
    }
  }

  /// Check if this node is in the cluster membership
  pub fn is_in_cluster(&self) -> Result<bool> {
    let last_membership = self
      .state_machine
      .get_last_membership()
      .map_err(|e| AnyError::error(format!("get_last_membership error: {}", e)))?;
    let node_id = *self.raft.node_id();

    // Only check voter_ids
    let is_voter = last_membership
      .membership()
      .voter_ids()
      .any(|id| id == node_id);

    Ok(is_voter)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::Config;
  use std::collections::{BTreeMap, BTreeSet};
  use tempfile::tempdir;

  /// Helper function to create a test config
  fn create_test_config(data_dir: &str, node_id: u64, addr: &str) -> Config {
    Config {
      node_id,
      raft: crate::config::RaftConfig {
        addr: addr.to_string(),
        single: true,
        join: vec![],
      },
      rocksdb: crate::config::RocksdbConfig {
        data_path: data_dir.to_string(),
        max_open_files: 1024,
      },
    }
  }

  /// Helper function to set up membership for testing
  async fn setup_membership(raft_node: &RaftNode, node_ids: Vec<u64>) {
    let sm = &raft_node.state_machine;
    let mut nodes = BTreeMap::new();

    for node_id in node_ids {
      nodes.insert(
        node_id,
        crate::raft::types::Node {
          node_id,
          endpoint: crate::raft::types::Endpoint::new("127.0.0.1", 1000 + node_id as u32),
        },
      );
    }

    let voter_ids: BTreeSet<u64> = nodes.keys().cloned().collect();
    let membership = openraft::Membership::new(vec![voter_ids], nodes).unwrap();
    let log_id = crate::raft::types::LogId {
      leader_id: crate::raft::types::LeaderId {
        term: 1,
        node_id: 1,
      },
      index: 1,
    };

    let stored_membership = openraft::StoredMembership::new(Some(log_id), membership);
    sm.set_last_membership(&stored_membership).unwrap();
  }

  #[tokio::test]
  async fn test_is_in_cluster_node_exists() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();

    // Create RaftNode with node_id=1
    let config = create_test_config(&data_path, 1, "127.0.0.1:5001");
    let raft_node = RaftNode::create(&config).await?;

    // Set up membership with node 1
    setup_membership(&raft_node, vec![1, 2, 3]).await;

    // Check if node 1 is in cluster
    let result = raft_node.is_in_cluster()?;
    assert!(result, "Node 1 should be in the cluster");

    Ok(())
  }

  #[tokio::test]
  async fn test_is_in_cluster_node_not_exists() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();

    // Create RaftNode with node_id=4 (not in membership)
    let config = create_test_config(&data_path, 4, "127.0.0.1:5004");
    let raft_node = RaftNode::create(&config).await?;

    // Set up membership with nodes 1, 2, 3 (not including node 4)
    setup_membership(&raft_node, vec![1, 2, 3]).await;

    // Check if node 4 is in cluster
    let result = raft_node.is_in_cluster()?;
    assert!(!result, "Node 4 should not be in the cluster");

    Ok(())
  }
}
