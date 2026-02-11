use crate::error::ManagementError;
use crate::error::Result;
use crate::error::RockRaftError;
use crate::error::StartupError;
use crate::grpc::JoinConnectionFactory;
use crate::raft::protobuf::raft_service_client::RaftServiceClient;
use crate::raft::protobuf as pb;
use crate::raft::types::ForwardRequestBody;
use crate::raft::types::JoinRequest;
use anyerror::AnyError;
use openraft::error::{InitializeError, RaftError};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
use tracing::debug;
use tracing::error;
use tracing::info;

use std::result::Result as StdResult;
use tokio::time::sleep;

use openraft::Config as OpenRaftConfig;
use openraft::Raft;
use tonic::Status;
use tonic::transport::Server;

use super::LeaderHandler;
use super::parsed_config::ParsedConfig;
use crate::config::Config;
use crate::config::RaftConfig;
use crate::engine::RocksDBEngine;
use crate::raft::grpc_client::ClientPool;
use crate::raft::network::NetworkFactory;
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::raft::store::RocksLogStore;
use crate::raft::store::RocksStateMachine;
use crate::raft::store::column_family_list;
use crate::raft::types::{Endpoint, ForwardRequest, ForwardResponse, Node, TypeConfig};
use crate::raft::types::{ForwardToLeader, NodeId};
use crate::service::RaftServiceImpl;

pub struct RaftNode {
  engine: Arc<RocksDBEngine>,
  raft: Arc<Raft<TypeConfig>>,

  config: ParsedConfig,

  factory: NetworkFactory,

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
    let factory = NetworkFactory::new(client_pool);

    // Configure Raft
    let raft_config = OpenRaftConfig::default();

    // Create Raft instance
    let raft = Arc::new(
      Raft::new(
        node_id,
        Arc::new(raft_config),
        factory.clone(),
        log_store,
        state_machine.clone(),
      )
      .await
      .map_err(|e| crate::error::OpenRaft::Fatal(e))?,
    );

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx_for_struct) = broadcast::channel(1);

    Ok(Arc::new(Self {
      engine,
      raft,
      config: ParsedConfig::from(config)?,
      factory,
      state_machine: Arc::new(state_machine),
      shutdown_tx,
      _shutdown_rx: shutdown_rx_for_struct,
      service_handle: std::sync::Mutex::new(None),
    }))
  }

  pub async fn start(raft_node: Arc<Self>) -> Result<()> {
    let config = &raft_node.config;
    if config.raft_single {
      let node = Node {
        node_id: config.node_id,
        endpoint: config.raft_endpoint.clone(),
      };
      raft_node.init_cluster(node).await?;
    } else {
      raft_node.join_cluster().await?;
    }

    Self::start_raft_service(raft_node).await
  }

  /// Start the Raft gRPC service in a separate thread
  async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
    let raft_endpoint = raft_node.config.raft_endpoint.clone();

    // Subscribe to shutdown signal
    let mut shutdown_rx = raft_node.shutdown_tx.subscribe();

    // Clone raft_node for the spawned task
    let raft_node_for_service = raft_node.clone();

    // Spawn gRPC server in a separate thread/task
    let handle = tokio::task::spawn(async move {
      tracing::info!("Starting Raft gRPC service on {}", raft_endpoint);

      // Create gRPC service instance
      let raft_service = RaftServiceImpl::new(raft_node_for_service);

      // Create TCP listener
      let listener = match TcpListener::bind(&raft_endpoint.to_string()).await {
        Ok(l) => l,
        Err(e) => {
          tracing::error!("Failed to bind gRPC server to {}: {}", raft_endpoint, e);
          return;
        }
      };

      tracing::info!("Raft gRPC service listening on {}", raft_endpoint);

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

  /// Initialize the Raft cluster with a single node
  /// * `Ok(())` - Successfully initialized the cluster
  /// * `Err(AnyError)` - Failed to initialize with StartupError variants:
  ///   - `StartupError::InvalidConfig` if node configuration is invalid
  ///   - `StartupError::AddNodeError` if adding node to cluster fails
  async fn init_cluster(&self, node: Node) -> Result<()> {
    if node.node_id != *self.raft.node_id() {
      let err = StartupError::invalid_config(format!(
        "Node ID {} does not match current node ID {}",
        node.node_id,
        self.raft.node_id()
      ));
      return Err(crate::error::RockRaftError::from(err));
    }

    // Validate endpoint
    if node.endpoint.addr().is_empty() {
      let err = StartupError::invalid_config("Node endpoint address cannot be empty");
      return Err(crate::error::RockRaftError::from(err));
    }

    // Initialize cluster with the node
    let node_id = node.node_id;
    let mut nodes = std::collections::BTreeMap::new();
    nodes.insert(node_id, node);

    if let Err(e) = self.raft.initialize(nodes).await {
      match e {
        RaftError::APIError(e) => match e {
          InitializeError::NotAllowed(e) => {
            info!("Already initialized: {}", e);
          }
          InitializeError::NotInMembers(e) => {
            let err = StartupError::InvalidConfig(e.to_string());
            return Err(err.into());
          }
        },
        RaftError::Fatal(e) => {
          let err = StartupError::OtherError(e.to_string());
          return Err(err.into());
        }
      }
    }

    Ok(())
  }

  pub async fn join_cluster(&self) -> Result<()> {
    let config = &self.config;
    if config.raft_join.is_empty() {
      info!("'--join' is empty, do not need joining cluster");
      return Ok(());
    }

    if self.is_in_cluster()? {
      info!("node has already in cluster, do not need joining cluster");
      return Ok(());
    }

    Ok(self.do_join_cluster().await?.into())
  }

  async fn do_join_cluster(&self) -> StdResult<(), ManagementError> {
    let config = &self.config;
    let addrs = &config.raft_join;
    let mut errors = vec![];
    let raft_address = config.raft_endpoint.to_string();
    let raft_advertise_address = config.raft_advertise_endpoint.to_string();

    for addr in addrs {
      if addr == &raft_address || addr == &raft_advertise_address {
        debug!("ignore join cluster via self node address {}", addr);
        continue;
      }
      for _i in 0..3 {
        let result = self.join_via(addr).await;
        match result {
          Ok(x) => return Ok(x),
          Err(api_error) => {
            let can_retry = api_error.is_retryable();

            if can_retry {
              debug!("try to connect to addr {} again", addr);
              sleep(Duration::from_millis(1_000)).await;
              continue;
            } else {
              errors.push(api_error);
              break;
            }
          }
        }
      }
    }

    Err(ManagementError::Join(AnyError::error(format!(
      "fail to join node-{} to cluster via {:?}, errors: {}",
      self.raft.node_id(),
      addrs,
      errors
        .into_iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ")
    ))))
  }

  async fn send_forward_request(
    &self,
    addr: &String,
    request: ForwardRequest,
  ) -> std::result::Result<pb::RaftReply, Status> {
    let timeout = Some(Duration::from_millis(10_000));
    let chan_result = JoinConnectionFactory::create_rpc_channel(addr, timeout, None).await;
    let channel = match chan_result {
      Ok(channel) => channel,
      Err(e) => {
        error!("Failed to connect to {}: {:?}", addr, e);
        return Err(Status::internal(format!("Failed to connect: {}", e)));
      }
    };

    let mut raft_client = RaftServiceClient::new(channel);

    let response = raft_client
      .forward(request)
      .await
      .map_err(|e| Status::internal(format!("Failed to forward request: {}", e)))?;

    Ok(response.into_inner())
  }

  async fn join_via(&self, addr: &String) -> Result<()> {
    let config = &self.config;

    let join_req = JoinRequest {
      node_id: config.node_id,
      endpoint: config.raft_endpoint.clone(),
    };

    let req = ForwardRequest {
      forward_to_leader: 1,
      body: ForwardRequestBody::Join(join_req),
    };

    let reply = self.send_forward_request(addr, req).await?;

    if reply.error.is_empty() {
      Ok(())
    } else {
      Err(RockRaftError::Raft(format!(
        "Join failed: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }

  async fn handle_join_request(
    &self,
    join_req: JoinRequest,
  ) -> std::result::Result<ForwardResponse, Status> {
    let leader_handler = LeaderHandler::new(self);
    match leader_handler.join(join_req).await {
      Ok(()) => Ok(ForwardResponse::Join(())),
      Err(e) => {
        error!("Failed to join node: {:?}", e);
        Err(Status::internal(format!("Failed to join node: {}", e)))
      }
    }
  }

  async fn forward_request_to_leader(
    &self,
    leader_id: NodeId,
    request: ForwardRequest,
  ) -> std::result::Result<ForwardResponse, Status> {
    // Get leader's endpoint from membership
    let membership = self
      .state_machine
      .get_last_membership()
      .map_err(|e| Status::internal(format!("Failed to get membership: {}", e)))?;

    let leader_node = membership
      .membership()
      .get_node(&leader_id)
      .ok_or_else(|| Status::internal("Leader id not found in membership"))?;

    let leader_addr = leader_node.endpoint.to_string();

    let reply = self.send_forward_request(&leader_addr, request).await?;

    if reply.error.is_empty() {
      // Deserialize the response data
      let forward_response: ForwardResponse = bincode::deserialize(&reply.data)
        .map_err(|e| Status::internal(format!("Failed to deserialize response: {}", e)))?;
      Ok(forward_response)
    } else {
      Err(Status::internal(format!(
        "Leader returned error: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }

  /// Check if the error is retriable (network/connection related)
  fn is_retriable_error(status: &Status) -> bool {
    matches!(
      status.code(),
      tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Internal
    )
  }

  pub async fn handle_forward_request(
    &self,
    request: ForwardRequest,
  ) -> std::result::Result<ForwardResponse, Status> {
    debug!("recv forward req: {:?}", request);

    const MAX_RETRIES: u32 = 20;
    const RETRY_INTERVAL: Duration = Duration::from_secs(1);

    for attempt in 0..MAX_RETRIES {
      // Check if this node is the leader
      match self.assume_leader().await {
        Ok(_) => {
          // This node is leader, handle the request based on body type
          return match &request.body {
            ForwardRequestBody::Join(join_req) => self.handle_join_request(join_req.clone()).await,
          };
        }
        Err(forward_err) => {
          // This node is not the leader, forward the entire request to the leader
          match forward_err.leader_id {
            Some(leader_id) => {
              match self.forward_request_to_leader(leader_id, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                  // Check if this is a retriable error (network/connection related)
                  if Self::is_retriable_error(&e) && attempt < MAX_RETRIES - 1 {
                    debug!(
                      "Failed to forward request to leader ({}), retrying {}/{}",
                      e,
                      attempt + 1,
                      MAX_RETRIES
                    );
                    sleep(RETRY_INTERVAL).await;
                    continue;
                  } else {
                    // Non-retriable error or max retries reached
                    return Err(e);
                  }
                }
              }
            }
            None => {
              // No leader available, retry if not the last attempt
              if attempt < MAX_RETRIES - 1 {
                debug!(
                  "No leader available to forward request, retrying {}/{}",
                  attempt + 1,
                  MAX_RETRIES
                );
                sleep(RETRY_INTERVAL).await;
                continue;
              } else {
                return Err(Status::internal(
                  "No leader available to forward request after max retries",
                ));
              }
            }
          }
        }
      }
    }

    Err(Status::internal(
      "No leader available to forward request after max retries",
    ))
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
        address: addr.to_string(),
        advertise_host: "".to_string(),
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
