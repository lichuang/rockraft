use crate::error::{Error, Result};
use crate::grpc::JoinConnectionFactory;
use crate::raft::protobuf as pb;
use crate::raft::protobuf::raft_service_client::RaftServiceClient;
use crate::raft::types::ForwardRequestBody;
use crate::raft::types::JoinRequest;

use openraft::error::{InitializeError, RaftError};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio::time::{Duration, timeout};
use tracing::debug;
use tracing::error;
use tracing::info;

use std::result::Result as StdResult;
use tokio::time::sleep;

use openraft::Config as OpenRaftConfig;
use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;

use tonic::transport::Server;

use super::LeaderHandler;
use crate::config::Config;
use crate::engine::RocksDBEngine;
use crate::raft::grpc_client::ClientPool;
use crate::raft::network::NetworkFactory;
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::raft::store::RocksLogStore;
use crate::raft::store::RocksStateMachine;
use crate::raft::store::column_family_list;
use crate::raft::types::{
  AppliedState, BatchWriteReply, BatchWriteReq, ForwardRequest, ForwardResponse, LogEntry, Node,
  TxnReply, TxnReq, TypeConfig, decode,
};
use crate::raft::types::{
  ForwardToLeader, GetKVReply, GetKVReq, GetMembersReply, GetMembersReq, LeaveRequest, NodeId,
  ScanPrefixReply, ScanPrefixReq,
};
use crate::service::RaftServiceImpl;

/// Retry configuration for forward operations
const MAX_RETRIES: u32 = 20;
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct RaftNode {
  #[allow(dead_code)]
  engine: Arc<RocksDBEngine>,
  raft: Arc<Raft<TypeConfig>>,

  config: Config,

  #[allow(dead_code)]
  factory: NetworkFactory,

  state_machine: Arc<RocksStateMachine>,

  shutdown_tx: broadcast::Sender<()>,
  _shutdown_rx: broadcast::Receiver<()>,
  service_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RaftNode {
  /// Get a reference to the underlying Raft instance
  pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
    &self.raft
  }

  /// Get a reference to the state machine
  pub fn state_machine(&self) -> &Arc<RocksStateMachine> {
    &self.state_machine
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
      .map_err(|e| Error::internal(format!("Failed to create raft: {}", e)))?,
    );

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx_for_struct) = broadcast::channel(1);

    Ok(Arc::new(Self {
      engine,
      raft,
      config: config.clone(),
      factory,
      state_machine: Arc::new(state_machine),
      shutdown_tx,
      _shutdown_rx: shutdown_rx_for_struct,
      service_handle: Mutex::new(None),
    }))
  }

  pub async fn start(raft_node: Arc<Self>) -> Result<()> {
    let config = &raft_node.config;

    Self::start_raft_service(raft_node.clone()).await?;

    if config.raft.single {
      let node = Node {
        node_id: config.node_id,
        endpoint: config.raft.endpoint.clone(),
      };
      raft_node.init_cluster(node).await?;
    } else {
      raft_node.join_cluster().await?;
    }

    Ok(())
  }

  /// Start the Raft gRPC service (for builder use)
  ///
  /// This function spawns the gRPC server in a background task and waits for
  /// the service to successfully bind to the endpoint before returning.
  pub(crate) async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
    let raft_endpoint = raft_node.config.raft.endpoint.clone();

    // Subscribe to shutdown signal
    let mut shutdown_rx = raft_node.shutdown_tx.subscribe();

    // Clone raft_node for the spawned task
    let raft_node_for_service = raft_node.clone();

    // Create oneshot channel to signal startup completion
    let (startup_tx, startup_rx) = oneshot::channel::<StdResult<(), String>>();

    // Spawn gRPC server in a separate thread/task
    let handle = tokio::task::spawn(async move {
      tracing::info!("Starting Raft gRPC service on {}", raft_endpoint);

      // Create gRPC service instance
      let raft_service = RaftServiceImpl::new(raft_node_for_service);

      // Create TCP listener
      let listener = match TcpListener::bind(&raft_endpoint.to_string()).await {
        Ok(l) => l,
        Err(e) => {
          let err_msg = format!("Failed to bind gRPC server to {}: {}", raft_endpoint, e);
          tracing::error!("{}", err_msg);
          // Signal startup failure
          let _ = startup_tx.send(Err(err_msg));
          return;
        }
      };

      // Signal startup success
      if startup_tx.send(Ok(())).is_err() {
        error!("Failed to signal startup completion");
        return;
      }

      info!("Raft gRPC service listening on {}", raft_endpoint);

      // Run the gRPC server with shutdown handling
      let server_future = Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

      tokio::select! {
        // Wait for shutdown signal
        _ = shutdown_rx.recv() => {
          info!("Raft gRPC service received shutdown signal, shutting down...");
        }

        // Wait for server to finish (if it errors)
        result = server_future => {
          match result {
            Ok(_) => info!("Raft gRPC service finished normally"),
            Err(e) => error!("Raft gRPC service error: {}", e),
          }
        }
      }

      info!("Raft gRPC service stopped");
    });

    // Wait for startup completion signal
    match startup_rx.await {
      Ok(Ok(())) => {
        // Store the handle in RaftNode
        *raft_node.service_handle.lock().unwrap() = Some(handle);
        info!("Raft gRPC service started successfully");
        Ok(())
      }
      Ok(Err(err_msg)) => {
        // Wait for the task to finish to ensure proper cleanup
        let _ = handle.await;
        Err(Error::internal(err_msg))
      }
      Err(_) => {
        // Channel closed unexpectedly (task panicked)
        let _ = handle.await;
        Err(Error::internal(
          "gRPC service startup task failed unexpectedly",
        ))
      }
    }
  }

  async fn get_leader(&self) -> Result<Option<NodeId>> {
    let deadline = Duration::from_millis(2000);
    let mut metrics_rx = self.raft.metrics();

    let result = timeout(deadline, async {
      loop {
        if let Some(leader) = metrics_rx.borrow_watched().current_leader {
          return Ok(Some(leader));
        }
        if let Err(e) = WatchReceiver::changed(&mut metrics_rx).await {
          // If changed() returns an error, the watch channel is closed
          // or receiver lagged. Return the error to the caller
          let error_msg = format!("Metrics watch error: {:?}", e);
          tracing::debug!("{}", error_msg);
          return Err(Error::internal(error_msg));
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
  async fn assume_leader(&self) -> StdResult<LeaderHandler<'_>, ForwardToLeader> {
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
  fn is_in_cluster(&self) -> Result<bool> {
    let last_membership = self
      .state_machine
      .get_last_membership()
      .map_err(|e| Error::internal(format!("get_last_membership error: {}", e)))?;
    let node_id = *self.raft.node_id();

    // Only check voter_ids
    let is_voter = last_membership
      .membership()
      .voter_ids()
      .any(|id| id == node_id);

    Ok(is_voter)
  }

  /// Initialize the Raft cluster with a single node (for builder use)
  /// * `Ok(())` - Successfully initialized the cluster
  /// * `Err(Error)` - Failed to initialize:
  ///   - `InvalidConfig` if node configuration is invalid
  ///   - `Internal` if adding node to cluster fails
  pub(crate) async fn init_cluster(&self, node: Node) -> Result<()> {
    if node.node_id != *self.raft.node_id() {
      return Err(Error::config(format!(
        "Node ID {} does not match current node ID {}",
        node.node_id,
        self.raft.node_id()
      )));
    }

    // Validate endpoint
    if node.endpoint.addr().is_empty() {
      return Err(Error::config("Node endpoint address cannot be empty"));
    }

    // Add current node to state machine first
    info!("Adding node {} to state machine", node.node_id);
    self.state_machine.add_node(node.clone()).map_err(|e| {
      error!("Failed to add node: {}", e);
      Error::internal(format!("Failed to add node: {}", e))
    })?;
    info!("Node {} added to state machine successfully", node.node_id);

    // Initialize cluster with the node
    let node_id = node.node_id;
    let mut nodes = BTreeMap::new();
    nodes.insert(node_id, node);

    if let Err(e) = self.raft.initialize(nodes).await {
      match e {
        RaftError::APIError(e) => match e {
          InitializeError::NotAllowed(e) => {
            info!("Already initialized: {}", e);
          }
          InitializeError::NotInMembers(e) => {
            return Err(Error::config(e.to_string()));
          }
        },
        RaftError::Fatal(e) => {
          return Err(Error::internal(e.to_string()));
        }
      }
    }

    Ok(())
  }

  pub(crate) async fn join_cluster(&self) -> Result<()> {
    let config = &self.config;
    if config.raft.join.is_empty() {
      info!("'--join' is empty, do not need joining cluster");
      return Ok(());
    }

    if self.is_in_cluster()? {
      info!("node has already in cluster, do not need joining cluster");
      return Ok(());
    }

    self.do_join_cluster().await?;
    Ok(())
  }

  async fn do_join_cluster(&self) -> Result<()> {
    let config = &self.config;
    let addrs = &config.raft.join;
    let mut errors = vec![];
    let raft_address = config.raft.endpoint.to_string();
    let raft_advertise_address = config.raft.advertise_endpoint.to_string();

    for addr in addrs {
      if addr == &raft_address || addr == &raft_advertise_address {
        debug!("ignore join cluster via self node address {}", addr);
        continue;
      }
      for _i in 0..3 {
        let result = self.join_via(addr).await;
        info!("join cluster via {} result: {:?}", addr, result);

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

    Err(Error::internal(format!(
      "fail to join node-{} to cluster via {:?}, errors: {}",
      self.raft.node_id(),
      addrs,
      errors
        .into_iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ")
    )))
  }

  async fn send_forward_request(
    &self,
    addr: &String,
    request: ForwardRequest,
  ) -> Result<pb::RaftReply> {
    let timeout = Some(Duration::from_millis(10_000));
    let chan_result = JoinConnectionFactory::create_rpc_channel(addr, timeout, None).await;
    let channel = match chan_result {
      Ok(channel) => channel,
      Err(e) => {
        error!("Failed to connect to {}: {:?}", addr, e);
        return Err(e);
      }
    };

    let mut raft_client = RaftServiceClient::new(channel);

    let response = raft_client
      .forward(request)
      .await
      .map_err(|e| Error::internal(format!("Failed to forward request: {}", e)))?;

    Ok(response.into_inner())
  }

  async fn join_via(&self, addr: &String) -> Result<()> {
    let config = &self.config;

    let join_req = JoinRequest {
      node_id: config.node_id,
      endpoint: config.raft.endpoint.clone(),
    };

    let req = ForwardRequest {
      forward_to_leader: 1,
      body: ForwardRequestBody::Join(join_req),
    };

    let reply = self.send_forward_request(addr, req).await?;

    if reply.error.is_empty() {
      Ok(())
    } else {
      Err(Error::internal(format!(
        "Join failed: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }

  /// Write a log entry to the raft cluster
  ///
  /// The entry is appended to the raft log and replicated to a majority of nodes
  /// before returning. This provides strong consistency - once the call returns,
  /// the entry is guaranteed to be durable and will not be lost even if the
  /// current leader fails.
  ///
  /// # Consistency
  /// - Linearizable writes: all nodes agree on the same write order
  /// - Durable on majority: returns only after replication to majority
  ///
  /// # Leader Handling
  /// - If this node is leader: writes directly to local log
  /// - If not leader: transparently forwards to current leader with retry
  /// - If leader changes during operation: automatically retries with new leader
  ///
  /// # Errors
  /// - Returns retryable error if no leader is available (cluster electing)
  /// - Returns permanent error for invalid entries or internal failures
  pub async fn write(&self, entry: LogEntry) -> Result<AppliedState> {
    debug!("write log entry: {:?}", entry);

    // Check if this node is the leader
    match self.assume_leader().await {
      Ok(leader) => leader.write(entry).await,
      Err(forward_err) => {
        // Forward to remote leader
        self
          .forward_write_to_leader(forward_err.leader_id, entry)
          .await
      }
    }
  }

  async fn forward_write_to_leader(
    &self,
    leader_id: Option<NodeId>,
    entry: LogEntry,
  ) -> Result<AppliedState> {
    let request = ForwardRequest {
      forward_to_leader: 1,
      body: ForwardRequestBody::Write(entry),
    };

    let response = self.forward_with_leader_id(leader_id, request).await?;
    match response {
      ForwardResponse::Write(applied_state) => Ok(applied_state),
      _ => Err(Error::internal("Unexpected response type from leader")),
    }
  }

  /// Batch write multiple entries atomically
  ///
  /// All entries in the batch are applied as a single atomic unit - either
  /// all succeed or all fail. This is useful for maintaining referential
  /// integrity across multiple keys.
  ///
  /// The batch is replicated to a majority of nodes as a single log entry,
  /// ensuring atomicity even during leader failures.
  ///
  /// # Performance
  /// - More efficient than individual writes for multiple entries
  /// - Single network round-trip when leader is local
  /// - Amortized fsync cost across all entries in batch
  ///
  /// # Consistency
  /// - Atomic: all entries visible together or not at all
  /// - Same durability guarantees as single write
  pub async fn batch_write(&self, req: BatchWriteReq) -> Result<BatchWriteReply> {
    debug!("batch write: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => leader.batch_write(req).await,
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::BatchWrite(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::BatchWrite(applied_state) => Ok(applied_state),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Execute an atomic compare-and-swap transaction
  ///
  /// Checks all conditions against the current state, then atomically executes
  /// either `if_then` (conditions met) or `else_then` (conditions not met).
  ///
  /// This enables optimistic concurrency control - read a value, compute new
  /// state based on it, then commit only if the value hasn't changed.
  ///
  /// # Use Cases
  /// - Atomic increments: read counter, increment, write only if unchanged
  /// - Lock acquisition: set lock only if not already held
  /// - Conditional updates: update field only if preconditions hold
  ///
  /// # Performance Notes
  /// - Conditions are evaluated on the leader without writing to log
  /// - Only the chosen branch (if_then or else_then) is logged
  /// - Failed conditions still cost a round-trip but no log write
  ///
  /// # Example
  /// ```rust,no_run
  /// use rockraft::raft::types::{TxnReq, TxnCondition, UpsertKV};
  ///
  /// // Only update if key still has expected value
  /// let req = TxnReq::new(vec![TxnCondition::eq("key", b"expected_value")])
  ///   .if_then(UpsertKV::insert("key", b"new_value"));
  /// let reply = raft_node.txn(req).await?;
  /// ```
  pub async fn txn(&self, req: TxnReq) -> Result<TxnReply> {
    debug!("transaction: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => leader.txn(req).await,
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::Txn(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::Txn(reply) => Ok(reply),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Atomically swap a value, returning the previous value
  ///
  /// Combines a read and write into a single atomic operation. Useful for
  /// atomic updates where you need to know what was there before.
  ///
  /// Implemented as a transaction with `return_previous` enabled.
  ///
  /// # Concurrency
  /// The swap is atomic - no other write can interleave between the read
  /// and write. If multiple nodes call getset concurrently, they will
  /// be serialized through raft and each will see the value set by the
  /// previous successful call.
  ///
  /// # Example
  /// ```rust,no_run
  /// # use rockraft::node::RaftNode;
  /// # async fn example(node: &RaftNode) -> Result<(), Box<dyn std::error::Error>> {
  /// let old_value = node.getset("my_key", b"new_value").await?;
  /// match old_value {
  ///   Some(prev) => println!("Previous value: {:?}", prev),
  ///   None => println!("Key did not exist"),
  /// }
  /// # Ok(())
  /// # }
  /// ```
  pub async fn getset(
    &self,
    key: impl ToString,
    value: impl AsRef<[u8]>,
  ) -> Result<Option<Vec<u8>>> {
    use crate::raft::types::UpsertKV;

    let req = TxnReq::new(vec![]) // No conditions, always execute
      .if_then(UpsertKV::insert(key, value.as_ref()))
      .with_return_previous();

    match self.txn(req).await? {
      TxnReply::Success { prev_values, .. } => Ok(prev_values.into_iter().next().flatten()),
    }
  }

  /// Read a value from the KV store
  ///
  /// Returns the value as of the latest committed log entry. Reads may
  /// be served by any node (not just the leader) for better throughput.
  ///
  /// # Consistency
  /// - Monotonic reads: once you see a value, you'll never see an older one
  /// - May return stale data briefly after leader change (until state catches up)
  /// - For strongly consistent reads, use a transaction
  ///
  /// # Performance
  /// - Served locally if this node has applied the latest log entry
  /// - No network round-trip in the common case
  /// - Linearizable with respect to writes
  pub async fn read(&self, req: GetKVReq) -> Result<GetKVReply> {
    debug!("read kv: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => leader.read(req).await,
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::GetKV(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::GetKV(value) => Ok(value),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Scan keys with a given prefix
  ///
  /// Returns all key-value pairs where the key starts with the given prefix.
  /// Results are returned in lexicographic order by key.
  ///
  /// # Use Cases
  /// - Listing items in a "directory" (e.g., all keys under "users/")
  /// - Range queries when keys have structured prefixes
  /// - Pagination when combined with key boundaries
  ///
  /// # Performance Notes
  /// - Iterates over all keys with prefix - O(n) where n = matching keys
  /// - Consider using transactions for large result sets
  /// - No snapshot isolation - may see interleaved writes during scan
  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    debug!("scan_prefix: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => leader.scan_prefix(req).await,
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::ScanPrefix(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::ScanPrefix(results) => Ok(results),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Add a new node to the raft cluster
  ///
  /// The new node will be added as a learner initially, then promoted to voter
  /// once it catches up with the log. This is a two-phase process coordinated
  /// by the leader.
  ///
  /// # Membership Changes
  /// - New node starts as non-voting learner (receives log but doesn't vote)
  /// - Once caught up, automatically promoted to voter
  /// - Cluster availability maintained during the process (majority still valid)
  ///
  /// # Safety
  /// - Only one membership change at a time (raft invariant)
  /// - Returns error if another change is in progress
  /// - Idempotent: returns success if node already in cluster
  pub async fn join(&self, req: JoinRequest) -> Result<()> {
    debug!("join node: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => {
        leader.join(req).await?;
        Ok(())
      }
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::Join(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::Join(()) => Ok(()),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Remove a node from the raft cluster
  ///
  /// The node is demoted to learner first (stops voting), then removed from
  /// the cluster. This ensures the cluster maintains availability during
  /// the removal process.
  ///
  /// # Use Cases
  /// - Graceful node decommission (drain and remove)
  /// - Removing failed nodes that won't rejoin
  /// - Shrinking cluster size
  ///
  /// # Safety
  /// - Cannot remove the last node (would lose quorum)
  /// - Only one membership change at a time
  /// - Idempotent: returns success if node already not in cluster
  pub async fn leave(&self, req: LeaveRequest) -> Result<()> {
    debug!("leave node: {:?}", req);

    match self.assume_leader().await {
      Ok(leader) => {
        leader.leave(req).await?;
        Ok(())
      }
      Err(forward_err) => {
        let request = ForwardRequest {
          forward_to_leader: 1,
          body: ForwardRequestBody::Leave(req),
        };
        let response = self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await?;
        match response {
          ForwardResponse::Leave(()) => Ok(()),
          _ => Err(Error::internal("Unexpected response type from leader")),
        }
      }
    }
  }

  /// Get the current cluster membership
  ///
  /// Returns all nodes currently in the cluster, including their IDs and
  /// network endpoints. This reflects the committed membership configuration.
  ///
  /// # Note on Staleness
  /// The returned membership may be slightly stale if a configuration change
  /// (join/leave) is in progress. For critical decisions, check the latest
  /// membership from the raft metrics instead.
  pub async fn get_members(&self, req: GetMembersReq) -> Result<GetMembersReply> {
    debug!("get members: {:?}", req);

    // This operation can be handled by any node via LeaderHandler
    let leader_handler = LeaderHandler::new(self);
    leader_handler.get_members(req).await
  }

  async fn forward_request_to_leader(
    &self,
    leader_id: NodeId,
    request: ForwardRequest,
  ) -> Result<ForwardResponse> {
    // Get leader's endpoint from membership
    let membership = self
      .state_machine
      .get_last_membership()
      .map_err(|e| Error::internal(format!("Failed to get membership: {}", e)))?;

    let leader_node = membership
      .membership()
      .get_node(&leader_id)
      .ok_or_else(|| Error::internal("Leader id not found in membership"))?;

    let leader_addr = leader_node.endpoint.to_string();

    let reply = self.send_forward_request(&leader_addr, request).await?;

    if reply.error.is_empty() {
      // Deserialize the response data
      let forward_response: ForwardResponse = decode(&reply.data)
        .map_err(|e| Error::internal(format!("Failed to deserialize response: {}", e)))?;
      Ok(forward_response)
    } else {
      Err(Error::internal(format!(
        "Leader returned error: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }

  /// Check if the error is retriable (network/connection related)
  fn is_retriable_error(error: &Error) -> bool {
    error.is_retryable()
  }

  /// Handle a forwarded request (gRPC entry point)
  ///
  /// This is the entry point for requests coming from other nodes via gRPC.
  /// It handles the request using LeaderHandler if this node is leader,
  /// or forwards to the actual leader if not.
  pub async fn handle_forward_request(&self, request: ForwardRequest) -> Result<ForwardResponse> {
    debug!("recv forward req: {:?}", request);

    for attempt in 0..MAX_RETRIES {
      match self.assume_leader().await {
        Ok(leader) => {
          // This node is leader, handle using LeaderHandler
          return Self::dispatch_leader_handler(leader, request.body).await;
        }
        Err(forward_err) => {
          // Forward to the actual leader
          let retry_reason = match forward_err.leader_id {
            Some(leader_id) => {
              match self
                .forward_request_to_leader(leader_id, request.clone())
                .await
              {
                Ok(response) => return Ok(response),
                Err(e) => {
                  if Self::is_retriable_error(&e) {
                    Some(format!("Failed to forward request ({e})"))
                  } else {
                    return Err(e);
                  }
                }
              }
            }
            None => Some("No leader available to forward request".to_string()),
          };

          if let Some(reason) = retry_reason
            && attempt < MAX_RETRIES - 1
          {
            debug!("{}, retrying {}/{}", reason, attempt + 1, MAX_RETRIES);
            sleep(RETRY_INTERVAL).await;
            continue;
          }

          return Err(Error::internal(
            "No leader available to forward request after max retries",
          ));
        }
      }
    }

    Err(Error::internal(
      "No leader available to forward request after max retries",
    ))
  }

  /// Dispatch a request body to the appropriate LeaderHandler method
  ///
  /// This is a compatibility layer that routes ForwardRequestBody to the
  /// corresponding type-safe LeaderHandler method.
  async fn dispatch_leader_handler(
    leader: LeaderHandler<'_>,
    body: ForwardRequestBody,
  ) -> Result<ForwardResponse> {
    match body {
      ForwardRequestBody::Write(entry) => {
        let result = leader.write(entry).await?;
        Ok(ForwardResponse::Write(result))
      }
      ForwardRequestBody::BatchWrite(req) => {
        let result = leader.batch_write(req).await?;
        Ok(ForwardResponse::BatchWrite(result))
      }
      ForwardRequestBody::Txn(req) => {
        let result = leader.txn(req).await?;
        Ok(ForwardResponse::Txn(result))
      }
      ForwardRequestBody::GetKV(req) => {
        let result = leader.read(req).await?;
        Ok(ForwardResponse::GetKV(result))
      }
      ForwardRequestBody::ScanPrefix(req) => {
        let result = leader.scan_prefix(req).await?;
        Ok(ForwardResponse::ScanPrefix(result))
      }
      ForwardRequestBody::Join(req) => {
        leader.join(req).await?;
        Ok(ForwardResponse::Join(()))
      }
      ForwardRequestBody::Leave(req) => {
        leader.leave(req).await?;
        Ok(ForwardResponse::Leave(()))
      }
      ForwardRequestBody::GetMembers(req) => {
        let result = leader.get_members(req).await?;
        Ok(ForwardResponse::GetMembers(result))
      }
    }
  }

  /// Forward a request to a specific leader by ID
  ///
  /// Looks up the leader's endpoint and forwards the request via gRPC.
  async fn forward_with_leader_id(
    &self,
    leader_id: Option<NodeId>,
    request: ForwardRequest,
  ) -> Result<ForwardResponse> {
    let leader_id = leader_id.ok_or_else(|| {
      Error::retryable(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No leader available",
      ))
    })?;
    self.forward_request_to_leader(leader_id, request).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::RaftConfig;
  use crate::config::RocksdbConfig;
  use crate::raft::types::Endpoint;
  use tempfile::tempdir;

  /// Helper function to create a test config
  fn create_test_config(data_dir: &str, node_id: u64, addr: &str) -> Config {
    use crate::config::Endpoint;

    let endpoint = Endpoint::parse(addr).expect("Invalid test address");
    Config {
      node_id,
      raft: RaftConfig {
        endpoint: endpoint.clone(),
        advertise_endpoint: endpoint,
        single: true,
        join: vec![],
      },
      rocksdb: RocksdbConfig {
        data_path: data_dir.to_string(),
        max_open_files: 1024,
      },
    }
  }

  /// Helper function to set up nodes for testing
  async fn setup_nodes(raft_node: &RaftNode, node_ids: Vec<u64>) {
    let sm = &raft_node.state_machine;

    for node_id in node_ids {
      let node = Node {
        node_id,
        endpoint: Endpoint::new("127.0.0.1", 1000 + node_id as u32),
      };
      sm.add_node(node).unwrap();
    }
  }

  #[tokio::test]
  async fn test_is_in_cluster_node_exists() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();

    // Create RaftNode with node_id=1
    let config = create_test_config(&data_path, 1, "127.0.0.1:5001");
    let raft_node = RaftNode::create(&config).await?;

    // Set up nodes with node 1, 2, 3
    setup_nodes(&raft_node, vec![1, 2, 3]).await;

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

    // Set up nodes 1, 2, 3 (not including node 4)
    setup_nodes(&raft_node, vec![1, 2, 3]).await;

    // Check if node 4 is in cluster
    let result = raft_node.is_in_cluster()?;
    assert!(!result, "Node 4 should not be in the cluster");

    Ok(())
  }
}
