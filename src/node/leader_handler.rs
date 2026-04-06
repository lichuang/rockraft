use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::raft::types::AppliedState;
use crate::raft::types::BatchWriteReq;
use crate::raft::types::Cmd;
use crate::raft::types::ForwardRequestBody;
use crate::raft::types::ForwardResponse;
use crate::raft::types::GetKVReq;
use crate::raft::types::GetMembersReq;
use crate::raft::types::JoinRequest;
use crate::raft::types::LeaveRequest;
use crate::raft::types::LogEntry;
use crate::raft::types::Node;
use crate::raft::types::ScanPrefixReq;
use crate::raft::types::TxnReq;
use crate::raft::types::TypeConfig;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::RaftError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tracing::debug;
use tracing::error;
use tracing::info;

/// LeaderHandler provides methods that can only be called on a leader node
pub struct LeaderHandler<'a> {
  node: &'a RaftNode,
}

impl<'a> LeaderHandler<'a> {
  /// Create a new LeaderHandler from a RaftNode reference
  pub fn new(node: &'a RaftNode) -> Self {
    Self { node }
  }

  /// Get a reference to the underlying RaftNode
  pub fn node(&self) -> &RaftNode {
    self.node
  }

  /// Get a reference to the underlying Raft instance
  pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
    self.node.raft()
  }

  /// Handle a forward request body
  ///
  /// This function handles different types of forward requests based on the body type.
  /// It can only be called on the leader node.
  ///
  /// # Arguments
  /// * `body` - The ForwardRequestBody to handle
  ///
  /// # Returns
  /// * `Ok(ForwardResponse)` - The response to the request
  /// * `Err(Status)` - If the operation failed
  pub async fn handle(&self, body: ForwardRequestBody) -> Result<ForwardResponse> {
    match body {
      ForwardRequestBody::Join(req) => self.handle_join(req).await,
      ForwardRequestBody::Leave(req) => self.handle_leave(req).await,
      ForwardRequestBody::GetMembers(req) => self.handle_get_members(req).await,
      ForwardRequestBody::Write(entry) => self.handle_write(entry).await,
      ForwardRequestBody::GetKV(req) => self.handle_get_kv(req).await,
      ForwardRequestBody::ScanPrefix(req) => self.handle_scan_prefix(req).await,
      ForwardRequestBody::BatchWrite(req) => self.handle_batch_write(req).await,
      ForwardRequestBody::Txn(req) => self.handle_txn(req).await,
    }
  }

  /// Handle join request
  ///
  /// This function adds a node to the raft cluster. It first writes a log entry
  /// to add the node, then changes the membership to include the new node as a voter.
  async fn handle_join(&self, req: JoinRequest) -> Result<ForwardResponse> {
    let node_id = req.node_id;
    info!("Handling join request for node {}", node_id);

    // Get current membership and check if node already exists
    let metrics = self.raft().metrics().borrow_watched().clone();
    let membership = metrics.membership_config.membership();

    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if voters.contains(&node_id) {
      info!("Node {} already in membership, skipping join", node_id);
      return Ok(ForwardResponse::Join(()));
    }

    // First, sync all existing nodes to state machine (to ensure new node gets all node info)
    info!("Syncing existing nodes to state machine");
    for (id, node) in membership.nodes() {
      info!("Syncing node {} to state machine", id);
      let entry = LogEntry::new(Cmd::AddNode {
        node: node.clone(),
        overriding: true,
      });
      if let Err(e) = self.write(entry).await {
        error!("Failed to sync node {}: {:?}", id, e);
        return Err(Error::internal(format!(
          "Failed to sync node {}: {}",
          id, e
        )));
      }
    }

    let node = Node {
      node_id,
      endpoint: req.endpoint.clone(),
    };

    // Write a log entry to add the new node
    info!("Writing AddNode command for node {}", node_id);
    let entry = LogEntry::new(Cmd::AddNode {
      node: node.clone(),
      overriding: false,
    });

    if let Err(e) = self.write(entry).await {
      error!("Failed to write AddNode entry: {:?}", e);
      return Err(Error::internal(format!("Failed to join node: {}", e)));
    }
    info!("AddNode command written successfully for node {}", node_id);

    // Change membership to add the new node as voter (retain removed voters as learners)
    info!("Changing membership to add node {} as voter", node_id);
    let mut add_voters = BTreeMap::new();
    add_voters.insert(node_id, node);

    let msg = ChangeMembers::AddVoters(add_voters);
    if let Err(e) = self.raft().change_membership(msg, false).await {
      error!("Failed to change membership: {:?}", e);
      return Err(Error::internal(format!("Failed to join node: {}", e)));
    }
    info!("Node {} joined successfully", node_id);

    Ok(ForwardResponse::Join(()))
  }

  /// Handle leave request
  ///
  /// This function removes a node from the raft cluster. It first writes a log entry
  /// to remove the node, then changes the membership to exclude the node.
  async fn handle_leave(&self, req: LeaveRequest) -> Result<ForwardResponse> {
    let node_id = req.node_id;

    // Get current membership and check if node exists
    let metrics = self.raft().metrics().borrow_watched().clone();
    let membership = metrics.membership_config.membership();

    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if !voters.contains(&node_id) {
      // Node not in cluster, consider it already left
      return Ok(ForwardResponse::Leave(()));
    }

    // Write a log entry to remove the node
    let entry = LogEntry::new(Cmd::RemoveNode { node_id });

    if let Err(e) = self.write(entry).await {
      error!("Failed to leave node: {:?}", e);
      return Err(Error::internal(format!("Failed to leave node: {}", e)));
    }

    // Change membership to remove the node
    let mut remove_voters: BTreeSet<u64> = BTreeSet::new();
    remove_voters.insert(node_id);

    if let Err(e) = self.raft().change_membership(remove_voters, true).await {
      error!("Failed to leave node: {:?}", e);
      return Err(Error::internal(format!("Failed to leave node: {}", e)));
    }

    Ok(ForwardResponse::Leave(()))
  }

  /// Handle write request
  async fn handle_write(&self, entry: LogEntry) -> Result<ForwardResponse> {
    match self.write(entry).await {
      Ok(applied_state) => Ok(ForwardResponse::Write(applied_state)),
      Err(e) => {
        error!("Failed to write log entry: {:?}", e);
        Err(Error::internal(format!("Failed to write log entry: {}", e)))
      }
    }
  }

  /// Handle batch write request
  ///
  /// This function writes multiple entries atomically to the raft cluster.
  /// All entries are applied as a single log entry, ensuring atomicity.
  async fn handle_batch_write(&self, req: BatchWriteReq) -> Result<ForwardResponse> {
    if req.entries.is_empty() {
      return Ok(ForwardResponse::BatchWrite(AppliedState::None));
    }

    let entry = LogEntry::new(Cmd::BatchUpsertKV {
      entries: req.entries,
    });

    match self.write(entry).await {
      Ok(applied_state) => Ok(ForwardResponse::BatchWrite(applied_state)),
      Err(e) => {
        error!("Failed to write batch log entry: {:?}", e);
        Err(Error::internal(format!(
          "Failed to write batch log entry: {}",
          e
        )))
      }
    }
  }

  /// Handle get_kv request
  async fn handle_get_kv(&self, req: GetKVReq) -> Result<ForwardResponse> {
    match self.node.state_machine().get_kv(&req.key) {
      Ok(value) => Ok(ForwardResponse::GetKV(value)),
      Err(e) => {
        error!("Failed to get kv: {:?}", e);
        Err(Error::internal(format!("Failed to get kv: {}", e)))
      }
    }
  }

  /// Handle scan_prefix request
  ///
  /// This function scans all key-value pairs with the given prefix from the KV store.
  /// Note: This is a read-only operation and can be handled by any node, but for
  /// consistency, it's forwarded to the leader.
  async fn handle_scan_prefix(&self, req: ScanPrefixReq) -> Result<ForwardResponse> {
    match self.node.state_machine().scan_prefix(&req.prefix) {
      Ok(results) => Ok(ForwardResponse::ScanPrefix(results)),
      Err(e) => {
        error!("Failed to scan prefix: {:?}", e);
        Err(Error::internal(format!("Failed to scan prefix: {}", e)))
      }
    }
  }

  /// Handle get_members request
  ///
  /// This function returns the current cluster members.
  /// Note: This operation can be handled by any node, not just the leader.
  async fn handle_get_members(&self, _req: GetMembersReq) -> Result<ForwardResponse> {
    match self.node.state_machine().get_nodes() {
      Ok(nodes) => Ok(ForwardResponse::GetMembers(nodes)),
      Err(e) => {
        error!("Failed to get members: {:?}", e);
        Err(Error::internal(format!("Failed to get members: {}", e)))
      }
    }
  }

  /// Handle transaction request
  ///
  /// This function executes a transaction that checks conditions and performs
  /// operations atomically based on the condition results.
  async fn handle_txn(&self, req: TxnReq) -> Result<ForwardResponse> {
    // Build a transaction command
    let entry = LogEntry::new(Cmd::Txn { req, result: None });

    match self.write(entry).await {
      Ok(AppliedState::Txn(reply)) => {
        // Transaction was applied successfully
        Ok(ForwardResponse::Txn(reply))
      }
      Ok(_) => {
        // Should not happen - Txn command always returns AppliedState::Txn
        error!("Unexpected AppliedState from transaction");
        Err(Error::internal("Unexpected response from transaction"))
      }
      Err(e) => {
        error!("Failed to execute transaction: {:?}", e);
        Err(Error::internal(format!(
          "Failed to execute transaction: {}",
          e
        )))
      }
    }
  }

  /// Write a log entry to the raft cluster
  ///
  /// This function writes a LogEntry to the raft log and waits for it to be applied.
  /// It can only be called on the leader node.
  ///
  /// The `time_ms` field of the entry will be set to the current timestamp before writing.
  ///
  /// # Arguments
  /// * `entry` - The LogEntry to write
  ///
  /// # Returns
  /// * `Ok(AppliedState)` - The result of applying the log entry
  /// * `Err(Error)` - If the operation failed or this node is not the leader
  pub async fn write(&self, mut entry: LogEntry) -> Result<AppliedState> {
    // Set the current timestamp in milliseconds, safe to unwrap
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_millis() as u64;
    entry.time_ms = Some(now);

    let node_id = self.raft().node_id();

    match self.raft().client_write(entry).await {
      Ok(response) => {
        debug!(
          node_id = %node_id,
          log_id = %response.log_id,
          "Successfully wrote log entry"
        );
        Ok(response.data)
      }
      Err(e) => {
        error!(
          node_id = %node_id,
          error = %e,
          "Failed to write log entry"
        );
        match e {
          RaftError::APIError(api_err) => {
            Err(Error::internal(format!("client write error: {}", api_err)))
          }
          RaftError::Fatal(fatal_err) => {
            Err(Error::internal(format!("fatal raft error: {}", fatal_err)))
          }
        }
      }
    }
  }
}
