use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::raft::types::AppliedState;
use crate::raft::types::BatchWriteReq;
use crate::raft::types::Cmd;
use crate::raft::types::GetKVReq;
use crate::raft::types::GetMembersReply;
use crate::raft::types::GetMembersReq;
use crate::raft::types::JoinRequest;
use crate::raft::types::LeaveRequest;
use crate::raft::types::LogEntry;
use crate::raft::types::Node;
use crate::raft::types::ScanPrefixReply;
use crate::raft::types::ScanPrefixReq;
use crate::raft::types::TxnReply;
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

/// Helper macro to convert errors with logging.
///
/// Usage: `map_err_log!(result, "operation name")`
///
/// This logs the error at error level with the operation name and returns
/// an `Error::internal` with a descriptive message.
#[macro_export]
macro_rules! map_err_log {
  ($result:expr, $context:expr) => {
    $result.map_err(|e| {
      error!("{}: {:?}", $context, e);
      Error::internal(format!("{}: {}", $context, e))
    })
  };
}

/// Helper macro to convert RaftError to internal Error with logging.
#[macro_export]
macro_rules! map_raft_err_log {
  ($result:expr, $context:expr) => {
    $result.map_err(|e| {
      error!("{}: {:?}", $context, e);
      match e {
        RaftError::APIError(api_err) => Error::internal(format!("{}: {}", $context, api_err)),
        RaftError::Fatal(fatal_err) => {
          Error::internal(format!("{}: fatal raft error: {}", $context, fatal_err))
        }
      }
    })
  };
}

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

  /// Write a log entry to the raft log (leader-only)
  ///
  /// Appends the entry to the local log and replicates it to followers.
  /// Returns after the entry is committed (majority acknowledgment).
  ///
  /// # Leader Requirement
  /// This method **must** only be called when this node is the leader.
  /// Callers should use `RaftNode::assume_leader()` to verify leadership first.
  ///
  /// # Timestamp
  /// The `time_ms` field is automatically set to the current time before writing.
  pub async fn write(&self, entry: LogEntry) -> Result<AppliedState> {
    self.do_write(entry).await
  }

  /// Add a new node to the cluster (leader-only)
  ///
  /// Adds the node as a learner first, then promotes to voter.
  /// Syncs all existing nodes to the new node before promotion.
  ///
  /// # Leader Requirement
  /// Must be called on the leader. Use `RaftNode::assume_leader()` first.
  pub async fn add_node(&self, req: JoinRequest) -> Result<()> {
    let node_id = req.node_id;
    info!("Handling join request for node {}", node_id);

    // Get current membership and check if node already exists
    let metrics = self.raft().metrics().borrow_watched().clone();
    let membership = metrics.membership_config.membership();

    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if voters.contains(&node_id) {
      info!("Node {} already in membership, skipping join", node_id);
      return Ok(());
    }

    // Sync existing nodes that are missing from the state machine.
    // This ensures new nodes learn about all cluster members via log replay.
    let sm = self.node.state_machine();
    for (id, node) in membership.nodes() {
      if sm
        .contains_node(*id)
        .map_err(|e| Error::internal(format!("contains_node check failed: {}", e)))?
      {
        continue;
      }
      info!("Syncing missing node {} to state machine", id);
      let entry = LogEntry::new(Cmd::AddNode {
        node: node.clone(),
        overriding: true,
      });
      map_err_log!(
        self.write(entry).await,
        format!("Failed to sync node {}", id)
      )?;
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

    map_err_log!(self.write(entry).await, "Failed to join node")?;
    info!("AddNode command written successfully for node {}", node_id);

    // Change membership to add the new node as voter (retain removed voters as learners)
    info!("Changing membership to add node {} as voter", node_id);
    let mut add_voters = BTreeMap::new();
    add_voters.insert(node_id, node);

    let msg = ChangeMembers::AddVoters(add_voters);
    map_err_log!(
      self.raft().change_membership(msg, false).await,
      "Failed to join node"
    )?;
    info!("Node {} joined successfully", node_id);

    Ok(())
  }

  /// Remove a node from the cluster (leader-only)
  ///
  /// Demotes to learner first, then removes from membership.
  /// Safe to call even if node is already not in cluster (idempotent).
  ///
  /// # Leader Requirement
  /// Must be called on the leader. Use `RaftNode::assume_leader()` first.
  pub async fn remove_node(&self, req: LeaveRequest) -> Result<()> {
    let node_id = req.node_id;

    // Get current membership and check if node exists
    let metrics = self.raft().metrics().borrow_watched().clone();
    let membership = metrics.membership_config.membership();

    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if !voters.contains(&node_id) {
      // Node not in cluster, consider it already left
      return Ok(());
    }

    // Write a log entry to remove the node
    let entry = LogEntry::new(Cmd::RemoveNode { node_id });

    map_err_log!(self.write(entry).await, "Failed to leave node")?;

    // Change membership to remove the node
    let mut remove_voters: BTreeSet<u64> = BTreeSet::new();
    remove_voters.insert(node_id);

    map_err_log!(
      self.raft().change_membership(remove_voters, true).await,
      "Failed to leave node"
    )?;

    Ok(())
  }

  /// Batch write entries atomically (leader-only)
  ///
  /// All entries are committed as a single raft log entry.
  /// Atomic: either all entries are applied or none are.
  ///
  /// # Leader Requirement
  /// Must be called on the leader. Use `RaftNode::assume_leader()` first.
  pub async fn batch_write(&self, req: BatchWriteReq) -> Result<AppliedState> {
    if req.entries.is_empty() {
      return Ok(AppliedState::None);
    }

    let entry = LogEntry::new(Cmd::BatchUpsertKV {
      entries: req.entries,
    });

    map_err_log!(
      self.do_write(entry).await,
      "Failed to write batch log entry"
    )
  }

  /// Read a value from the state machine (leader-only)
  ///
  /// Reads directly from the local state machine.
  /// Consider using `RaftNode::read()` which can read from any node.
  pub async fn read(&self, req: GetKVReq) -> Result<Option<Vec<u8>>> {
    map_err_log!(
      self.node.state_machine().get_kv(&req.key),
      "Failed to get kv"
    )
  }

  /// Scan keys by prefix from the state machine (leader-only)
  ///
  /// Returns all key-value pairs with keys starting with the given prefix.
  /// Results are sorted lexicographically by key.
  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    map_err_log!(
      self.node.state_machine().scan_prefix(&req.prefix),
      "Failed to scan prefix"
    )
  }

  /// Get cluster membership from state machine (leader-only)
  ///
  /// Returns the committed membership configuration.
  /// This reflects the last applied membership change.
  pub async fn get_members(&self, _req: GetMembersReq) -> Result<GetMembersReply> {
    map_err_log!(
      self.node.state_machine().get_nodes(),
      "Failed to get members"
    )
  }

  /// Execute a conditional transaction (leader-only)
  ///
  /// Evaluates conditions, then atomically executes if_then or else_then branch.
  /// Used for optimistic concurrency control.
  ///
  /// # Leader Requirement
  /// Must be called on the leader. Use `RaftNode::assume_leader()` first.
  pub async fn txn(&self, req: TxnReq) -> Result<TxnReply> {
    // Build a transaction command
    let entry = LogEntry::new(Cmd::Txn { req, result: None });

    match map_err_log!(self.do_write(entry).await, "Failed to execute transaction")? {
      AppliedState::Txn(reply) => Ok(reply),
      _ => {
        // Should not happen - Txn command always returns AppliedState::Txn
        error!("Unexpected AppliedState from transaction");
        Err(Error::internal("Unexpected response from transaction"))
      }
    }
  }

  /// Core write implementation (leader-only)
  ///
  /// Appends entry to raft log, replicates to followers, waits for commit.
  /// All write operations (write, batch_write, txn) delegate to this method.
  ///
  /// # Safety
  /// Caller must ensure this node is the leader. This method does not check.
  async fn do_write(&self, mut entry: LogEntry) -> Result<AppliedState> {
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
        Err(match e {
          RaftError::APIError(api_err) => {
            Error::internal(format!("client write error: {}", api_err))
          }
          RaftError::Fatal(fatal_err) => {
            Error::internal(format!("fatal raft error: {}", fatal_err))
          }
        })
      }
    }
  }
}
