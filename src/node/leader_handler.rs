use crate::config::ReadConsistency;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::Result;
use crate::error::RetryReason;
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
use crate::raft::types::NodeId;
use crate::raft::types::ScanPrefixReply;
use crate::raft::types::ScanPrefixReq;
use crate::raft::types::TxnReply;
use crate::raft::types::TxnReq;
use crate::raft::types::TypeConfig;
use openraft::ChangeMembers;
use openraft::Raft;
use openraft::ReadPolicy;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::ClientWriteError;
use openraft::error::LinearizableReadError;
use openraft::error::RaftError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

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

/// Log and contextualize an internal `Error`, but pass leader redirects
/// through unchanged.
///
/// Write wrappers (batch_write/txn/add_node/remove_node) must not flatten
/// `ForwardToLeader` into a non-retryable internal error: the caller needs
/// the redirect and its leader id to forward the request to the real leader.
fn preserve_redirect(e: Error, context: &str) -> Error {
  if matches!(e.kind(), ErrorKind::ForwardToLeader { .. }) {
    debug!("{}: {:?}", context, e);
    return e;
  }
  error!("{}: {:?}", context, e);
  Error::internal(format!("{}: {}", context, e))
}

/// Convert a client-write RaftError into our Error, preserving redirects.
///
/// A node can step down between `assume_leader()` and the actual write;
/// openraft then returns `ForwardToLeader`. That must NOT be flattened
/// into a non-retryable internal error — the caller needs the redirect
/// (and its leader id) to forward the request to the real leader.
fn map_client_write_err(
  e: RaftError<TypeConfig, ClientWriteError<TypeConfig>>,
  context: &str,
) -> Error {
  if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
    debug!(
      "{}: stepped down, forward to leader {:?}",
      context, forward.leader_id
    );
    return Error::forward_to_leader(forward.leader_id);
  }

  error!("{}: {:?}", context, e);
  match e {
    RaftError::APIError(api_err) => Error::internal(format!("{}: {}", context, api_err)),
    RaftError::Fatal(fatal_err) => {
      Error::internal(format!("{}: fatal raft error: {}", context, fatal_err))
    }
  }
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
  fn is_voter(&self, node_id: NodeId) -> bool {
    self
      .raft()
      .metrics()
      .borrow_watched()
      .membership_config
      .membership()
      .voter_ids()
      .any(|id| id == node_id)
  }

  pub async fn add_node(&self, req: JoinRequest) -> Result<()> {
    let node_id = req.node_id;
    info!("Handling join request for node {}", node_id);

    if self.is_voter(node_id) {
      info!("Node {} already in membership, skipping join", node_id);
      return Ok(());
    }

    let membership = self
      .raft()
      .metrics()
      .borrow_watched()
      .membership_config
      .membership()
      .clone();

    // Sync all existing nodes to the log so the joining node learns about
    // every cluster member via log replay.
    for (id, node) in membership.nodes() {
      info!("Syncing node {} to state machine", id);
      let entry = LogEntry::new(Cmd::AddNode {
        node: node.clone(),
        overriding: true,
      });
      self
        .write(entry)
        .await
        .map_err(|e| preserve_redirect(e, &format!("Failed to sync node {}", id)))?;
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

    self
      .write(entry)
      .await
      .map_err(|e| preserve_redirect(e, "Failed to join node"))?;
    info!("AddNode command written successfully for node {}", node_id);

    // Change membership to add the new node as voter (retain removed voters as learners)
    info!("Changing membership to add node {} as voter", node_id);
    let mut add_voters = BTreeMap::new();
    add_voters.insert(node_id, node);

    let msg = ChangeMembers::AddVoters(add_voters);
    self
      .raft()
      .change_membership(msg, false)
      .await
      .map_err(|e| map_client_write_err(e, "Failed to join node"))?;
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

    if !self.is_voter(node_id) {
      return Ok(());
    }

    // Write a log entry to remove the node
    let entry = LogEntry::new(Cmd::RemoveNode { node_id });

    self
      .write(entry)
      .await
      .map_err(|e| preserve_redirect(e, "Failed to leave node"))?;

    // Change membership to remove the node
    let mut remove_voters: BTreeSet<u64> = BTreeSet::new();
    remove_voters.insert(node_id);

    self
      .raft()
      .change_membership(remove_voters, true)
      .await
      .map_err(|e| map_client_write_err(e, "Failed to leave node"))?;

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

    self
      .do_write(entry)
      .await
      .map_err(|e| preserve_redirect(e, "Failed to write batch log entry"))
  }

  /// Read a value from the state machine (leader-only)
  ///
  /// Read a value, honoring the configured read consistency level.
  ///
  /// `LeaseRead`/`ReadIndex` run the openraft leadership confirmation and
  /// state-machine catch-up before the lookup; the weaker levels read the
  /// local state machine directly. This is reached only on the leader (via
  /// the forward path), so the confirmation here applies to the node that
  /// answers.
  pub async fn read(&self, req: GetKVReq) -> Result<Option<Vec<u8>>> {
    self
      .linearizable_read(|| {
        map_err_log!(
          self.node.state_machine().get_kv(&req.key),
          "Failed to get kv"
        )
      })
      .await
  }

  /// Scan keys by prefix, honoring the configured read consistency level.
  ///
  /// Same confirmation flow as `read`.
  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    self
      .linearizable_read(|| {
        map_err_log!(
          self.node.state_machine().scan_prefix(&req.prefix),
          "Failed to scan prefix"
        )
      })
      .await
  }

  /// Run `f` behind the configured read-consistency confirmation.
  ///
  /// `LeaseRead`/`ReadIndex` call `raft.ensure_linearizable` with the
  /// matching openraft policy before running `f`; the other levels run `f`
  /// directly. The confirmation error is retryable — leadership may be in
  /// transition, and the caller (CoreDB) retries.
  async fn linearizable_read<T, F>(&self, f: F) -> Result<T>
  where
    F: FnOnce() -> Result<T>,
  {
    match self.node.config.raft.read_consistency {
      ReadConsistency::LeaderApplied | ReadConsistency::LocalApplied => f(),
      ReadConsistency::LeaseRead => {
        self.confirm_read(ReadPolicy::LeaseRead).await?;
        f()
      }
      ReadConsistency::ReadIndex => {
        self.confirm_read(ReadPolicy::ReadIndex).await?;
        f()
      }
    }
  }

  /// Confirm leadership for a linearizable read, mapping openraft's error to
  /// a retryable library error.
  async fn confirm_read(&self, policy: ReadPolicy) -> Result<()> {
    self
      .raft()
      .ensure_linearizable(policy)
      .await
      .map_err(|e| match e {
        RaftError::APIError(LinearizableReadError::ForwardToLeader(leader_id)) => {
          debug!("linearizable read redirect: leader_id={:?}", leader_id);
          Error::retryable_with_reason(RetryReason::LeaderTransition)
        }
        other => Error::internal(format!("linearizable read confirmation failed: {}", other)),
      })?;
    Ok(())
  }

  /// Get cluster membership from the state machine
  ///
  /// Returns the applied membership configuration (voters and learners).
  /// Reached via the forward path: followers route GetMembers to the leader,
  /// and the leader answers from its own applied state.
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
    let entry = LogEntry::new(Cmd::Txn { req });

    match self
      .do_write(entry)
      .await
      .map_err(|e| preserve_redirect(e, "Failed to execute transaction"))?
    {
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
    entry.time_ms = Some(crate::utils::now_millis());

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
        Err(map_client_write_err(e, "client write"))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::preserve_redirect;
  use crate::error::Error;

  #[test]
  fn test_preserve_redirect_passes_forward_to_leader_through() {
    // Write wrappers (batch_write/txn/add_node/remove_node) must not flatten
    // a leader redirect into a non-retryable internal error.
    let err = preserve_redirect(Error::forward_to_leader(Some(3)), "test op");
    assert_eq!(err.forward_leader_id(), Some(3));
    assert!(err.is_retryable());
  }

  #[test]
  fn test_preserve_redirect_wraps_other_errors() {
    let err = preserve_redirect(Error::internal("boom"), "test op");
    assert!(!err.is_retryable());
    assert!(err.to_string().contains("test op"));
  }
}
