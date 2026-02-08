use crate::error::ManagementError;
use crate::error::OpenRaft;
use crate::error::Result;
use crate::error::RockRaftError;
use crate::node::RaftNode;
use crate::raft::types::AppliedState;
use crate::raft::types::JoinRequest;
use crate::raft::types::LogEntry;
use crate::raft::types::Node;
use crate::raft::types::TypeConfig;
use anyerror::AnyError;
use openraft::Raft;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::debug;
use tracing::error;

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

  /// Join a node to the cluster
  ///
  /// This function adds a node to the raft cluster. It first adds the node as a learner,
  /// then changes the membership to include the new node as a voter.
  ///
  /// # Arguments
  /// * `req` - The JoinRequest containing node_id and endpoint
  ///
  /// # Returns
  /// * `Ok(())` - If the node was successfully added to the cluster
  /// * `Err(RockRaftError)` - If the operation failed
  pub async fn join(&self, req: JoinRequest) -> Result<()> {
    let node_id = req.node_id;
    let node = Node {
      node_id,
      endpoint: req.endpoint,
    };

    // Add the node as a learner first (non-blocking)
    self
      .raft()
      .add_learner(node_id, node, false)
      .await
      .map_err(|e| RockRaftError::Management(ManagementError::Join(AnyError::new(&e))))?;

    // Get current membership and add the new node as voter
    let metrics = self.raft().metrics().borrow().clone();
    let membership = metrics.membership_config.membership();

    // Build new voter set including existing voters and the new node
    let mut new_voters: BTreeSet<u64> = membership.voter_ids().collect();
    new_voters.insert(node_id);

    // Change membership to include the new node as voter (retain removed voters as learners)
    self
      .raft()
      .change_membership(new_voters, true)
      .await
      .map_err(|e| RockRaftError::Management(ManagementError::Join(AnyError::new(&e))))?;

    Ok(())
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
  /// * `Err(RockRaftError)` - If the operation failed or this node is not the leader
  pub async fn write(&self, mut entry: LogEntry) -> Result<AppliedState> {
    // Set the current timestamp in milliseconds, safe to unwrap
    let now = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
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
          openraft::error::RaftError::APIError(api_err) => {
            Err(RockRaftError::OpenRaft(OpenRaft::ClientWrite(api_err)))
          }
          openraft::error::RaftError::Fatal(fatal_err) => {
            Err(RockRaftError::OpenRaft(OpenRaft::Fatal(fatal_err)))
          }
        }
      }
    }
  }
}
