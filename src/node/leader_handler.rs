use crate::error::ManagementError;
use crate::error::Result;
use crate::error::RockRaftError;
use crate::node::RaftNode;
use crate::raft::types::JoinRequest;
use crate::raft::types::Node;
use crate::raft::types::TypeConfig;
use anyerror::AnyError;
use openraft::Raft;
use std::collections::BTreeSet;
use std::sync::Arc;

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
}
