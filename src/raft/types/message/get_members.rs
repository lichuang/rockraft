use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;

use crate::raft::types::Membership;
use crate::raft::types::Node;
use crate::raft::types::NodeId;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct GetMembersReq {}

/// The applied membership, split the way OpenRaft keeps it.
///
/// `nodes` holds voters and learners alike, because OpenRaft tracks both in the
/// membership's node map; `voters` names the subset that votes. Reporting only
/// the node table, as this reply used to, made a learner indistinguishable from
/// a voter.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct GetMembersReply {
  pub voters: BTreeSet<NodeId>,
  pub nodes: BTreeMap<NodeId, Node>,
}

impl GetMembersReply {
  /// Build the reply from an applied membership.
  pub fn from_membership(membership: &Membership) -> Self {
    let voters = membership.voter_ids().collect();
    let nodes = membership
      .nodes()
      .map(|(node_id, node)| (*node_id, node.clone()))
      .collect();
    Self { voters, nodes }
  }

  /// The nodes that belong to the membership without voting.
  pub fn learners(&self) -> BTreeSet<NodeId> {
    self
      .nodes
      .keys()
      .copied()
      .filter(|node_id| !self.voters.contains(node_id))
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use std::collections::BTreeMap;
  use std::collections::BTreeSet;

  use super::*;
  use crate::config::Endpoint;

  fn node(node_id: NodeId) -> Node {
    Node {
      node_id,
      endpoint: Endpoint::new("127.0.0.1", 7000 + node_id as u32),
    }
  }

  /// A learner sits in the membership's node map but not in its voter set, and
  /// the reply has to keep that difference visible.
  #[test]
  fn test_get_members_reply_keeps_learners_out_of_the_voter_set() {
    let voters = BTreeSet::from([1, 2]);
    let nodes = BTreeMap::from([(1, node(1)), (2, node(2)), (3, node(3))]);
    let membership = Membership::new(vec![voters], nodes).unwrap();

    let reply = GetMembersReply::from_membership(&membership);

    assert_eq!(reply.voters, BTreeSet::from([1, 2]));
    assert_eq!(
      reply.nodes.keys().copied().collect::<Vec<_>>(),
      vec![1, 2, 3]
    );
    assert_eq!(reply.learners(), BTreeSet::from([3]));
  }
}
