use serde::{Deserialize, Serialize};

use super::{NodeId, endpoint::Endpoint};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Node {
  pub node_id: NodeId,

  /// Raft service endpoint to connect to.
  pub endpoint: Endpoint,
}
