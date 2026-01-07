use std::fmt;

use serde::{Deserialize, Serialize};

use super::{NodeId, endpoint::Endpoint};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Node {
  pub node_id: NodeId,

  pub endpoint: Endpoint,
}

impl fmt::Display for Node {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}={}", self.node_id, self.endpoint)
  }
}
