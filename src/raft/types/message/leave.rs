use crate::raft::types::NodeId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
  pub node_id: NodeId,
}
