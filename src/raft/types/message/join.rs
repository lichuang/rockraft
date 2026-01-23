use crate::raft::protobuf as pb;
use crate::raft::types::Endpoint;
use crate::raft::types::NodeId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinRequest {
  pub node_id: NodeId,
  pub endpoint: Endpoint,
}
