use crate::raft::protobuf as pb;
use crate::raft::types::message::JoinRequest;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRequestBody {
  Join(JoinRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
  pub forward_to_leader: u64,
  pub body: ForwardRequestBody,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardResponse {}

impl tonic::IntoRequest<pb::RaftRequest> for ForwardRequest {
  fn into_request(self) -> tonic::Request<pb::RaftRequest> {
    let data = bincode::serialize(&self).expect("fail to serialize ForwardRequest");
    let req = pb::RaftRequest { data };
    tonic::Request::new(req)
  }
}
