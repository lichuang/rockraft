use crate::raft::protobuf as pb;
use tonic::Status;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRequestBody {}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
  pub forward_to_leader: u64,
  pub body: ForwardRequestBody,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardResponse {}

impl tonic::IntoRequest<pb::RaftRequest> for ForwardRequest {
  fn into_request(self) -> tonic::Request<pb::RaftRequest> {
    let data = bincode::serialize(&self).expect("fail to serialize ForwardRequest");
    let req = pb::RaftRequest { data };
    tonic::Request::new(req)
  }
}
