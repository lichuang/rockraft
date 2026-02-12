use crate::raft::protobuf as pb;
use crate::raft::types::AppliedState;
use crate::raft::types::LogEntry;
use crate::raft::types::message::GetMembersReq;
use crate::raft::types::message::GetMembersReply;
use crate::raft::types::message::JoinRequest;
use crate::raft::types::message::LeaveRequest;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetKVReq {
  pub key: String,
}

pub type GetKVReply = Option<Vec<u8>>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRequestBody {
  Join(JoinRequest),
  Leave(LeaveRequest),
  GetMembers(GetMembersReq),
  Write(LogEntry),
  GetKV(GetKVReq),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
  pub forward_to_leader: u64,
  pub body: ForwardRequestBody,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardResponse {
  Join(()),
  Leave(()),
  GetMembers(GetMembersReply),
  Write(AppliedState),
  GetKV(GetKVReply),
}

impl tonic::IntoRequest<pb::RaftRequest> for ForwardRequest {
  fn into_request(self) -> tonic::Request<pb::RaftRequest> {
    let data = bincode::serialize(&self).expect("fail to serialize ForwardRequest");
    let req = pb::RaftRequest { data };
    tonic::Request::new(req)
  }
}
