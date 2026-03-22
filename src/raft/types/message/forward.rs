use crate::raft::protobuf as pb;
use crate::raft::types::AppliedState;
use crate::raft::types::LogEntry;
use crate::raft::types::TxnReply;
use crate::raft::types::TxnReq;
use crate::raft::types::UpsertKV;
use crate::raft::types::encode;
use crate::raft::types::message::GetMembersReply;
use crate::raft::types::message::GetMembersReq;
use crate::raft::types::message::JoinRequest;
use crate::raft::types::message::LeaveRequest;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetKVReq {
  pub key: String,
}

pub type GetKVReply = Option<Vec<u8>>;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ScanPrefixReq {
  pub prefix: Vec<u8>,
}

pub type ScanPrefixReply = Vec<(Vec<u8>, Vec<u8>)>;

/// Request for batch write operation
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BatchWriteReq {
  /// Entries to upsert or delete atomically
  pub entries: Vec<UpsertKV>,
}

/// Response for batch write operation
pub type BatchWriteReply = AppliedState;

/// Request for transaction operation
pub type TxnRequest = TxnReq;

/// Response for transaction operation
pub type TxnResponse = TxnReply;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ForwardRequestBody {
  Join(JoinRequest),
  Leave(LeaveRequest),
  GetMembers(GetMembersReq),
  Write(LogEntry),
  GetKV(GetKVReq),
  ScanPrefix(ScanPrefixReq),
  BatchWrite(BatchWriteReq),
  Txn(TxnRequest),
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
  ScanPrefix(ScanPrefixReply),
  BatchWrite(BatchWriteReply),
  Txn(TxnResponse),
}

impl tonic::IntoRequest<pb::RaftRequest> for ForwardRequest {
  fn into_request(self) -> tonic::Request<pb::RaftRequest> {
    let data = encode(&self).expect("fail to serialize ForwardRequest");
    let req = pb::RaftRequest { data };
    tonic::Request::new(req)
  }
}
