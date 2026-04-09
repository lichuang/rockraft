use crate::error::{Error, Result};
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

// TxnReq and TxnReply are defined in crate::raft::types::cmd::txn
// They are re-exported in crate::raft::types::mod for convenience

/// The payload of a forwarded request
///
/// This enum represents all possible operations that can be forwarded
/// to the leader for execution.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RequestPayload {
  Join(JoinRequest),
  Leave(LeaveRequest),
  GetMembers(GetMembersReq),
  Write(LogEntry),
  GetKV(GetKVReq),
  ScanPrefix(ScanPrefixReq),
  BatchWrite(BatchWriteReq),
  Txn(TxnReq),
}

/// Deprecated: Use `RequestPayload` instead
#[deprecated(since = "0.1.6", note = "Use RequestPayload instead")]
#[allow(dead_code)]
pub type ForwardRequestBody = RequestPayload;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
  pub forward_to_leader: u64,
  pub body: RequestPayload,
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
  Txn(TxnReply),
}

impl ForwardResponse {
  /// Return the variant name as a static string, for error messages
  fn variant_name(&self) -> &'static str {
    match self {
      ForwardResponse::Write(_) => "Write",
      ForwardResponse::BatchWrite(_) => "BatchWrite",
      ForwardResponse::Txn(_) => "Txn",
      ForwardResponse::GetKV(_) => "GetKV",
      ForwardResponse::ScanPrefix(_) => "ScanPrefix",
      ForwardResponse::Join(_) => "Join",
      ForwardResponse::Leave(_) => "Leave",
      ForwardResponse::GetMembers(_) => "GetMembers",
    }
  }

  /// Extract a Write response
  pub fn into_write(self) -> Result<AppliedState> {
    match self {
      ForwardResponse::Write(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected Write response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a BatchWrite response
  pub fn into_batch_write(self) -> Result<BatchWriteReply> {
    match self {
      ForwardResponse::BatchWrite(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected BatchWrite response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a Txn response
  pub fn into_txn(self) -> Result<TxnReply> {
    match self {
      ForwardResponse::Txn(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected Txn response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a GetKV response
  pub fn into_get_kv(self) -> Result<GetKVReply> {
    match self {
      ForwardResponse::GetKV(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected GetKV response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a ScanPrefix response
  pub fn into_scan_prefix(self) -> Result<ScanPrefixReply> {
    match self {
      ForwardResponse::ScanPrefix(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected ScanPrefix response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a Join response
  pub fn into_join(self) -> Result<()> {
    match self {
      ForwardResponse::Join(()) => Ok(()),
      other => Err(Error::internal(format!(
        "Expected Join response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a Leave response
  pub fn into_leave(self) -> Result<()> {
    match self {
      ForwardResponse::Leave(()) => Ok(()),
      other => Err(Error::internal(format!(
        "Expected Leave response, got {}",
        other.variant_name()
      ))),
    }
  }

  /// Extract a GetMembers response
  pub fn into_get_members(self) -> Result<GetMembersReply> {
    match self {
      ForwardResponse::GetMembers(r) => Ok(r),
      other => Err(Error::internal(format!(
        "Expected GetMembers response, got {}",
        other.variant_name()
      ))),
    }
  }
}

impl tonic::IntoRequest<pb::RaftRequest> for ForwardRequest {
  fn into_request(self) -> tonic::Request<pb::RaftRequest> {
    let data = encode(&self).expect("fail to serialize ForwardRequest");
    let req = pb::RaftRequest { data };
    tonic::Request::new(req)
  }
}
