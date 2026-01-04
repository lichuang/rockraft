use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::node::RaftNode;
use crate::raft::protobuf::raft_service_server::RaftService;

pub struct RaftServiceImpl {
  raft: Arc<RaftNode>,
}

impl RaftServiceImpl {
  /// Create a new RaftServiceImpl instance
  pub fn new(raft: Arc<RaftNode>) -> Self {
    Self { raft }
  }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
  /// Handle AppendEntries RPC from other Raft nodes
  async fn append(
    &self,
    request: Request<crate::raft::protobuf::AppendRequest>,
  ) -> Result<Response<crate::raft::protobuf::AppendReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let append_req: openraft::raft::AppendEntriesRequest<crate::raft::types::TypeConfig> =
      bincode::deserialize(&req.value)
        .map_err(|e| Status::internal(format!("Failed to deserialize append request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .raft
      .raft()
      .append_entries(append_req)
      .await
      .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize append response: {}", e)))?;

    let reply = crate::raft::protobuf::AppendReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }

  /// Handle Vote RPC from other Raft nodes
  async fn vote(
    &self,
    request: Request<crate::raft::protobuf::VoteRequest>,
  ) -> Result<Response<crate::raft::protobuf::VoteReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let vote_req: openraft::raft::VoteRequest<crate::raft::types::TypeConfig> =
      bincode::deserialize(&req.value)
        .map_err(|e| Status::internal(format!("Failed to deserialize vote request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .raft
      .raft()
      .vote(vote_req)
      .await
      .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize vote response: {}", e)))?;

    let reply = crate::raft::protobuf::VoteReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }

  /// Handle InstallSnapshot RPC from other Raft nodes
  async fn snapshot(
    &self,
    request: Request<crate::raft::protobuf::SnapshotRequest>,
  ) -> Result<Response<crate::raft::protobuf::SnapshotReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let snapshot_req: openraft::raft::InstallSnapshotRequest<crate::raft::types::TypeConfig> =
      bincode::deserialize(&req.value)
        .map_err(|e| Status::internal(format!("Failed to deserialize snapshot request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .raft
      .raft()
      .install_snapshot(snapshot_req)
      .await
      .map_err(|e| Status::internal(format!("InstallSnapshot failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize snapshot response: {}", e)))?;

    let reply = crate::raft::protobuf::SnapshotReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }
}
