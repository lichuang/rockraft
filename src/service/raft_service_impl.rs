use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::raft::protobuf as pb;

use crate::node::RaftNode;
use crate::raft::types::{ForwardRequest, ForwardResponse, TypeConfig};
use openraft::raft;
use pb::raft_service_server::RaftService;

pub struct RaftServiceImpl {
  node: Arc<RaftNode>,
}

impl RaftServiceImpl {
  pub fn new(node: Arc<RaftNode>) -> Self {
    Self { node }
  }

  fn result_to_raft_reply(result: Result<ForwardResponse, Status>) -> pb::RaftReply {
    match result {
      Ok(response) => {
        let data = bincode::serialize(&response).expect("Failed to serialize ForwardResponse");
        pb::RaftReply {
          data,
          error: String::new().into(),
        }
      }
      Err(status) => pb::RaftReply {
        data: Vec::new(),
        error: status.to_string().into(),
      },
    }
  }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
  async fn forward(
    &self,
    request: Request<pb::RaftRequest>,
  ) -> Result<Response<pb::RaftReply>, Status> {
    let req = request.into_inner();
    let forward_req: ForwardRequest = bincode::deserialize(&req.data)
      .map_err(|e| Status::internal(format!("Failed to deserialize forward request: {}", e)))?;
    let response = self.node.handle_forward_request(forward_req).await;
    let reply = Self::result_to_raft_reply(response);
    Ok(Response::new(reply))
  }

  /// Handle AppendEntries RPC from other Raft nodes
  async fn append(
    &self,
    request: Request<pb::AppendRequest>,
  ) -> Result<Response<pb::AppendReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let append_req: raft::AppendEntriesRequest<TypeConfig> = bincode::deserialize(&req.value)
      .map_err(|e| Status::internal(format!("Failed to deserialize append request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .append_entries(append_req)
      .await
      .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize append response: {}", e)))?;

    let reply = pb::AppendReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }

  /// Handle Vote RPC from other Raft nodes
  async fn vote(
    &self,
    request: Request<pb::VoteRequest>,
  ) -> Result<Response<pb::VoteReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let vote_req: raft::VoteRequest<TypeConfig> = bincode::deserialize(&req.value)
      .map_err(|e| Status::internal(format!("Failed to deserialize vote request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .vote(vote_req)
      .await
      .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize vote response: {}", e)))?;

    let reply = pb::VoteReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }

  /// Handle InstallSnapshot RPC from other Raft nodes
  async fn snapshot(
    &self,
    request: Request<pb::SnapshotRequest>,
  ) -> Result<Response<pb::SnapshotReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let snapshot_req: raft::InstallSnapshotRequest<TypeConfig> =
      bincode::deserialize(&req.value)
        .map_err(|e| Status::internal(format!("Failed to deserialize snapshot request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .install_snapshot(snapshot_req)
      .await
      .map_err(|e| Status::internal(format!("InstallSnapshot failed: {}", e)))?;

    // Serialize the response
    let response_data = bincode::serialize(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize snapshot response: {}", e)))?;

    let reply = pb::SnapshotReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }
}
