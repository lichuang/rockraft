use std::collections::HashMap;
use std::fs::File;
use std::io::SeekFrom;
use std::sync::Arc;

use openraft::raft::{
  AppendEntriesRequest, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::error::Error;
use crate::raft::protobuf as pb;

use crate::node::RaftNode;
use crate::raft::types::{ForwardRequest, ForwardResponse, Snapshot, TypeConfig, decode, encode};
use openraft::async_runtime::watch::WatchReceiver;
use pb::raft_service_server::RaftService;

/// State for receiving a snapshot chunk
struct StreamingSnapshot {
  snapshot_id: String,
  data: tokio::fs::File,
}

pub struct RaftServiceImpl {
  node: Arc<RaftNode>,
  /// Store ongoing snapshot streaming operations
  streaming_snapshots: Mutex<HashMap<String, StreamingSnapshot>>,
}

impl RaftServiceImpl {
  pub fn new(node: Arc<RaftNode>) -> Self {
    Self {
      node,
      streaming_snapshots: Mutex::new(HashMap::new()),
    }
  }

  fn result_to_raft_reply(result: Result<ForwardResponse, Error>) -> pb::RaftReply {
    match result {
      Ok(response) => {
        let data = encode(&response).expect("Failed to serialize ForwardResponse");
        pb::RaftReply {
          data,
          error: String::new().into(),
        }
      }
      Err(err) => {
        let error_msg = err.to_string();
        pb::RaftReply {
          data: Vec::new(),
          error: error_msg.into(),
        }
      }
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
    let forward_req: ForwardRequest = decode(&req.data)
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
    let append_req: AppendEntriesRequest<TypeConfig> = decode(&req.value)
      .map_err(|e| Status::internal(format!("Failed to deserialize append request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .append_entries(append_req)
      .await
      .map_err(|e| Status::internal(format!("AppendEntries failed: {}", e)))?;

    // Serialize the response
    let response_data = encode(&result)
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
    let vote_req: VoteRequest<TypeConfig> = decode(&req.value)
      .map_err(|e| Status::internal(format!("Failed to deserialize vote request: {}", e)))?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .vote(vote_req)
      .await
      .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;

    // Serialize the response
    let response_data = encode(&result)
      .map_err(|e| Status::internal(format!("Failed to serialize vote response: {}", e)))?;

    let reply = pb::VoteReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }

  /// Handle InstallSnapshot RPC from other Raft nodes
  ///
  /// This method receives snapshot chunks and assembles them into a complete snapshot.
  /// When all chunks are received (done=true), it installs the snapshot via install_full_snapshot.
  async fn snapshot(
    &self,
    request: Request<pb::SnapshotRequest>,
  ) -> Result<Response<pb::SnapshotReply>, Status> {
    let req = request.into_inner();

    // Deserialize the request
    let snapshot_req: InstallSnapshotRequest<TypeConfig> = decode(&req.value)
      .map_err(|e| Status::internal(format!("Failed to deserialize snapshot request: {}", e)))?;

    let vote = snapshot_req.vote;
    let snapshot_id = snapshot_req.meta.snapshot_id.clone();
    let snapshot_meta = snapshot_req.meta.clone();
    let offset = snapshot_req.offset;
    let done = snapshot_req.done;

    let mut streaming_guard = self.streaming_snapshots.lock().await;

    // Check if this is a new snapshot or continuation
    let curr_id = streaming_guard
      .get(&snapshot_id)
      .map(|s| s.snapshot_id.clone());

    if curr_id != Some(snapshot_id.clone()) {
      // New snapshot - must start at offset 0
      if offset != 0 {
        return Err(Status::internal(format!(
          "Snapshot mismatch: expected offset 0 for new snapshot {}, got {}",
          snapshot_id, offset
        )));
      }

      // Create a temporary file for receiving the snapshot
      let std_file =
        File::create(std::env::temp_dir().join(format!("rockraft_snapshot_{}", snapshot_id)))
          .map_err(|e| Status::internal(format!("Failed to create temp file: {}", e)))?;
      let tokio_file = tokio::fs::File::from_std(std_file);

      streaming_guard.insert(
        snapshot_id.clone(),
        StreamingSnapshot {
          snapshot_id: snapshot_id.clone(),
          data: tokio_file,
        },
      );
    }

    // Get the streaming snapshot entry and write the chunk
    let streaming = streaming_guard
      .get_mut(&snapshot_id)
      .ok_or_else(|| Status::internal("Streaming snapshot not found"))?;

    // Seek to the correct position and write the data
    streaming
      .data
      .seek(SeekFrom::Start(offset))
      .await
      .map_err(|e| Status::internal(format!("Failed to seek: {}", e)))?;

    streaming
      .data
      .write_all(&snapshot_req.data)
      .await
      .map_err(|e| Status::internal(format!("Failed to write snapshot data: {}", e)))?;

    // If done, finalize the snapshot
    if done {
      let streaming = streaming_guard
        .remove(&snapshot_id)
        .ok_or_else(|| Status::internal("Streaming snapshot not found"))?;

      let mut data = streaming.data;

      // Sync data to disk
      data
        .shutdown()
        .await
        .map_err(|e| Status::internal(format!("Failed to shutdown file: {}", e)))?;

      // Create the snapshot and install it
      let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: data,
      };

      let result = self
        .node
        .raft()
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(|e| Status::internal(format!("InstallFullSnapshot failed: {}", e)))?;

      // Serialize the response
      let response_data = encode(&result)
        .map_err(|e| Status::internal(format!("Failed to serialize snapshot response: {}", e)))?;

      let reply = pb::SnapshotReply {
        value: response_data,
      };

      return Ok(Response::new(reply));
    }

    // Return intermediate response with current vote
    let my_vote = WatchReceiver::borrow_watched(&self.node.raft().metrics()).vote;
    let resp = InstallSnapshotResponse::<TypeConfig> { vote: my_vote };
    let response_data = encode(&resp)
      .map_err(|e| Status::internal(format!("Failed to serialize snapshot response: {}", e)))?;

    let reply = pb::SnapshotReply {
      value: response_data,
    };

    Ok(Response::new(reply))
  }
}
