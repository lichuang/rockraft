use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::raft::{
  AppendEntriesRequest, InstallSnapshotRequest, InstallSnapshotResponse, SnapshotResponse,
  VoteRequest,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::error::Error;
use crate::raft::protobuf as pb;

use crate::node::RaftNode;
use crate::raft::types::{
  ForwardRequest, ForwardResponse, Snapshot, SnapshotMeta, TypeConfig, decode, encode,
};
use openraft::async_runtime::watch::WatchReceiver;
use pb::raft_service_server::RaftService;

/// Abandoned streaming snapshots are evicted after this timeout.
const STALE_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(300);

/// Helper trait for converting errors to tonic::Status
trait IntoStatus<T> {
  /// Convert error to Status::internal with the given context message
  fn map_internal(self, context: &str) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> IntoStatus<T> for Result<T, E> {
  fn map_internal(self, context: &str) -> Result<T, Status> {
    self.map_err(|e| Status::internal(format!("{}: {}", context, e)))
  }
}

/// State for receiving a snapshot chunk
struct StreamingSnapshot {
  data: tokio::fs::File,
  created_at: Instant,
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
      Ok(response) => match encode(&response) {
        Ok(data) => pb::RaftReply {
          data,
          error: String::new().into(),
        },
        Err(e) => {
          tracing::error!("Failed to serialize ForwardResponse: {}", e);
          pb::RaftReply {
            data: Vec::new(),
            error: format!("internal error: serialize failed: {}", e).into(),
          }
        }
      },
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
    let forward_req: ForwardRequest =
      decode(&req.data).map_internal("Failed to deserialize forward request")?;
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
    let append_req: AppendEntriesRequest<TypeConfig> =
      decode(&req.value).map_internal("Failed to deserialize append request")?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .append_entries(append_req)
      .await
      .map_internal("AppendEntries failed")?;

    // Serialize the response
    let response_data = encode(&result).map_internal("Failed to serialize append response")?;

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
    let vote_req: VoteRequest<TypeConfig> =
      decode(&req.value).map_internal("Failed to deserialize vote request")?;

    // Forward to Raft instance
    let result = self
      .node
      .raft()
      .vote(vote_req)
      .await
      .map_internal("Vote failed")?;

    // Serialize the response
    let response_data = encode(&result).map_internal("Failed to serialize vote response")?;

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

    let snapshot_req: InstallSnapshotRequest<TypeConfig> =
      decode(&req.value).map_internal("Failed to deserialize snapshot request")?;

    let vote = snapshot_req.vote;
    let snapshot_id = snapshot_req.meta.snapshot_id.clone();
    let snapshot_meta = snapshot_req.meta.clone();
    let offset = snapshot_req.offset;
    let done = snapshot_req.done;

    // Phase 1: ensure a streaming entry exists and write the chunk
    let maybe_complete = {
      let mut guard = self.streaming_snapshots.lock().await;
      self.evict_stale(&mut guard);

      self
        .ensure_streaming_entry(&mut guard, &snapshot_id, offset)
        .await?;

      self
        .write_chunk(&mut guard, &snapshot_id, offset, &snapshot_req.data)
        .await?;

      if done {
        Some(guard.remove(&snapshot_id).unwrap())
      } else {
        None
      }
    };

    // Phase 2: finalize or return intermediate response
    match maybe_complete {
      Some(streaming) => {
        let response = self
          .install_snapshot(vote, snapshot_meta, streaming)
          .await?;
        let reply = pb::SnapshotReply {
          value: encode(&response).map_internal("Failed to serialize snapshot response")?,
        };
        Ok(Response::new(reply))
      }
      None => {
        let vote = WatchReceiver::borrow_watched(&self.node.raft().metrics()).vote;
        let resp = InstallSnapshotResponse::<TypeConfig> { vote };
        let reply = pb::SnapshotReply {
          value: encode(&resp).map_internal("Failed to serialize snapshot response")?,
        };
        Ok(Response::new(reply))
      }
    }
  }
}

impl RaftServiceImpl {
  fn evict_stale(&self, snapshots: &mut HashMap<String, StreamingSnapshot>) {
    snapshots.retain(|_, s| s.created_at.elapsed() < STALE_SNAPSHOT_TIMEOUT);
  }

  async fn ensure_streaming_entry(
    &self,
    snapshots: &mut HashMap<String, StreamingSnapshot>,
    snapshot_id: &str,
    offset: u64,
  ) -> Result<(), Status> {
    let exists = snapshots.get(snapshot_id).is_some();
    if exists {
      return Ok(());
    }

    if offset != 0 {
      return Err(Status::internal(format!(
        "Snapshot mismatch: expected offset 0 for new snapshot {}, got {}",
        snapshot_id, offset
      )));
    }

    let std_file = self
      .node
      .state_machine()
      .create_snapshot_temp_file(snapshot_id)
      .map_internal("Failed to create snapshot temp file")?;

    snapshots.insert(
      snapshot_id.to_string(),
      StreamingSnapshot {
        data: tokio::fs::File::from_std(std_file),
        created_at: Instant::now(),
      },
    );

    Ok(())
  }

  async fn write_chunk(
    &self,
    snapshots: &mut HashMap<String, StreamingSnapshot>,
    snapshot_id: &str,
    offset: u64,
    data: &[u8],
  ) -> Result<(), Status> {
    let streaming = snapshots
      .get_mut(snapshot_id)
      .ok_or_else(|| Status::internal("Streaming snapshot not found"))?;

    streaming
      .data
      .seek(SeekFrom::Start(offset))
      .await
      .map_internal("Failed to seek")?;

    streaming
      .data
      .write_all(data)
      .await
      .map_internal("Failed to write snapshot data")?;

    Ok(())
  }

  async fn install_snapshot(
    &self,
    vote: openraft::Vote<TypeConfig>,
    snapshot_meta: SnapshotMeta,
    streaming: StreamingSnapshot,
  ) -> Result<SnapshotResponse<TypeConfig>, Status> {
    let mut file = streaming.data;
    file
      .shutdown()
      .await
      .map_internal("Failed to shutdown file")?;

    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    self
      .node
      .raft()
      .install_full_snapshot(vote, snapshot)
      .await
      .map_internal("InstallFullSnapshot failed")
  }
}
