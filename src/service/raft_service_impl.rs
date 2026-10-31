use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::raft::{
  AppendEntriesRequest, InstallSnapshotRequest, InstallSnapshotResponse, SnapshotResponse,
  VoteRequest,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::error::{ApiError, Error};
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
  /// Where the chunks are assembled; removed once the install is over.
  path: PathBuf,
  data: tokio::fs::File,
  created_at: Instant,
  /// Total bytes accepted so far; equals the next expected offset.
  received: u64,
}

/// Turn a fully received snapshot file into something `install_snapshot` can
/// read.
///
/// The chunks were written at explicit offsets, so the handle's cursor sits at
/// the end of the file while `install_snapshot` reads from the current
/// position: rewinding is what makes the data visible to the zstd decoder.
/// The length check catches a transfer whose last chunk was lost or replayed.
async fn prepare_for_install(streaming: &mut StreamingSnapshot) -> Result<(), Status> {
  streaming
    .data
    .flush()
    .await
    .map_internal("Failed to flush snapshot data")?;

  let len = streaming
    .data
    .metadata()
    .await
    .map_internal("Failed to stat snapshot file")?
    .len();

  if len != streaming.received {
    return Err(Status::invalid_argument(format!(
      "Snapshot file holds {} bytes but {} were streamed",
      len, streaming.received
    )));
  }

  streaming
    .data
    .seek(SeekFrom::Start(0))
    .await
    .map_internal("Failed to rewind snapshot file")?;

  Ok(())
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
          error: Vec::new(),
        },
        Err(e) => {
          tracing::error!("Failed to serialize ForwardResponse: {}", e);
          Self::error_reply(&Error::internal(format!("serialize failed: {}", e)))
        }
      },
      Err(err) => Self::error_reply(&err),
    }
  }

  /// Encode an error into the reply using the wire format: a
  /// postcard-serialized `ApiError`. Every client site decodes it the
  /// same way, so redirects keep their leader id and stay retryable
  /// across the wire.
  fn error_reply(err: &Error) -> pb::RaftReply {
    let api_err = ApiError::from_error(err);
    match encode(&api_err) {
      Ok(data) => pb::RaftReply {
        data: Vec::new(),
        error: data,
      },
      Err(e) => {
        tracing::error!("Failed to serialize ApiError: {}", e);
        pb::RaftReply {
          data: Vec::new(),
          error: format!("internal error: {}", err).into(),
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
        // The entry must exist (write_chunk just accepted the chunk); treat
        // a miss as a protocol error rather than panicking the RPC task.
        Some(guard.remove(&snapshot_id).ok_or_else(|| {
          Status::internal(format!(
            "Streaming snapshot {} evicted before completion",
            snapshot_id
          ))
        })?)
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
    snapshots.retain(|_, s| {
      let fresh = s.created_at.elapsed() < STALE_SNAPSHOT_TIMEOUT;
      if !fresh {
        // Best effort: an abandoned transfer must not leave a partial file
        // behind in the snapshot directory.
        let _ = std::fs::remove_file(&s.path);
      }
      fresh
    });
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

    let path = self
      .node
      .state_machine()
      .snapshot_temp_file_path(snapshot_id);
    let std_file = self
      .node
      .state_machine()
      .create_snapshot_temp_file(snapshot_id)
      .map_internal("Failed to create snapshot temp file")?;

    snapshots.insert(
      snapshot_id.to_string(),
      StreamingSnapshot {
        path,
        data: tokio::fs::File::from_std(std_file),
        created_at: Instant::now(),
        received: 0,
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

    // Offset continuity: chunks must land exactly at the end of what we have
    // accepted. A gap, rewind, or overlap would silently corrupt the
    // assembled snapshot file.
    if offset != streaming.received {
      return Err(Status::invalid_argument(format!(
        "Snapshot {} offset mismatch: expected {}, got {}",
        snapshot_id, streaming.received, offset
      )));
    }

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

    streaming.received += data.len() as u64;

    Ok(())
  }

  async fn install_snapshot(
    &self,
    vote: openraft::Vote<TypeConfig>,
    snapshot_meta: SnapshotMeta,
    mut streaming: StreamingSnapshot,
  ) -> Result<SnapshotResponse<TypeConfig>, Status> {
    prepare_for_install(&mut streaming).await?;

    let StreamingSnapshot { path, data, .. } = streaming;
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: data,
    };

    let res = self.node.raft().install_full_snapshot(vote, snapshot).await;

    // The assembled file has served its purpose either way: a failed install
    // is retried from offset 0, which recreates it.
    let _ = tokio::fs::remove_file(&path).await;

    res.map_err(|e| Status::internal(format!("InstallFullSnapshot failed: {}", e)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokio::io::AsyncReadExt;

  /// The receive path writes every chunk at its own offset, so the handle ends
  /// up at EOF, while `install_snapshot` reads from the current position:
  /// without the rewind the decoder sees an empty file and "recovers" nothing.
  #[tokio::test]
  async fn test_prepare_for_install_rewinds_the_streamed_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streamed_incomplete");

    // Same open options as `RocksStateMachine::create_snapshot_temp_file`:
    // the handle is written through and then read back by the installer.
    let std_file = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(&path)
      .unwrap();
    let mut file = tokio::fs::File::from_std(std_file);

    let payload: Vec<u8> = (0..=200u8).collect();
    let mut offset = 0u64;
    for chunk in payload.chunks(7) {
      file.seek(SeekFrom::Start(offset)).await.unwrap();
      file.write_all(chunk).await.unwrap();
      offset += chunk.len() as u64;
    }

    let mut streaming = StreamingSnapshot {
      path,
      data: file,
      created_at: Instant::now(),
      received: offset,
    };

    prepare_for_install(&mut streaming).await.unwrap();

    let mut got = Vec::new();
    streaming.data.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, payload, "the installer must read from offset 0");
  }

  /// A transfer whose last chunk never arrived must not be installed: the
  /// partial file would truncate the node's state to the bytes that did.
  #[tokio::test]
  async fn test_prepare_for_install_rejects_a_short_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_incomplete");

    let std_file = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(&path)
      .unwrap();
    let mut file = tokio::fs::File::from_std(std_file);
    file.write_all(b"0123456789").await.unwrap();

    let mut streaming = StreamingSnapshot {
      path,
      data: file,
      created_at: Instant::now(),
      received: 11,
    };

    let err = prepare_for_install(&mut streaming).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
  }
}
