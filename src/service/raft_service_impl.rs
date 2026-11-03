use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
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
use tracing::info;
use tracing::warn;

use crate::error::{ApiError, Error};
use crate::raft::protobuf as pb;

use crate::node::RaftNode;
use crate::raft::types::{
  ForwardRequest, ForwardResponse, Snapshot, SnapshotMeta, TypeConfig, decode, encode,
};
use openraft::async_runtime::watch::WatchReceiver;
use pb::raft_service_server::RaftService;

/// A streaming snapshot is evicted after this long without a new chunk.
///
/// This is an *idle* timeout, not a cap on the total transfer time: a large
/// snapshot on a slow link keeps refreshing it, so it is never interrupted
/// mid-transfer, while a dead transfer is reclaimed quickly.
const SNAPSHOT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How often idle transfers are swept, independently of incoming requests.
///
/// Eviction used to run only when a snapshot chunk arrived, so a transfer the
/// leader abandoned held its partial file until the process restarted.
const SNAPSHOT_REAP_INTERVAL: Duration = Duration::from_secs(60);

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
  /// Last time a chunk was accepted; drives idle eviction.
  last_active_at: Instant,
  /// Total bytes accepted so far; equals the next expected offset.
  received: u64,
}

impl StreamingSnapshot {
  /// Whether this transfer has been idle long enough to be evicted.
  fn is_stale(&self) -> bool {
    self.last_active_at.elapsed() >= SNAPSHOT_IDLE_TIMEOUT
  }

  /// Append one chunk at its stream offset.
  ///
  /// Chunks must land exactly at the end of what has already been accepted: a
  /// gap, rewind, or overlap would silently corrupt the assembled file.
  async fn write_chunk(&mut self, offset: u64, data: &[u8]) -> Result<(), Status> {
    if offset != self.received {
      return Err(Status::invalid_argument(format!(
        "Snapshot {} offset mismatch: expected {}, got {}",
        self.path.display(),
        self.received,
        offset
      )));
    }

    self
      .data
      .seek(SeekFrom::Start(offset))
      .await
      .map_internal("Failed to seek")?;

    self
      .data
      .write_all(data)
      .await
      .map_internal("Failed to write snapshot data")?;

    self.received += data.len() as u64;
    // The idle clock only advances on progress, so a slow but live transfer
    // is never mistaken for an abandoned one.
    self.last_active_at = Instant::now();

    Ok(())
  }

  /// Turn a fully received snapshot file into something `install_snapshot` can
  /// read.
  ///
  /// The chunks were written at explicit offsets, so the handle's cursor sits
  /// at the end of the file while `install_snapshot` reads from the current
  /// position: rewinding is what makes the data visible to the zstd decoder.
  /// The length check catches a transfer whose last chunk was lost or replayed.
  ///
  /// Consumes `self` and deletes the assembled file if preparation fails: the
  /// receive entry is already gone from `streaming_snapshots`, so no other
  /// cleanup path can see it.
  async fn into_installable(mut self) -> Result<(PathBuf, tokio::fs::File), Status> {
    let prepared: Result<(), Status> = async {
      self
        .data
        .flush()
        .await
        .map_internal("Failed to flush snapshot data")?;

      let len = self
        .data
        .metadata()
        .await
        .map_internal("Failed to stat snapshot file")?
        .len();

      if len != self.received {
        return Err(Status::invalid_argument(format!(
          "Snapshot file holds {} bytes but {} were streamed",
          len, self.received
        )));
      }

      self
        .data
        .seek(SeekFrom::Start(0))
        .await
        .map_internal("Failed to rewind snapshot file")?;

      Ok(())
    }
    .await;

    let StreamingSnapshot { path, data, .. } = self;

    if let Err(status) = prepared {
      // Close the handle first: this path is the same one the abandon/install
      // cleanup takes, and a platform that refuses to unlink an open file
      // would otherwise keep it around.
      drop(data);
      remove_snapshot_file(
        &path,
        &format!("preparing it for install failed: {}", status),
      )
      .await;
      return Err(status);
    }

    Ok((path, data))
  }
}

/// Deletes the file assembled for an incoming snapshot when it goes out of
/// scope, however the install ends -- including a cancelled request.
///
/// Once the entry leaves `streaming_snapshots`, nothing else owns the file: a
/// dropped `install_full_snapshot` future used to leave it until the next
/// restart. Unlinking while openraft still reads the file is safe, because
/// openraft owns its own handle: POSIX keeps the data alive for it, and a
/// platform that refuses the delete only logs and falls back to the startup
/// sweep.
struct TempSnapshotFile {
  path: PathBuf,
  reason: &'static str,
}

impl Drop for TempSnapshotFile {
  fn drop(&mut self) {
    match std::fs::remove_file(&self.path) {
      Ok(()) => info!(
        "Removed snapshot file '{}': {}",
        self.path.display(),
        self.reason
      ),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      Err(e) => warn!(
        "Failed to remove snapshot file '{}' ({}): {}",
        self.path.display(),
        self.reason,
        e
      ),
    }
  }
}

/// Delete a snapshot-related file, logging why it is no longer needed.
async fn remove_snapshot_file(path: &Path, reason: &str) {
  match tokio::fs::remove_file(path).await {
    Ok(()) => info!("Removed snapshot file '{}': {}", path.display(), reason),
    Err(e) => warn!(
      "Failed to remove snapshot file '{}' (reason: {}): {}",
      path.display(),
      reason,
      e
    ),
  }
}

/// Drop stale in-flight transfers from the map and delete their partial files.
///
/// Expired entries are removed from the map first and only then unlinked, so
/// their open handles are gone before the delete — the unlink also works on
/// platforms that refuse to delete a file that is still open.
async fn evict_stale(snapshots: &mut HashMap<String, StreamingSnapshot>) {
  let mut expired = Vec::new();
  snapshots.retain(|_, s| {
    let stale = s.is_stale();
    if stale {
      expired.push(s.path.clone());
    }
    !stale
  });

  for path in expired {
    remove_snapshot_file(
      &path,
      &format!(
        "no chunk received for {}s, so the streaming transfer is considered abandoned",
        SNAPSHOT_IDLE_TIMEOUT.as_secs()
      ),
    )
    .await;
  }
}

pub struct RaftServiceImpl {
  node: Arc<RaftNode>,
  /// Store ongoing snapshot streaming operations
  streaming_snapshots: Arc<Mutex<HashMap<String, StreamingSnapshot>>>,
}

impl RaftServiceImpl {
  pub fn new(node: Arc<RaftNode>) -> Self {
    let streaming_snapshots = Arc::new(Mutex::new(HashMap::new()));

    // Sweep abandoned transfers on a timer: waiting for the next snapshot
    // chunk means a transfer the leader gave up on is only reclaimed when some
    // other snapshot arrives, which can be never. The reaper holds a `Weak`,
    // so it exits by itself once the service is dropped.
    let weak = Arc::downgrade(&streaming_snapshots);
    tokio::spawn(async move {
      let mut tick = tokio::time::interval(SNAPSHOT_REAP_INTERVAL);
      loop {
        tick.tick().await;
        let Some(snapshots) = weak.upgrade() else {
          return;
        };
        let mut guard = snapshots.lock().await;
        evict_stale(&mut guard).await;
      }
    });

    Self {
      node,
      streaming_snapshots,
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
    let data = encode(&api_err).unwrap_or_else(|e| {
      // Serializing a small enum of strings and an `Option<u64>` does not fail
      // in practice. If it ever did, the reply still has to stay in the wire
      // format every client decodes -- a human-readable string here would only
      // trade the original error for "Failed to deserialize error response".
      tracing::error!("Failed to serialize ApiError: {}", e);
      encode(&ApiError::Internal("internal error".to_string())).unwrap_or_default()
    });

    pb::RaftReply {
      data: Vec::new(),
      error: data,
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
    let response = self
      .node
      .handle_forward_request(forward_req, req.hops)
      .await;
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
      evict_stale(&mut guard).await;

      self
        .ensure_streaming_entry(&mut guard, &snapshot_id, offset)
        .await?;

      guard
        .get_mut(&snapshot_id)
        .ok_or_else(|| Status::internal("Streaming snapshot not found"))?
        .write_chunk(offset, &snapshot_req.data)
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
        last_active_at: Instant::now(),
        received: 0,
      },
    );

    Ok(())
  }

  async fn install_snapshot(
    &self,
    vote: openraft::Vote<TypeConfig>,
    snapshot_meta: SnapshotMeta,
    streaming: StreamingSnapshot,
  ) -> Result<SnapshotResponse<TypeConfig>, Status> {
    let (path, data) = streaming.into_installable().await?;

    // Owns the file for the rest of the call, so a cancelled request cleans up
    // too. The reason is refined once the outcome is known.
    let mut cleanup = TempSnapshotFile {
      path,
      reason: "the install attempt is over",
    };

    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: data,
    };

    let res = self.node.raft().install_full_snapshot(vote, snapshot).await;

    cleanup.reason = if res.is_ok() {
      "the install attempt succeeded"
    } else {
      "the install attempt failed and will be retried from offset 0"
    };

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
  ///
  /// On success the file is handed to the caller, which deletes it once the
  /// install attempt is over.
  #[tokio::test]
  async fn test_into_installable_rewinds_and_keeps_the_file_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streamed_incomplete");

    let mut file = create_rw(&path);

    let payload: Vec<u8> = (0..=200u8).collect();
    let mut offset = 0u64;
    for chunk in payload.chunks(7) {
      file.seek(SeekFrom::Start(offset)).await.unwrap();
      file.write_all(chunk).await.unwrap();
      offset += chunk.len() as u64;
    }

    let streaming = StreamingSnapshot {
      path: path.clone(),
      data: file,
      last_active_at: Instant::now(),
      received: offset,
    };

    let (kept_path, mut data) = streaming.into_installable().await.unwrap();
    assert_eq!(kept_path, path);
    assert!(kept_path.exists(), "the caller still owns the file");

    let mut got = Vec::new();
    data.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, payload, "the installer must read from offset 0");

    drop(data);
    std::fs::remove_file(&kept_path).unwrap();
  }

  /// A transfer whose last chunk never arrived must not be installed: the
  /// partial file would truncate the node's state to the bytes that did. The
  /// failed transfer must also delete its own file — its entry is already out
  /// of the map, so nothing else can.
  #[tokio::test]
  async fn test_into_installable_rejects_a_short_file_and_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_incomplete");

    let mut file = create_rw(&path);
    file.write_all(b"0123456789").await.unwrap();

    let streaming = StreamingSnapshot {
      path: path.clone(),
      data: file,
      last_active_at: Instant::now(),
      received: 11,
    };

    let err = streaming.into_installable().await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
      !path.exists(),
      "a rejected transfer must not leave its file behind"
    );
  }

  /// The guard deletes the assembled file however the install ends, including
  /// a cancelled request — the case that used to leave it behind until the
  /// next restart.
  #[test]
  fn test_temp_snapshot_file_guard_removes_the_file_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cancelled_incomplete");
    std::fs::write(&path, b"partial").unwrap();

    {
      let _guard = TempSnapshotFile {
        path: path.clone(),
        reason: "test",
      };
      assert!(path.exists(), "the guard must not delete while it is held");
    }

    assert!(!path.exists(), "dropping the guard must delete the file");

    // A file that is already gone must not panic the drop.
    let _guard = TempSnapshotFile {
      path,
      reason: "test",
    };
  }

  /// Open a file the same way the receive path does: write the chunks through
  /// it and read the result back.
  fn create_rw(path: &std::path::Path) -> tokio::fs::File {
    let std_file = std::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(path)
      .unwrap();
    tokio::fs::File::from_std(std_file)
  }

  /// Serialize KV entries in the on-disk snapshot format:
  /// `[key_len][key][value_len][value]`, wrapped in one zstd frame.
  fn zstd_entries(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    use std::io::Write as _;

    let mut out = Vec::new();
    {
      let mut encoder = zstd::Encoder::new(&mut out, 3).unwrap();
      for (key, value) in entries {
        encoder
          .write_all(&(key.len() as u32).to_le_bytes())
          .unwrap();
        encoder.write_all(key).unwrap();
        encoder
          .write_all(&(value.len() as u32).to_le_bytes())
          .unwrap();
        encoder.write_all(value).unwrap();
      }
      encoder.finish().unwrap();
    }
    out
  }

  /// Expired in-flight transfers are dropped both from the map and from disk;
  /// live ones keep their files.
  #[tokio::test]
  async fn test_evict_stale_removes_expired_files_only() {
    let dir = tempfile::tempdir().unwrap();
    let stale_path = dir.path().join("stale_incomplete");
    let fresh_path = dir.path().join("fresh_incomplete");
    std::fs::write(&stale_path, b"stale").unwrap();
    std::fs::write(&fresh_path, b"fresh").unwrap();

    let mut snapshots = HashMap::new();
    snapshots.insert(
      "stale".to_string(),
      StreamingSnapshot {
        path: stale_path.clone(),
        data: create_rw(&stale_path),
        last_active_at: Instant::now() - SNAPSHOT_IDLE_TIMEOUT - Duration::from_secs(1),
        received: 5,
      },
    );
    snapshots.insert(
      "fresh".to_string(),
      StreamingSnapshot {
        path: fresh_path.clone(),
        data: create_rw(&fresh_path),
        last_active_at: Instant::now(),
        received: 5,
      },
    );

    evict_stale(&mut snapshots).await;

    assert!(!snapshots.contains_key("stale"));
    assert!(snapshots.contains_key("fresh"));
    assert!(
      !stale_path.exists(),
      "an expired transfer's file must be deleted"
    );
    assert!(fresh_path.exists(), "a live transfer's file must be kept");

    drop(snapshots);
    std::fs::remove_file(&fresh_path).unwrap();
  }

  /// Progress resets the idle clock: a slow transfer that keeps delivering
  /// chunks must never be evicted, no matter how long it has been running.
  #[tokio::test]
  async fn test_write_chunk_refreshes_the_idle_clock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slow_incomplete");
    let file = create_rw(&path);

    let mut streaming = StreamingSnapshot {
      path: path.clone(),
      data: file,
      last_active_at: Instant::now() - SNAPSHOT_IDLE_TIMEOUT - Duration::from_secs(1),
      received: 0,
    };
    assert!(streaming.is_stale(), "precondition: idle past the limit");

    streaming.write_chunk(0, b"chunk").await.unwrap();

    assert!(
      !streaming.is_stale(),
      "a transfer that made progress must not be treated as abandoned"
    );

    drop(streaming);
    std::fs::remove_file(&path).unwrap();
  }

  fn test_config(data_dir: &std::path::Path) -> crate::config::Config {
    use crate::config::Config;
    use crate::config::RaftConfig;
    use crate::config::ReadConsistency;
    use crate::config::RocksdbConfig;
    use crate::raft::types::Endpoint;

    let endpoint = Endpoint::new("127.0.0.1", 12345);
    Config {
      node_id: 1,
      raft: RaftConfig {
        endpoint: endpoint.clone(),
        advertise_endpoint: endpoint,
        join: vec![],
        heartbeat_interval: None,
        // Long enough that the node cannot campaign while the test runs: a
        // second campaign would advance its vote past the one the install
        // presents, and the install would be rejected.
        election_timeout_min: Some(10_000),
        election_timeout_max: Some(20_000),
        grpc_max_message_size: None,
        read_consistency: ReadConsistency::default(),
      },
      rocksdb: RocksdbConfig {
        data_path: data_dir.to_string_lossy().to_string(),
        max_open_files: 1024,
        block_cache_size: None,
        write_buffer_size: None,
        max_write_buffer_number: None,
      },
    }
  }

  /// End-to-end: a file assembled the way `write_chunk` does (cursor at EOF)
  /// must be rewound, installed, and removed by `install_snapshot`. This is the
  /// test that fails if `into_installable` stops rewinding the handle.
  #[tokio::test]
  async fn test_install_snapshot_installs_and_removes_the_streamed_file() {
    use crate::node::CreateOptions;
    use crate::raft::types::StoredMembership;
    use crate::utils::test::create_log_id;

    let temp = tempfile::tempdir().unwrap();
    let config = test_config(temp.path());
    let node = RaftNode::create(&config, &CreateOptions::default())
      .await
      .unwrap();
    let service = RaftServiceImpl::new(node.clone());

    let payload = zstd_entries(&[(b"key", b"value")]);

    let path = node
      .state_machine()
      .snapshot_temp_file_path("test-snapshot");
    let std_file = node
      .state_machine()
      .create_snapshot_temp_file("test-snapshot")
      .unwrap();
    let mut file = tokio::fs::File::from_std(std_file);
    let mut offset = 0u64;
    for chunk in payload.chunks(5) {
      file.seek(SeekFrom::Start(offset)).await.unwrap();
      file.write_all(chunk).await.unwrap();
      offset += chunk.len() as u64;
    }

    let streaming = StreamingSnapshot {
      path: path.clone(),
      data: file,
      last_active_at: Instant::now(),
      received: offset,
    };
    let meta = SnapshotMeta {
      last_log_id: Some(create_log_id(1, 1, 1)),
      last_membership: StoredMembership::default(),
      snapshot_id: "test-snapshot".to_string(),
    };

    service
      .install_snapshot(openraft::Vote::new_committed(1, 1), meta, streaming)
      .await
      .expect("install must succeed");

    assert_eq!(
      node.state_machine().get_kv("key").unwrap(),
      Some(b"value".to_vec()),
      "the installer must read the streamed file from offset 0"
    );
    assert!(!path.exists(), "the assembled file must be removed");

    node.shutdown().await.unwrap();
  }

  /// `RaftReply.error` carries a postcard-encoded `ApiError`; every client
  /// decodes it that way, so the server must never put anything else in there.
  #[test]
  fn test_error_reply_is_a_decodable_api_error() {
    let reply = RaftServiceImpl::error_reply(&Error::internal("boom"));
    assert!(reply.data.is_empty());

    let decoded: ApiError = decode(&reply.error).expect("error must decode as ApiError");
    assert_eq!(
      decoded,
      ApiError::Internal("internal error: boom".to_string())
    );
  }

  /// A redirect keeps its leader id across the wire, which is what lets a
  /// client forward the request instead of failing it.
  #[test]
  fn test_error_reply_keeps_the_redirect_target() {
    let reply = RaftServiceImpl::error_reply(&Error::forward_to_leader(Some(7)));

    let decoded: ApiError = decode(&reply.error).expect("error must decode as ApiError");
    assert_eq!(decoded, ApiError::ForwardToLeader { leader_id: Some(7) });
    assert_eq!(Error::from(decoded).forward_leader_id(), Some(7));
  }
}
