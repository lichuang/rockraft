use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::Unreachable;
use openraft::errors::ReplicationClosed;
use openraft::errors::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::AppendResult;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::VoteOf;
use std::future::Future;
use std::io::Error;
use std::io::SeekFrom;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

use crate::error::Result;
use crate::network::ClientPool;
use crate::raft::protobuf::AppendReply;
use crate::raft::protobuf::AppendRequest;
use crate::raft::protobuf::SnapshotReply;
use crate::raft::protobuf::SnapshotRequest as PbSnapshotRequest;
use crate::raft::protobuf::VoteReply;
use crate::raft::protobuf::VoteRequest as PbVoteRequest;
use crate::raft::types::Node;
use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotChunk;
use crate::raft::types::SnapshotData;
use crate::raft::types::TypeConfig;
use crate::raft::types::decode;
use crate::raft::types::encode;

static SNAPSHOT_TRANSFER_SEQ: AtomicU64 = AtomicU64::new(0);

trait ReplyValue {
  fn value(&self) -> &[u8];
}

impl ReplyValue for AppendReply {
  fn value(&self) -> &[u8] {
    &self.value
  }
}

impl ReplyValue for VoteReply {
  fn value(&self) -> &[u8] {
    &self.value
  }
}

impl ReplyValue for SnapshotReply {
  fn value(&self) -> &[u8] {
    &self.value
  }
}

/// Represents a network connection to a specific Raft node.
///
/// This struct handles the actual RPC communication with a target node using gRPC.
pub struct NetworkConnection {
  addr: String,
  client_pool: Arc<ClientPool>,
  // Reusable serialization buffer to avoid per-call allocations
  serialize_buf: Vec<u8>,
}

impl NetworkConnection {
  pub fn new(addr: String, client_pool: Arc<ClientPool>) -> Self {
    Self {
      addr,
      client_pool,
      serialize_buf: Vec::new(),
    }
  }

  async fn send_rpc<Req, Resp, Reply, F, Fut>(&mut self, req: &Req, send: F) -> Result<Resp>
  where
    Req: serde::Serialize,
    Resp: serde::de::DeserializeOwned,
    Reply: ReplyValue,
    F: FnOnce(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = tonic::Result<tonic::Response<Reply>>>,
  {
    self.serialize_buf.clear();
    encode(req)
      .map(|data| self.serialize_buf.extend_from_slice(&data))
      .map_err(|e| crate::error::Error::internal(format!("encode failed: {}", e)))?;

    let reply = send(self.serialize_buf.clone()).await?.into_inner();
    decode(reply.value())
      .map_err(|e| crate::error::Error::internal(format!("decode failed: {}", e)))
  }

  async fn append_entries_internal(
    &mut self,
    req: AppendEntriesRequest<TypeConfig>,
  ) -> Result<AppendResult<TypeConfig>> {
    let prev_log_id = req.prev_log_id;
    let last_log_id = req.entries.last().map(|entry| entry.log_id).or(prev_log_id);

    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;
    let response: AppendEntriesResponse<TypeConfig> = self
      .send_rpc(&req, |data| {
        conn.append(tonic::Request::new(AppendRequest { value: data }))
      })
      .await?;

    Ok(response.into_append_result(prev_log_id, last_log_id))
  }

  async fn vote_internal(
    &mut self,
    req: VoteRequest<TypeConfig>,
  ) -> Result<VoteResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;
    self
      .send_rpc(&req, |data| {
        conn.vote(tonic::Request::new(PbVoteRequest { value: data }))
      })
      .await
  }

  async fn install_snapshot_internal(
    &mut self,
    chunk: SnapshotChunk,
  ) -> Result<SnapshotResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;
    self
      .send_rpc(&chunk, |data| {
        conn.snapshot(tonic::Request::new(PbSnapshotRequest { value: data }))
      })
      .await
  }

  /// Send a snapshot in chunks to the target node.
  /// This implements the full_snapshot functionality by splitting the snapshot into chunks
  /// and sending them via install_snapshot RPCs.
  async fn send_snapshot_in_chunks(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot,
    cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
    option: RPCOption,
  ) -> StdResult<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    use futures::FutureExt;

    // Pin the cancel future
    let mut cancel = std::pin::pin!(cancel);

    // Get snapshot size by seeking to end
    let mut snapshot_data = snapshot.snapshot;
    let end = snapshot_data
      .seek(SeekFrom::End(0))
      .await
      .map_err(|e| StreamingError::from(Unreachable::new(&e)))?;

    // Seek back to beginning
    snapshot_data
      .seek(SeekFrom::Start(0))
      .await
      .map_err(|e| StreamingError::from(Unreachable::new(&e)))?;

    let mut offset = 0u64;
    // Use default chunk size of 64KB if not specified
    let chunk_size = option.snapshot_chunk_size().unwrap_or(64 * 1024);

    // The receiver keys the assembled file by this id; the vote prefix keeps
    // transfers from different leaders apart.
    let transfer_id = format!(
      "{}-{}",
      vote.leader_id,
      SNAPSHOT_TRANSFER_SEQ.fetch_add(1, Ordering::Relaxed)
    );

    loop {
      // Check if cancelled
      if let Some(err) = cancel.as_mut().now_or_never() {
        return Err(err.into());
      }

      // Read chunk
      let mut buf = Vec::with_capacity(chunk_size);
      while buf.len() < chunk_size {
        let n = snapshot_data
          .read_buf(&mut buf)
          .await
          .map_err(|e| StreamingError::from(Unreachable::new(&e)))?;
        if n == 0 {
          break;
        }
      }

      let n_read = buf.len();
      if n_read == 0 && offset < end {
        let io_err = Error::other("snapshot file ended before its reported size");
        return Err(StreamingError::from(Unreachable::new(&io_err)));
      }
      let done = (offset + n_read as u64) >= end;

      let chunk = SnapshotChunk {
        vote,
        meta: snapshot.meta.clone(),
        transfer_id: transfer_id.clone(),
        offset,
        data: buf,
        done,
      };

      // Send chunk using internal method
      match self.install_snapshot_internal(chunk).await {
        Ok(resp) => {
          if resp.vote != vote {
            // Higher vote received
            return Ok(resp);
          }
          if done {
            return Ok(resp);
          }
        }
        Err(err) => {
          // Convert RockRaftError to RPCError
          let io_err = Error::other(err.to_string());
          return Err(StreamingError::from(Unreachable::new(&io_err)));
        }
      }

      offset += n_read as u64;
    }
  }
}

impl RaftNetworkV2<TypeConfig> for NetworkConnection {
  type SnapshotData = SnapshotData;

  async fn append_entries(
    &mut self,
    rpc: AppendEntriesRequest<TypeConfig>,
    _option: RPCOption,
  ) -> StdResult<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
    self
      .append_entries_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn vote(
    &mut self,
    rpc: VoteRequest<TypeConfig>,
    _option: RPCOption,
  ) -> StdResult<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
    self
      .vote_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn full_snapshot(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot,
    cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
    option: RPCOption,
  ) -> StdResult<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    // Send snapshot in chunks using our internal method
    self
      .send_snapshot_in_chunks(vote, snapshot, cancel, option)
      .await
  }
}

/// Factory for creating network connections to Raft cluster members.
///
/// This struct implements the [`RaftNetworkFactory`] trait required by OpenRaft
/// to create [`NetworkConnection`] instances for communicating with other nodes.
#[derive(Clone)]
pub struct NetworkFactory {
  client_pool: Arc<ClientPool>,
}

impl NetworkFactory {
  pub fn new(client_pool: Arc<ClientPool>) -> Self {
    Self { client_pool }
  }

  pub fn new_client_with_addr(&self, addr: String) -> NetworkConnection {
    NetworkConnection::new(addr, self.client_pool.clone())
  }
}

impl openraft::RaftNetworkFactory<TypeConfig> for NetworkFactory {
  type Network = NetworkConnection;

  async fn new_client(&mut self, _target: u64, node: &Node) -> Self::Network {
    self.new_client_with_addr(node.endpoint.to_string())
  }
}
