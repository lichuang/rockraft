use openraft::error::NetworkError;
use openraft::error::RPCError;

use openraft::errors::ReplicationClosed;
use openraft::errors::StreamingError;
use openraft::network::RPCOption;
use openraft::network::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::Snapshot;
use openraft::type_config::alias::VoteOf;
use std::future::Future;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

use crate::error::APIError;
use crate::error::Result;
use crate::raft::grpc_client::ClientPool;
use crate::raft::protobuf::AppendRequest;
use crate::raft::protobuf::RaftReply;
use crate::raft::protobuf::RaftRequest;
use crate::raft::protobuf::SnapshotRequest as PbSnapshotRequest;
use crate::raft::protobuf::VoteRequest as PbVoteRequest;
use crate::raft::types::ForwardRequest;
use crate::raft::types::ForwardResponse;
use crate::raft::types::Node;
use crate::raft::types::TypeConfig;
use crate::raft::types::decode;
use crate::raft::types::encode;

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

  pub async fn forward(&mut self, req: ForwardRequest) -> Result<ForwardResponse> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    self.serialize_buf.clear();

    encode(&req)
      .map(|data| self.serialize_buf.extend_from_slice(&data))
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let grpc_request = tonic::Request::new(RaftRequest {
      data: self.serialize_buf.clone(),
    });

    let grpc_response = conn.forward(grpc_request).await?;

    let reply: RaftReply = grpc_response.into_inner();

    if !reply.error.is_empty() {
      let error: APIError = decode(&reply.error)?;
      Err(error.into())
    } else {
      let result: ForwardResponse = decode(&reply.data)?;
      Ok(result)
    }
  }

  /// Internal function to send AppendEntries RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn append_entries_internal(
    &mut self,
    req: AppendEntriesRequest<TypeConfig>,
  ) -> Result<AppendEntriesResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    self.serialize_buf.clear();

    encode(&req)
      .map(|data| self.serialize_buf.extend_from_slice(&data))
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let grpc_request = tonic::Request::new(AppendRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.append(grpc_request).await?;

    let append_reply = grpc_response.into_inner();

    let result: AppendEntriesResponse<TypeConfig> = decode(&append_reply.value)?;

    Ok(result)
  }

  /// Internal function to send Vote RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn vote_internal(
    &mut self,
    req: VoteRequest<TypeConfig>,
  ) -> Result<VoteResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    self.serialize_buf.clear();

    encode(&req)
      .map(|data| self.serialize_buf.extend_from_slice(&data))
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let grpc_request = tonic::Request::new(PbVoteRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.vote(grpc_request).await?;

    let vote_reply = grpc_response.into_inner();

    let result: VoteResponse<TypeConfig> = decode(&vote_reply.value)?;

    Ok(result)
  }

  /// Internal function to send InstallSnapshot RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn install_snapshot_internal(
    &mut self,
    req: InstallSnapshotRequest<TypeConfig>,
  ) -> Result<SnapshotResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    self.serialize_buf.clear();

    encode(&req)
      .map(|data| self.serialize_buf.extend_from_slice(&data))
      .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let grpc_request = tonic::Request::new(PbSnapshotRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.snapshot(grpc_request).await?;

    let snapshot_reply = grpc_response.into_inner();

    let result: SnapshotResponse<TypeConfig> = decode(&snapshot_reply.value)?;

    Ok(result)
  }

  /// Send a snapshot in chunks to the target node.
  /// This implements the full_snapshot functionality by splitting the snapshot into chunks
  /// and sending them via install_snapshot RPCs.
  async fn send_snapshot_in_chunks(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot<TypeConfig>,
    cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
    option: RPCOption,
  ) -> std::result::Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
    use futures::FutureExt;

    // Pin the cancel future
    let mut cancel = std::pin::pin!(cancel);

    // Get snapshot size by seeking to end
    let mut snapshot_data = snapshot.snapshot;
    let end = snapshot_data
      .seek(SeekFrom::End(0))
      .await
      .map_err(|e| StreamingError::from(openraft::error::Unreachable::new(&e)))?;

    // Seek back to beginning
    snapshot_data
      .seek(SeekFrom::Start(0))
      .await
      .map_err(|e| StreamingError::from(openraft::error::Unreachable::new(&e)))?;

    let mut offset = 0u64;
    // Use default chunk size of 64KB if not specified
    let chunk_size = option.snapshot_chunk_size().unwrap_or(64 * 1024);

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
          .map_err(|e| StreamingError::from(openraft::error::Unreachable::new(&e)))?;
        if n == 0 {
          break;
        }
      }

      let n_read = buf.len();
      let done = (offset + n_read as u64) >= end;

      let req = InstallSnapshotRequest {
        vote: vote.clone(),
        meta: snapshot.meta.clone(),
        offset,
        data: buf,
        done,
      };

      // Send chunk using internal method
      match self.install_snapshot_internal(req).await {
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
          let io_err = std::io::Error::new(std::io::ErrorKind::Other, err.to_string());
          return Err(StreamingError::from(openraft::error::Unreachable::new(
            &io_err,
          )));
        }
      }

      offset += n_read as u64;
    }
  }
}

impl RaftNetworkV2<TypeConfig> for NetworkConnection {
  async fn append_entries(
    &mut self,
    rpc: AppendEntriesRequest<TypeConfig>,
    _option: RPCOption,
  ) -> std::result::Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
    self
      .append_entries_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn vote(
    &mut self,
    rpc: VoteRequest<TypeConfig>,
    _option: RPCOption,
  ) -> std::result::Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
    self
      .vote_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn full_snapshot(
    &mut self,
    vote: VoteOf<TypeConfig>,
    snapshot: Snapshot<TypeConfig>,
    cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
    option: RPCOption,
  ) -> std::result::Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
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
