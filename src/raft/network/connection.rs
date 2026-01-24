use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use std::sync::Arc;

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

    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    let grpc_request = tonic::Request::new(RaftRequest {
      data: self.serialize_buf.clone(),
    });

    let grpc_response = conn.forward(grpc_request).await?;

    let reply: RaftReply = grpc_response.into_inner();

    if !reply.error.is_empty() {
      let error: APIError = bincode::deserialize(&reply.error)?;
      Err(error.into())
    } else {
      let result: ForwardResponse = bincode::deserialize(&reply.data)?;
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

    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    let grpc_request = tonic::Request::new(AppendRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.append(grpc_request).await?;

    let append_reply = grpc_response.into_inner();

    let result: AppendEntriesResponse<TypeConfig> = bincode::deserialize(&append_reply.value)?;

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

    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    let grpc_request = tonic::Request::new(PbVoteRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.vote(grpc_request).await?;

    let vote_reply = grpc_response.into_inner();

    let result: VoteResponse<TypeConfig> = bincode::deserialize(&vote_reply.value)?;

    Ok(result)
  }

  /// Internal function to send InstallSnapshot RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn install_snapshot_internal(
    &mut self,
    req: InstallSnapshotRequest<TypeConfig>,
  ) -> Result<InstallSnapshotResponse<TypeConfig>> {
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    self.serialize_buf.clear();

    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    let grpc_request = tonic::Request::new(PbSnapshotRequest {
      value: self.serialize_buf.clone(),
    });

    let grpc_response = conn.snapshot(grpc_request).await?;

    let snapshot_reply = grpc_response.into_inner();

    let result: InstallSnapshotResponse<TypeConfig> = bincode::deserialize(&snapshot_reply.value)?;

    Ok(result)
  }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
  async fn append_entries(
    &mut self,
    rpc: AppendEntriesRequest<TypeConfig>,
    _option: RPCOption,
  ) -> std::result::Result<
    AppendEntriesResponse<TypeConfig>,
    RPCError<TypeConfig, RaftError<TypeConfig>>,
  > {
    self
      .append_entries_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn install_snapshot(
    &mut self,
    rpc: InstallSnapshotRequest<TypeConfig>,
    _option: RPCOption,
  ) -> std::result::Result<
    InstallSnapshotResponse<TypeConfig>,
    RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
  > {
    self
      .install_snapshot_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
  }

  async fn vote(
    &mut self,
    rpc: VoteRequest<TypeConfig>,
    _option: RPCOption,
  ) -> std::result::Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
  {
    self
      .vote_internal(rpc)
      .await
      .map_err(|e| RPCError::Network(NetworkError::new(&e)))
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
