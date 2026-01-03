use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use std::sync::Arc;

use crate::error::Result;
use crate::raft::grpc_client::ClientPool;
use crate::raft::protobuf::AppendRequest;
use crate::raft::protobuf::SnapshotRequest as PbSnapshotRequest;
use crate::raft::protobuf::VoteRequest as PbVoteRequest;
use crate::raft::types::RaftNode;
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

  /// Internal function to send AppendEntries RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn append_entries_internal(
    &mut self,
    req: AppendEntriesRequest<TypeConfig>,
  ) -> Result<AppendEntriesResponse<TypeConfig>> {
    // Get a gRPC client from the connection pool
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    // Clear and reuse the serialization buffer
    self.serialize_buf.clear();

    // Serialize the request using bincode
    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    // Create the gRPC request with serialized data
    let grpc_request = tonic::Request::new(AppendRequest {
      value: self.serialize_buf.clone(),
    });

    // Call the Append RPC from the RaftService
    let grpc_response = conn.append(grpc_request).await?;

    // Extract the response payload
    let append_reply = grpc_response.into_inner();

    // Deserialize the response
    let result: AppendEntriesResponse<TypeConfig> = bincode::deserialize(&append_reply.value)?;

    Ok(result)
  }

  /// Internal function to send Vote RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn vote_internal(
    &mut self,
    req: VoteRequest<TypeConfig>,
  ) -> Result<VoteResponse<TypeConfig>> {
    // Get a gRPC client from the connection pool
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    // Clear and reuse the serialization buffer
    self.serialize_buf.clear();

    // Serialize the request using bincode
    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    // Create the gRPC request with serialized data
    let grpc_request = tonic::Request::new(PbVoteRequest {
      value: self.serialize_buf.clone(),
    });

    // Call the Vote RPC from the RaftService
    let grpc_response = conn.vote(grpc_request).await?;

    // Extract the response payload
    let vote_reply = grpc_response.into_inner();

    // Deserialize the response
    let result: VoteResponse<TypeConfig> = bincode::deserialize(&vote_reply.value)?;

    Ok(result)
  }

  /// Internal function to send InstallSnapshot RPC to the target node.
  /// This function handles serialization of the request and deserialization of the response.
  async fn install_snapshot_internal(
    &mut self,
    req: InstallSnapshotRequest<TypeConfig>,
  ) -> Result<InstallSnapshotResponse<TypeConfig>> {
    // Get a gRPC client from the connection pool
    let mut conn = self.client_pool.raft_service_client(&self.addr).await?;

    // Clear and reuse the serialization buffer
    self.serialize_buf.clear();

    // Serialize the request using bincode
    bincode::serialize_into(&mut self.serialize_buf, &req)?;

    // Create the gRPC request with serialized data
    let grpc_request = tonic::Request::new(PbSnapshotRequest {
      value: self.serialize_buf.clone(),
    });

    // Call the Snapshot RPC from the RaftService
    let grpc_response = conn.snapshot(grpc_request).await?;

    // Extract the response payload
    let snapshot_reply = grpc_response.into_inner();

    // Deserialize the response
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
pub struct NetworkFactory {
  client_pool: Arc<ClientPool>,
}

impl NetworkFactory {
  pub fn new(client_pool: Arc<ClientPool>) -> Self {
    Self { client_pool }
  }
}

impl openraft::RaftNetworkFactory<TypeConfig> for NetworkFactory {
  type Network = NetworkConnection;

  async fn new_client(&mut self, _target: u64, node: &RaftNode) -> Self::Network {
    NetworkConnection::new(node.rpc_addr.clone(), self.client_pool.clone())
  }
}
