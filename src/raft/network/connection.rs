use crate::error::Result;
use std::sync::Arc;

use crate::raft::grpc_client::ClientPool;
use crate::raft::protobuf::AppendRequest;
use crate::raft::protobuf::VoteRequest as PbVoteRequest;
use crate::raft::types::TypeConfig;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

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
}
