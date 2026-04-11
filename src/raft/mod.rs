//! Raft consensus implementation.
//!
//! This module integrates OpenRaft with RocksDB storage and gRPC networking:
//! - [`store`]: Persistent log and state machine storage
//! - [`network`]: Network transport for Raft RPCs
//! - [`types`]: Core data types and messages
//! - [`protobuf`]: Generated gRPC service definitions

#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("raftpb");
}

pub mod network;
pub mod store;
pub mod types;
