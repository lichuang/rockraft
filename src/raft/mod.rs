//! Raft consensus implementation.
//!
//! This module integrates OpenRaft with RocksDB storage and gRPC networking:
//! - [`store`]: Persistent log and state machine storage
//! - [`network`]: Network transport for Raft RPCs
//! - [`types`]: Core data types and messages
//! - [`protobuf`]: Generated gRPC service definitions

#[allow(clippy::all, dead_code)]
pub(crate) mod protobuf {
  tonic::include_proto!("raftpb");
}

pub(crate) mod network;
pub(crate) mod store;
pub mod types;

// `RaftNode::state_machine()` returns this type, so it has to be nameable from
// outside the crate even though the rest of `store` stays internal.
pub use store::RocksStateMachine;
