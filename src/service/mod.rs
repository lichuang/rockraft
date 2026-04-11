//! gRPC service implementation for Raft protocol.
//!
//! Provides the server-side implementation of the Raft service:
//! - [`RaftServiceImpl`]: Handles incoming gRPC requests from other nodes
//! - Request forwarding between nodes
//! - Snapshot streaming support

mod raft_service_impl;

pub use raft_service_impl::RaftServiceImpl;
