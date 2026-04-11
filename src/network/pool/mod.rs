//! Connection pooling for Raft inter-node communication.
//!
//! Provides a connection pool using `mobc` for efficient reuse of
//! gRPC connections between Raft cluster nodes.

mod client_pool;
mod manager;

pub use client_pool::ClientPool;
pub use manager::RaftServiceManager;
