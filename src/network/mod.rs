//! Network layer — gRPC connection management and pooling.
//!
//! This module provides network connectivity for Raft cluster communication:
//! - Connection management for initial cluster join operations
//! - Connection pooling for inter-node Raft protocol communication
//! - DNS resolution and TLS support

pub(crate) mod connection;
pub(crate) mod pool;

// Re-export commonly used types
pub use connection::JoinConnectionFactory;
pub use pool::ClientPool;
