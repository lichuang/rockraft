//! Raft network layer implementation.
//!
//! Implements the network communication between Raft cluster nodes:
//! - [`NetworkConnection`]: Handles RPC calls to specific nodes
//! - [`NetworkFactory`]: Creates connections for the Raft protocol
//!
//! This module integrates with OpenRaft's network traits to provide
//! gRPC-based communication for AppendEntries, Vote, and Snapshot RPCs.

mod connection;

pub use connection::NetworkConnection;
pub use connection::NetworkFactory;
