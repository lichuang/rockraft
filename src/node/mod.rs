//! Core Raft node implementation and public API.
//!
//! This module provides the main interface for interacting with a Raft cluster:
//!
//! - [`RaftNode`]: The primary type for cluster operations (read, write, cluster management)
//! - [`RaftNodeBuilder`]: Fluent builder for constructing and initializing nodes
//! - [`LeaderHandler`]: Handle for executing leader-only operations
//!
//! # Example
//!
//! ```rust,no_run
//! use rockraft::node::RaftNodeBuilder;
//! use rockraft::config::Config;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # fn load_config() -> Config { panic!() }
//! let config: Config = load_config();
//! let node = RaftNodeBuilder::from_config(&config).await?;
//!
//! // Use the node for cluster operations
//! // node.write(entry).await?;
//! # Ok(())
//! # }
//! ```

mod cluster;
mod forward;
mod leader_handler;
#[allow(clippy::module_inception)]
mod node;
mod node_builder;

pub use leader_handler::LeaderHandler;
pub use node::RaftNode;
pub use node_builder::RaftNodeBuilder;
