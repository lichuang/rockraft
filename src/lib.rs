//! # Rockraft
//!
//! A strongly consistent distributed key-value store library built on Raft consensus protocol and RocksDB.
//!
//! [![Crates.io](https://img.shields.io/crates/v/rockraft)](https://crates.io/crates/rockraft)
//! [![Docs.rs](https://docs.rs/rockraft/badge.svg)](https://docs.rs/rockraft)
//! [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/lichuang/rockraft/blob/main/LICENSE)
//!
//! ## Overview
//!
//! Rockraft is a Rust library that provides distributed consensus for data replication,
//! ensuring high availability and fault tolerance for distributed systems. It combines:
//!
//! - **OpenRaft** - A production-ready Raft consensus implementation
//! - **RocksDB** - High-performance embedded database
//! - **gRPC** - Efficient inter-node communication with connection pooling
//!
//! ## Features
//!
//! - ✅ **Strong Consistency** - All nodes maintain consistent state through Raft consensus
//! - ✅ **Fault Tolerance** - Automatic leader election and failover
//! - ✅ **High Performance** - RocksDB storage with efficient serialization (postcard)
//! - ✅ **Easy Setup** - Simple configuration and cluster initialization
//! - ✅ **Snapshot Support** - Efficient storage recovery and compaction
//! - ✅ **Connection Pooling** - Optimized gRPC connection management
//! - ✅ **Multi-Node Support** - Scale from single node to large clusters
//! - ✅ **Atomic Batch Writes** - Multiple key-value operations in a single transaction
//! - ✅ **Prefix Scanning** - Efficient range queries with prefix support
//!
//! ## Quick Start
//!
//! ```no_run
//! use rockraft::node::RaftNodeBuilder;
//! use rockraft::config::Config;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Load configuration
//!     let config: Config = load_config(); // Your config loading logic
//!
//!     // Create and start the Raft node
//!     let node = RaftNodeBuilder::from_config(&config).await?;
//!
//!     // Use the node (see examples for more details)
//!     // node.write(...).await?;
//!
//!     Ok(())
//! }
//!
//! fn load_config() -> Config {
//!     // Your config loading logic here
//!     todo!("Load your configuration")
//! }
//! ```
//!
//! ## Module Overview
//!
//! - [`config`] - Configuration structures for nodes and clusters
//! - [`node`] - Core Raft node implementation and builder
//! - [`raft`] - Raft types and storage backends (RocksDB state machine and log storage)
//! - [`network`] - Network layer for inter-node communication (connection management and pooling)
//! - [`service`] - High-level service abstractions
//! - [`engine`] - Query engine and command execution
//! - [`error`] - Error types and handling
//! - [`utils`] - Utility functions
//!
//! ## Cluster Management
//!
//! To create a multi-node cluster:
//!
//! 1. Start the first node as a single-node cluster
//! 2. Add additional nodes using the cluster management API:
//!
//! ```no_run
//! use rockraft::node::RaftNode;
//! use rockraft::raft::types::{JoinRequest, Endpoint};
//!
//! // Add a new node to the cluster
//! async fn add_node(node: &RaftNode) -> Result<(), Box<dyn std::error::Error>> {
//!     let req = JoinRequest {
//!         node_id: 2,
//!         endpoint: Endpoint::parse("127.0.0.1:50052")?,
//!     };
//!     node.add_node(req).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Batch Operations
//!
//! Perform atomic batch writes:
//!
//! ```no_run
//! use rockraft::raft::types::{BatchWriteReq, UpsertKV};
//!
//! let req = BatchWriteReq {
//!     entries: vec![
//!         UpsertKV::insert("key1", b"value1"),
//!         UpsertKV::insert("key2", b"value2"),
//!         UpsertKV::delete("old_key"),
//!     ],
//! };
//! // raft.batch_write(req).await?;
//! ```
//!
//! ## Examples
//!
//! See the [`example`](https://github.com/lichuang/rockraft/tree/main/example) directory
//! for a complete working cluster with HTTP API.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        Application                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
//! │  │   Service   │  │   Engine    │  │    Node Manager     │  │
//! │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      Raft Consensus                         │
//! │                     (via OpenRaft)                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────────┐  ┌─────────────────────────────┐   │
//! │  │  RocksStateMachine  │  │     RocksLogStorage         │   │
//! │  │  (Key-Value Store)  │  │  (Raft Log Persistence)     │   │
//! │  └─────────────────────┘  └─────────────────────────────┘   │
//! │                         RocksDB                             │
//! ├─────────────────────────────────────────────────────────────┤
//! │              gRPC Network (RaftService)                     │
//! │              with Connection Pooling                        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## License
//!
//! This project is licensed under the [Apache 2.0 License](https://github.com/lichuang/rockraft/blob/main/LICENSE).
//!
//! ## Acknowledgments
//!
//! - [OpenRaft](https://github.com/databendlabs/openraft) - Raft consensus implementation
//! - [RocksDB](https://github.com/facebook/rocksdb) - High-performance database

pub mod config;
pub mod engine;
pub mod error;
pub mod network;
pub mod node;
pub mod raft;
pub mod service;
pub mod utils;
