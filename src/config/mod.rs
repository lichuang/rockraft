//! Configuration management for RockRaft nodes.
//!
//! Provides configuration structures and parsing for:
//! - Node identification and network endpoints
//! - Raft consensus parameters
//! - RocksDB storage settings
//!
//! # Example
//!
//! ```rust
//! use rockraft::config::{Config, RaftConfig, RocksdbConfig, Endpoint};
//!
//! let config = Config {
//!     node_id: 1,
//!     raft: RaftConfig {
//!         endpoint: Endpoint::parse("127.0.0.1:5001").unwrap(),
//!         advertise_endpoint: Endpoint::parse("127.0.0.1:5001").unwrap(),
//!         join: vec![],
//!     },
//!     rocksdb: RocksdbConfig {
//!         data_path: "/tmp/raft".to_string(),
//!         max_open_files: 1024,
//!     },
//! };
//! ```

#[allow(clippy::module_inception)]
mod config;
mod default;
mod endpoint;

pub use config::Config;
pub use config::RaftConfig;
pub use config::RocksdbConfig;
pub use endpoint::Endpoint;
