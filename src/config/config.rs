use serde::Deserialize;
use serde::Serialize;

use super::default::default_raft_config;
use super::default::default_rocksdb_config;
use crate::error::Result;
use crate::error::StartupError;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Config {
  pub node_id: u64,

  #[serde(default = "default_raft_config")]
  pub raft: RaftConfig,

  #[serde(default = "default_rocksdb_config")]
  pub rocksdb: RocksdbConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RocksdbConfig {
  pub data_path: String,
  pub max_open_files: i32,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RaftConfig {
  pub addr: String,

  /// Single node raft cluster.
  pub single: bool,

  /// Bring up a raft node and join a cluster.
  ///
  /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
  pub join: Vec<String>,
}

impl Config {
  /// Validate the configuration to ensure it is correct.
  pub fn validate(&self) -> Result<()> {
    if self.raft.single && !self.raft.join.is_empty() {
      return Err(
        StartupError::invalid_config(
          "'single' mode cannot be used together with 'join' configuration",
        )
        .into(),
      );
    }
    Ok(())
  }
}
