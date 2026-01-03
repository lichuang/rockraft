use serde::Deserialize;
use serde::Serialize;

use super::default::default_raft_address;
use super::default::default_rocksdb_config;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Config {
  pub node_id: u64,

  #[serde(default = "default_raft_address")]
  pub raft_addr: String,

  #[serde(default = "default_rocksdb_config")]
  pub rocksdb: RocksdbConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RocksdbConfig {
  pub data_path: String,
  pub max_open_files: i32,
}
