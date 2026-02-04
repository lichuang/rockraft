use super::config::{RaftConfig, RocksdbConfig};

pub fn default_raft_config() -> RaftConfig {
  RaftConfig {
    addr: "127.0.0.1:6682".to_string(),
    single: true,
    join: vec![],
  }
}

pub fn default_rocksdb_config() -> RocksdbConfig {
  RocksdbConfig {
    max_open_files: 10000,
    data_path: ".rockraft_data".to_string(),
  }
}
