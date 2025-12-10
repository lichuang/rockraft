use super::config::RocksdbConfig;

pub fn default_raft_address() -> String {
  "127.0.0.1:6682".to_string()
}

pub fn default_rocksdb_config() -> RocksdbConfig {
  RocksdbConfig {
    max_open_files: 10000,
    data_path: ".rocksraft_data".to_string(),
  }
}
