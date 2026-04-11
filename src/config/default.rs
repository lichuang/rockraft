use super::config::RawRaftConfig;
use super::config::RocksdbConfig;

pub(crate) fn default_raft_config() -> RawRaftConfig {
  RawRaftConfig {
    address: "127.0.0.1:6682".to_string(),
    advertise_host: "localhost".to_string(),
    single: true,
    join: vec![],
  }
}

pub(crate) fn default_rocksdb_config() -> RocksdbConfig {
  RocksdbConfig {
    max_open_files: 10000,
    data_path: ".rockraft_data".to_string(),
  }
}
