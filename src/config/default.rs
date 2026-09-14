use super::config::RawRaftConfig;
use super::config::RocksdbConfig;

/// Default maximum gRPC message size (encode and decode) for raft RPCs.
///
/// Tonic's default decode limit (4MB) is far below what AppendEntries can
/// legitimately carry (openraft batches up to `max_payload_entries = 300`
/// entries per RPC), which would permanently break replication for large
/// payloads. 256MB leaves ample headroom while still bounding memory use.
pub const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

pub(crate) fn default_raft_config() -> RawRaftConfig {
  RawRaftConfig {
    address: "127.0.0.1:6682".to_string(),
    advertise_host: "localhost".to_string(),
    join: vec![],
    heartbeat_interval: None,
    election_timeout_min: None,
    election_timeout_max: None,
    grpc_max_message_size: None,
  }
}

pub(crate) fn default_rocksdb_config() -> RocksdbConfig {
  RocksdbConfig {
    max_open_files: 10000,
    data_path: ".rockraft_data".to_string(),
  }
}
