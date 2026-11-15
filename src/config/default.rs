use super::config::RawRaftConfig;
use super::config::RocksdbConfig;
use super::read_consistency::ReadConsistency;

/// Default maximum gRPC message size (encode and decode) for raft RPCs.
///
/// Tonic's default decode limit (4MB) is far below what AppendEntries can
/// legitimately carry (a batch of `max_payload_entries` entries), which would
/// permanently break replication for large payloads. 256MB leaves ample
/// headroom while still bounding memory use.
pub const DEFAULT_GRPC_MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Default number of entries OpenRaft may put into one `AppendEntries` RPC.
///
/// OpenRaft's own default is 300. Nothing bounds the size of an entry except
/// what a user stores, so 300 entries can exceed `grpc_max_message_size`; the
/// follower then fails to decode the batch and replication retries the same
/// oversized payload forever. 50 entries leaves room for ~5MB entries under the
/// 256MB default cap. Clusters whose entries are large should lower it, and
/// those that store small entries can raise it.
pub const DEFAULT_MAX_PAYLOAD_ENTRIES: u64 = 50;

pub(crate) fn default_raft_config() -> RawRaftConfig {
  RawRaftConfig {
    address: "127.0.0.1:6682".to_string(),
    advertise_host: "localhost".to_string(),
    join: vec![],
    heartbeat_interval: None,
    election_timeout_min: None,
    election_timeout_max: None,
    grpc_max_message_size: None,
    max_payload_entries: None,
    read_consistency: ReadConsistency::default(),
  }
}

pub(crate) fn default_rocksdb_config() -> RocksdbConfig {
  RocksdbConfig {
    max_open_files: 10000,
    data_path: ".rockraft_data".to_string(),
    block_cache_size: None,
    write_buffer_size: None,
    max_write_buffer_number: None,
  }
}
