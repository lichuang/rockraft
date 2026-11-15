use serde::Deserialize;
use serde::Serialize;

use super::default::DEFAULT_MAX_PAYLOAD_ENTRIES;
use super::default::default_raft_config;
use super::default::default_rocksdb_config;
use super::endpoint::Endpoint;
use super::read_consistency::ReadConsistency;
use crate::error::Error;
use crate::error::Result;
use openraft::Config as OpenRaftConfig;

/// Node configuration with all fields parsed.
///
/// This struct is the single source of truth for configuration.
/// All string parsing (e.g., address strings to Endpoint) happens during
/// construction; the Raft timing values are validated later, when the node is
/// built, because OpenRaft validates the final `OpenRaftConfig` — including an
/// override supplied through [`RaftNodeBuilder::raft_config`].
///
/// [`RaftNodeBuilder::raft_config`]: crate::node::RaftNodeBuilder::raft_config
#[derive(Debug, Clone)]
pub struct Config {
  pub node_id: u64,
  pub raft: RaftConfig,
  pub rocksdb: RocksdbConfig,
}

/// RocksDB configuration
///
/// The tuning fields mirror `engine::RocksDBConfig`; `None` keeps the
/// engine's built-in default for that field.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RocksdbConfig {
  pub data_path: String,
  pub max_open_files: i32,
  /// Shared LRU block cache size in bytes.
  pub block_cache_size: Option<usize>,
  /// Memtable (write buffer) size in bytes per column family.
  pub write_buffer_size: Option<usize>,
  /// Number of memtables per column family before flushing is forced.
  pub max_write_buffer_number: Option<i32>,
}

impl RocksdbConfig {
  /// Resolve tuning options into an explicit engine config: `None` fields
  /// fall back to the engine defaults.
  pub(crate) fn resolve_engine_config(&self) -> crate::engine::RocksDBConfig {
    let d = crate::engine::RocksDBConfig::default();
    crate::engine::RocksDBConfig {
      block_cache_size: self.block_cache_size.unwrap_or(d.block_cache_size),
      write_buffer_size: self.write_buffer_size.unwrap_or(d.write_buffer_size),
      max_write_buffer_number: self
        .max_write_buffer_number
        .unwrap_or(d.max_write_buffer_number),
    }
  }
}

/// Raft configuration with parsed endpoints
#[derive(Debug, Clone)]
pub struct RaftConfig {
  /// The endpoint this node listens on (parsed from address string)
  pub endpoint: Endpoint,
  /// The advertised endpoint for other nodes to connect to
  pub advertise_endpoint: Endpoint,
  /// Addresses of nodes to join (empty for single-node cluster)
  pub join: Vec<String>,
  /// Heartbeat interval in milliseconds (overrides OpenRaft default)
  pub heartbeat_interval: Option<u64>,
  /// Minimum election timeout in milliseconds (overrides OpenRaft default)
  pub election_timeout_min: Option<u64>,
  /// Maximum election timeout in milliseconds (overrides OpenRaft default)
  pub election_timeout_max: Option<u64>,
  /// Maximum gRPC message size in bytes for raft RPCs (encode and decode).
  ///
  /// Defaults to 256MB. Must be large enough for the biggest AppendEntries
  /// payload (`max_payload_entries` × max entry size) or replication breaks.
  pub grpc_max_message_size: Option<u64>,
  /// Maximum number of entries OpenRaft puts into one `AppendEntries` RPC.
  ///
  /// Defaults to 50, below OpenRaft's own 300, so that a batch stays under
  /// `grpc_max_message_size` for entries of a few megabytes. The two settings
  /// have to agree: `max_payload_entries × max entry size` must stay below the
  /// gRPC cap, or the follower cannot decode the batch and replication retries
  /// it forever.
  pub max_payload_entries: Option<u64>,
  /// How reads are served: staleness/performance trade-off chosen per node.
  ///
  /// Defaults to `LeaderApplied` (the historical behavior). See
  /// `ReadConsistency` for the semantics of each level.
  pub read_consistency: ReadConsistency,
}

impl RaftConfig {
  /// Resolve the effective gRPC max message size in bytes.
  pub(crate) fn grpc_max_message_size(&self) -> usize {
    self
      .grpc_max_message_size
      .map(|v| v as usize)
      .unwrap_or(crate::config::DEFAULT_GRPC_MAX_MESSAGE_SIZE)
  }

  /// Build an OpenRaft Config from these settings, applying any overrides.
  ///
  /// The result is validated: `Raft::new` does not validate, and an unvalidated
  /// config panics inside the election timer (`random_range(min..max)` on an
  /// empty range) long after start-up. Failing here turns a bad TOML file into a
  /// start-up error instead.
  pub(crate) fn to_openraft_config(&self) -> Result<OpenRaftConfig> {
    let mut cfg = OpenRaftConfig::default();
    if let Some(v) = self.heartbeat_interval {
      cfg.heartbeat_interval = v;
    }
    if let Some(v) = self.election_timeout_min {
      cfg.election_timeout_min = v;
    }
    if let Some(v) = self.election_timeout_max {
      cfg.election_timeout_max = v;
    }
    cfg.max_payload_entries = self
      .max_payload_entries
      .unwrap_or(DEFAULT_MAX_PAYLOAD_ENTRIES);
    validate_raft_config(cfg)
  }
}

/// Validate an OpenRaft config, reporting a bad one as a configuration error.
///
/// Also used for a config handed straight to [`crate::node::RaftNodeBuilder`],
/// which never goes through [`RaftConfig::to_openraft_config`].
pub(crate) fn validate_raft_config(cfg: OpenRaftConfig) -> Result<OpenRaftConfig> {
  cfg
    .validate()
    .map_err(|e| Error::config(format!("invalid raft configuration: {}", e)))
}

/// Raw configuration for deserialization
///
/// This is an internal struct used only for deserializing from files/JSON.
/// Users should use `Config` which has all fields parsed.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct RawConfig {
  pub node_id: u64,

  #[serde(default = "default_raft_config")]
  pub raft: RawRaftConfig,

  #[serde(default = "default_rocksdb_config")]
  pub rocksdb: RocksdbConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub(crate) struct RawRaftConfig {
  pub address: String,
  pub advertise_host: String,
  pub join: Vec<String>,
  #[serde(default)]
  pub heartbeat_interval: Option<u64>,
  #[serde(default)]
  pub election_timeout_min: Option<u64>,
  #[serde(default)]
  pub election_timeout_max: Option<u64>,
  #[serde(default)]
  pub grpc_max_message_size: Option<u64>,
  #[serde(default)]
  pub max_payload_entries: Option<u64>,
  #[serde(default)]
  pub read_consistency: ReadConsistency,
}

impl Config {
  /// Validate and parse the configuration
  ///
  /// This method parses all string fields into their typed representations
  /// and validates the configuration.
  pub(crate) fn validate_and_parse(raw: RawConfig) -> Result<Self> {
    // Parse raft endpoint
    let endpoint = Endpoint::parse(&raw.raft.address)?;

    // Parse advertise endpoint (use same port if not specified)
    let advertise_endpoint = if raw.raft.advertise_host.is_empty() {
      endpoint.clone()
    } else {
      Endpoint::new(&raw.raft.advertise_host, endpoint.port())
    };

    Ok(Config {
      node_id: raw.node_id,
      raft: RaftConfig {
        endpoint,
        advertise_endpoint,
        join: raw.raft.join,
        heartbeat_interval: raw.raft.heartbeat_interval,
        election_timeout_min: raw.raft.election_timeout_min,
        election_timeout_max: raw.raft.election_timeout_max,
        grpc_max_message_size: raw.raft.grpc_max_message_size,
        max_payload_entries: raw.raft.max_payload_entries,
        read_consistency: raw.raft.read_consistency,
      },
      rocksdb: raw.rocksdb,
    })
  }
}

impl<'de> Deserialize<'de> for Config {
  fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let raw = RawConfig::deserialize(deserializer)?;
    Self::validate_and_parse(raw).map_err(serde::de::Error::custom)
  }
}

impl Serialize for Config {
  fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    // Convert back to RawConfig for serialization
    let raw = RawConfig {
      node_id: self.node_id,
      raft: RawRaftConfig {
        address: self.raft.endpoint.to_string(),
        advertise_host: self.raft.advertise_endpoint.addr().to_string(),
        join: self.raft.join.clone(),
        heartbeat_interval: self.raft.heartbeat_interval,
        election_timeout_min: self.raft.election_timeout_min,
        election_timeout_max: self.raft.election_timeout_max,
        grpc_max_message_size: self.raft.grpc_max_message_size,
        max_payload_entries: self.raft.max_payload_entries,
        read_consistency: self.raft.read_consistency,
      },
      rocksdb: self.rocksdb.clone(),
    };
    raw.serialize(serializer)
  }
}

#[cfg(test)]
mod tests {
  use super::super::default::DEFAULT_GRPC_MAX_MESSAGE_SIZE;
  use super::super::default::DEFAULT_MAX_PAYLOAD_ENTRIES;
  use super::*;

  fn make_raft_config(grpc_max_message_size: Option<u64>) -> RaftConfig {
    let endpoint = Endpoint::parse("127.0.0.1:6682").unwrap();
    RaftConfig {
      endpoint: endpoint.clone(),
      advertise_endpoint: endpoint,
      join: vec![],
      heartbeat_interval: None,
      election_timeout_min: None,
      election_timeout_max: None,
      grpc_max_message_size,
      max_payload_entries: None,
      read_consistency: ReadConsistency::default(),
    }
  }

  /// A bad election timeout used to reach OpenRaft unvalidated and panic inside
  /// the election tick (`random_range(min..max)` on an empty range). It has to
  /// come back as a configuration error from the call that builds the config.
  #[test]
  fn test_to_openraft_config_rejects_an_empty_election_timeout_range() {
    let mut cfg = make_raft_config(None);
    cfg.election_timeout_min = Some(2000);
    cfg.election_timeout_max = Some(1000);

    let err = cfg
      .to_openraft_config()
      .expect_err("min >= max must be rejected");

    assert!(
      err.to_string().contains("election"),
      "the error has to name the setting, got: {}",
      err
    );
  }

  #[test]
  fn test_to_openraft_config_rejects_an_election_timeout_below_the_heartbeat() {
    let mut cfg = make_raft_config(None);
    cfg.heartbeat_interval = Some(2000);
    cfg.election_timeout_min = Some(2000);
    cfg.election_timeout_max = Some(4000);

    let err = cfg
      .to_openraft_config()
      .expect_err("an election timeout at or below the heartbeat interval must be rejected");

    assert!(
      err.to_string().contains("heartbeat"),
      "the error has to name the setting, got: {}",
      err
    );
  }

  #[test]
  fn test_to_openraft_config_accepts_the_defaults() {
    let cfg = make_raft_config(None);
    assert!(
      cfg.to_openraft_config().is_ok(),
      "the defaults must survive validation"
    );
  }

  /// OpenRaft's own default (300) can exceed the gRPC cap once entries are
  /// large, so rockraft batches far fewer entries unless told otherwise.
  #[test]
  fn test_max_payload_entries_defaults_below_the_openraft_default() {
    let cfg = make_raft_config(None);
    assert_eq!(
      cfg.to_openraft_config().unwrap().max_payload_entries,
      DEFAULT_MAX_PAYLOAD_ENTRIES
    );
    assert!(
      DEFAULT_MAX_PAYLOAD_ENTRIES < openraft::Config::default().max_payload_entries,
      "the default has to be more conservative than OpenRaft's"
    );
  }

  #[test]
  fn test_max_payload_entries_override() {
    let mut cfg = make_raft_config(None);
    cfg.max_payload_entries = Some(7);
    assert_eq!(cfg.to_openraft_config().unwrap().max_payload_entries, 7);
  }

  #[test]
  fn test_grpc_max_message_size_defaults() {
    let cfg = make_raft_config(None);
    assert_eq!(cfg.grpc_max_message_size(), DEFAULT_GRPC_MAX_MESSAGE_SIZE);
  }

  #[test]
  fn test_grpc_max_message_size_override() {
    let cfg = make_raft_config(Some(64 * 1024 * 1024));
    assert_eq!(cfg.grpc_max_message_size(), 64 * 1024 * 1024);
  }

  #[test]
  fn test_raw_config_grpc_max_message_size_roundtrip() {
    let raw = RawConfig {
      node_id: 1,
      raft: RawRaftConfig {
        address: "127.0.0.1:6682".to_string(),
        advertise_host: "".to_string(),
        join: vec![],
        heartbeat_interval: None,
        election_timeout_min: None,
        election_timeout_max: None,
        grpc_max_message_size: Some(8 * 1024 * 1024),
        max_payload_entries: Some(10),
        read_consistency: ReadConsistency::default(),
      },
      rocksdb: Default::default(),
    };

    let config = Config::validate_and_parse(raw).unwrap();
    assert_eq!(config.raft.grpc_max_message_size, Some(8 * 1024 * 1024));
    assert_eq!(config.raft.grpc_max_message_size(), 8 * 1024 * 1024);
    assert_eq!(config.raft.max_payload_entries, Some(10));
  }

  /// The override has to survive the TOML parse chain, like the other raft
  /// knobs, so a deployment can tune it without touching the code.
  #[test]
  fn test_max_payload_entries_from_toml() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
max_payload_entries = 8
"#;
    let config: Config = toml::from_str(toml).unwrap();
    assert_eq!(config.raft.max_payload_entries, Some(8));
    assert_eq!(
      config
        .raft
        .to_openraft_config()
        .unwrap()
        .max_payload_entries,
      8
    );
  }

  /// Omitting the field keeps the rockraft default, not OpenRaft's 300.
  #[test]
  fn test_max_payload_entries_default_from_toml() {
    let config: Config = toml::from_str("node_id = 1").unwrap();
    assert_eq!(config.raft.max_payload_entries, None);
    assert_eq!(
      config
        .raft
        .to_openraft_config()
        .unwrap()
        .max_payload_entries,
      DEFAULT_MAX_PAYLOAD_ENTRIES
    );
  }

  /// `0` means OpenRaft would never replicate anything; the config validation
  /// added for the election timeouts rejects it at start-up too.
  #[test]
  fn test_max_payload_entries_zero_is_rejected() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
max_payload_entries = 0
"#;
    let config: Config = toml::from_str(toml).unwrap();

    let err = config
      .raft
      .to_openraft_config()
      .expect_err("zero entries per append must be rejected");

    assert!(
      err.to_string().contains("max_payload_entries"),
      "the error has to name the setting, got: {}",
      err
    );
  }

  /// The two knobs that have to agree are parsed together: a deployment that
  /// stores large entries lowers the batch and can raise the wire cap.
  #[test]
  fn test_batch_and_wire_limits_parse_together() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
max_payload_entries = 5
grpc_max_message_size = 67108864
"#;
    let config: Config = toml::from_str(toml).unwrap();

    assert_eq!(config.raft.max_payload_entries, Some(5));
    assert_eq!(config.raft.grpc_max_message_size, Some(64 * 1024 * 1024));

    let openraft = config.raft.to_openraft_config().unwrap();
    assert_eq!(openraft.max_payload_entries, 5);
  }

  /// `Config` serializes back through `RawConfig`; a tuning value must not be
  /// dropped on the way out and back in.
  #[test]
  fn test_max_payload_entries_survives_a_serde_roundtrip() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
max_payload_entries = 12
"#;
    let config: Config = toml::from_str(toml).unwrap();

    let serialized = toml::to_string(&config).unwrap();
    let restored: Config = toml::from_str(&serialized).unwrap();

    assert_eq!(restored.raft.max_payload_entries, Some(12));
    assert_eq!(
      restored
        .raft
        .to_openraft_config()
        .unwrap()
        .max_payload_entries,
      12
    );
  }
}

#[cfg(test)]
mod read_consistency_tests {
  use super::*;
  use crate::config::ReadConsistency;

  /// TOML with an explicit read_consistency value must flow through the
  /// RawConfig -> Config parse chain unaffected.
  #[test]
  fn test_read_consistency_from_toml() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
read_consistency = "read_index"
[rocksdb]
data_path = "/tmp/rc-test"
max_open_files = 10000
"#;
    let config: Config = toml::from_str(toml).unwrap();
    assert_eq!(config.raft.read_consistency, ReadConsistency::ReadIndex);
  }

  /// No field = default (current behavior preserved).
  #[test]
  fn test_read_consistency_defaults_forwarded() {
    let config: Config = toml::from_str("node_id = 1").unwrap();
    assert_eq!(config.raft.read_consistency, ReadConsistency::LeaderApplied);
  }

  #[test]
  fn test_read_consistency_invalid_value_rejected() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
read_consistency = "ultra"
"#;
    assert!(
      toml::from_str::<Config>(toml).is_err(),
      "unknown mode must be a config error, not silently ignored"
    );
  }
}

#[cfg(test)]
mod rocksdb_config_tests {
  use super::*;

  /// Explicit tuning values must flow TOML -> RocksdbConfig -> engine config.
  #[test]
  fn test_rocksdb_tuning_from_toml() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
[rocksdb]
data_path = "/tmp/tune-test"
max_open_files = 42
block_cache_size = 123456
write_buffer_size = 654321
max_write_buffer_number = 7
"#;
    let config: Config = toml::from_str(toml).unwrap();
    let tuned = config.rocksdb.resolve_engine_config();
    assert_eq!(tuned.block_cache_size, 123456);
    assert_eq!(tuned.write_buffer_size, 654321);
    assert_eq!(tuned.max_write_buffer_number, 7);
  }

  /// Missing tuning fields resolve to the engine defaults.
  #[test]
  fn test_rocksdb_tuning_defaults() {
    let toml = r#"
node_id = 1
[raft]
address = "127.0.0.1:7001"
advertise_host = "127.0.0.1"
join = []
[rocksdb]
data_path = "/tmp/tune-default"
max_open_files = 42
"#;
    let config: Config = toml::from_str(toml).unwrap();
    let resolved = config.rocksdb.resolve_engine_config();
    let default = crate::engine::RocksDBConfig::default();
    assert_eq!(resolved.block_cache_size, default.block_cache_size);
    assert_eq!(resolved.write_buffer_size, default.write_buffer_size);
    assert_eq!(
      resolved.max_write_buffer_number,
      default.max_write_buffer_number
    );
  }
}
