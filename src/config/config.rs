use serde::Deserialize;
use serde::Serialize;

use super::default::default_raft_config;
use super::default::default_rocksdb_config;
use super::endpoint::Endpoint;
use crate::error::Result;
use openraft::Config as OpenRaftConfig;

/// Node configuration with all fields parsed and validated.
///
/// This struct is the single source of truth for configuration.
/// All string parsing (e.g., address strings to Endpoint) happens during construction.
#[derive(Debug, Clone)]
pub struct Config {
  pub node_id: u64,
  pub raft: RaftConfig,
  pub rocksdb: RocksdbConfig,
}

/// RocksDB configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RocksdbConfig {
  pub data_path: String,
  pub max_open_files: i32,
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
  pub(crate) fn to_openraft_config(&self) -> OpenRaftConfig {
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
    cfg
  }
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
      },
      rocksdb: RocksdbConfig {
        data_path: raw.rocksdb.data_path,
        max_open_files: raw.rocksdb.max_open_files,
      },
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
      },
      rocksdb: self.rocksdb.clone(),
    };
    raw.serialize(serializer)
  }
}

#[cfg(test)]
mod tests {
  use super::super::default::DEFAULT_GRPC_MAX_MESSAGE_SIZE;
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
    }
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
      },
      rocksdb: Default::default(),
    };

    let config = Config::validate_and_parse(raw).unwrap();
    assert_eq!(config.raft.grpc_max_message_size, Some(8 * 1024 * 1024));
    assert_eq!(config.raft.grpc_max_message_size(), 8 * 1024 * 1024);
  }
}
