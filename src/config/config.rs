use serde::Deserialize;
use serde::Serialize;

use super::default::default_raft_config;
use super::default::default_rocksdb_config;
use super::endpoint::Endpoint;
use crate::error::Result;

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
      },
      rocksdb: self.rocksdb.clone(),
    };
    raw.serialize(serializer)
  }
}
