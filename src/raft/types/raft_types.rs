use core::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

pub type SnapshotData = tokio::fs::File;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageData {
  pub key: String,
  pub value: Bytes,
}

impl fmt::Display for StorageData {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "({:?}, {:?})", self.key, self.value)
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppResponseData {
  pub value: Option<Bytes>,
}

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftNode {
  pub node_id: u64,
  pub rpc_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct KeyValue {
  pub key: Bytes,
  pub value: Bytes,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StorageData,
        R = AppResponseData,
        Node = RaftNode,
        SnapshotData = SnapshotData
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;
