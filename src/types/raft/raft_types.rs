use core::fmt;

use bytes::Bytes;
use openraft::StorageError;
use serde::Deserialize;
use serde::Serialize;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub struct Node {
  pub node_id: u64,
  pub rpc_addr: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StorageData,
        R = AppResponseData,
        Node = Node,
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type Vote = openraft::Vote<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
