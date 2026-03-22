use std::fmt;

use serde::{Deserialize, Serialize};

use crate::raft::types::{Node, NodeId};

use super::txn::{TxnReply, TxnReq};
use super::upsert_kv::UpsertKV;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
  /// Add node if absent
  AddNode {
    node: Node,
    /// Whether to override existing record.
    #[serde(default)]
    overriding: bool,
  },

  /// Remove node
  RemoveNode { node_id: NodeId },

  /// Update or insert a general purpose kv store
  UpsertKV(UpsertKV),

  /// Batch update or insert multiple kv entries atomically
  BatchUpsertKV { entries: Vec<UpsertKV> },

  /// Transaction with conditional operations
  ///
  /// The transaction executes condition checks and performs operations
  /// atomically based on the condition results.
  Txn {
    req: TxnReq,
    result: Option<TxnReply>,
  },
}

impl fmt::Display for Cmd {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Cmd::AddNode { node, overriding } => {
        if *overriding {
          write!(f, "add_node(override):{}", node)
        } else {
          write!(f, "add_node(no-override):{}", node)
        }
      }
      Cmd::RemoveNode { node_id } => {
        write!(f, "remove_node:{}", node_id)
      }

      Cmd::UpsertKV(upsert_kv) => {
        write!(f, "upsert_kv:{}", upsert_kv)
      }

      Cmd::BatchUpsertKV { entries } => {
        write!(f, "batch_upsert_kv: {} entries", entries.len())
      }

      Cmd::Txn { req, .. } => {
        write!(
          f,
          "txn: {} conditions, {} if_then, {} else_then",
          req.condition.len(),
          req.if_then.len(),
          req.else_then.len()
        )
      }
    }
  }
}
