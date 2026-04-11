use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::raft::types::{decode, encode};
use futures::Stream;
use futures::TryStreamExt;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::alias::SnapshotDataOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use rocksdb::BoundColumnFamily;
use tracing::info;

use rocksdb::DB;

use super::keys::LAST_APPLIED_LOG_KEY;
use super::keys::NODES_KEY;
use super::keys::SM_DATA_FAMILY;
use super::keys::SM_META_FAMILY;
use super::snapshot::build_snapshot;
use super::snapshot::get_current_snapshot;
use crate::raft::store::snapshot::recover_snapshot;
use crate::raft::types::AppliedState;
use crate::raft::types::Cmd;
use crate::raft::types::LogId;
use crate::raft::types::Node;
use crate::raft::types::NodeId;
use crate::raft::types::Operation;
use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotMeta;
use crate::raft::types::StoredMembership;
use crate::raft::types::SysData;
use crate::raft::types::TxnOp;
use crate::raft::types::TypeConfig;
use crate::raft::types::read_logs_err;

#[derive(Debug)]
pub struct RocksStateMachine {
  db: Arc<DB>,
  snapshot_dir: PathBuf,

  // In-memory write-through cache of small metadata (nodes, last_applied_log_id)
  // that is also persisted to RocksDB (SM_META column family).
  //
  // Why not read from RocksDB directly?
  // - apply() processes a stream of log entries; cached `nodes` and `last_applied`
  //   must be visible to subsequent entries within the same batch before the
  //   WriteBatch is committed.
  // - Avoids repeated RocksDB meta-CF lookups on every applied entry.
  //
  // Invariant: every mutation (add_node, set_last_applied_log_id) updates both
  // this cache *and* RocksDB atomically.
  sys_data: Arc<Mutex<SysData>>,
}

/// Evaluate a transaction condition against the actual value
fn evaluate_condition(expected: &TxnOp, actual: Option<&Vec<u8>>) -> bool {
  match expected {
    TxnOp::Exists => actual.is_some(),
    TxnOp::NotExists => actual.is_none(),
    TxnOp::Equal(expected_value) => actual
      .map(|v| v.as_slice() == expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::NotEqual(expected_value) => actual
      .map(|v| v.as_slice() != expected_value.as_slice())
      .unwrap_or(true),
    TxnOp::Greater(expected_value) => actual
      .map(|v| v.as_slice() > expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::Less(expected_value) => actual
      .map(|v| v.as_slice() < expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::GreaterEqual(expected_value) => actual
      .map(|v| v.as_slice() >= expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::LessEqual(expected_value) => actual
      .map(|v| v.as_slice() <= expected_value.as_slice())
      .unwrap_or(false),
  }
}

impl Clone for RocksStateMachine {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      snapshot_dir: self.snapshot_dir.clone(),
      sys_data: self.sys_data.clone(),
    }
  }
}

impl RocksStateMachine {
  fn lock_sys_data(&self) -> Result<std::sync::MutexGuard<'_, SysData>, io::Error> {
    self
      .sys_data
      .lock()
      .map_err(|e| Error::other(format!("Mutex lock failed: {}", e)))
  }

  pub async fn new(db: Arc<DB>, data_dir: PathBuf) -> Result<RocksStateMachine, Error> {
    db.cf_handle(SM_META_FAMILY)
      .ok_or_else(|| Error::other("column family `_sm_meta` not found"))?;
    db.cf_handle(SM_DATA_FAMILY)
      .ok_or_else(|| Error::other("column family `_sm_data` not found"))?;

    let snapshot_dir = data_dir.join("snapshot");
    let sys_data = Self::recover_sys_data(&db)?;

    Ok(Self {
      db,
      snapshot_dir,
      sys_data: Arc::new(Mutex::new(sys_data)),
    })
  }

  fn cf_sm_meta(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(SM_META_FAMILY).unwrap()
  }

  fn cf_sm_data(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(SM_DATA_FAMILY).unwrap()
  }

  fn recover_sys_data(db: &Arc<DB>) -> Result<SysData, io::Error> {
    let cf_meta = db
      .cf_handle(SM_META_FAMILY)
      .ok_or_else(|| io::Error::other("column family `_sm_meta` not found"))?;

    let last_applied = match db.get_cf(&cf_meta, LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let log_id = decode(&v).map_err(read_logs_err)?;
        Some(log_id)
      }
      Ok(None) => None,
      Err(e) => return Err(read_logs_err(e)),
    };

    // Recover nodes
    let nodes = db
      .get_cf(&cf_meta, NODES_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| decode(&bytes).map_err(read_logs_err))
      .transpose()?
      .unwrap_or_default();

    Ok(SysData {
      last_applied,
      nodes,
    })
  }

  fn get_last_applied_log_id(&self) -> Result<Option<LogId>, io::Error> {
    Ok(self.lock_sys_data()?.last_applied)
  }

  /// Get last membership (constructed from nodes)
  pub fn get_last_membership(&self) -> Result<StoredMembership, io::Error> {
    self.build_membership_from_nodes()
  }

  /// Build StoredMembership from current nodes
  fn build_membership_from_nodes(&self) -> Result<StoredMembership, io::Error> {
    let sys_data = self.lock_sys_data()?;
    let node_ids: BTreeSet<NodeId> = sys_data.nodes.keys().cloned().collect();
    let nodes = sys_data.nodes.clone();

    // If no nodes, return default membership
    if node_ids.is_empty() {
      return Ok(StoredMembership::default());
    }

    // Create membership with all nodes as voters
    let membership = Membership::new(vec![node_ids], nodes)
      .map_err(|e| io::Error::other(format!("Failed to create membership: {}", e)))?;

    Ok(StoredMembership::new(None, membership))
  }

  /// Get a value from the KV store by key
  pub fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
    self
      .db
      .get_cf(&self.cf_sm_data(), key.as_bytes())
      .map_err(read_logs_err)
  }

  /// Scan all key-value pairs with the given prefix from the KV store
  #[allow(clippy::type_complexity)]
  pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, io::Error> {
    let cf = self.cf_sm_data();
    let mut results = Vec::new();

    // Use iterator starting from the prefix
    // This works regardless of the prefix extractor configuration
    let iter = self.db.iterator_cf(
      &cf,
      rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
    );

    for item in iter {
      let (key, value) = item.map_err(read_logs_err)?;

      // Check if the key has the prefix
      if !key.starts_with(prefix) {
        break;
      }

      results.push((key.to_vec(), value.to_vec()));
    }

    Ok(results)
  }

  fn set_last_applied_log_id(&self, log_id: Option<LogId>) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    match log_id {
      Some(id) => {
        let data = encode(&id).map_err(read_logs_err)?;
        self
          .db
          .put_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY, data)
          .map_err(read_logs_err)?;
        sys_data.last_applied = log_id;
      }
      None => {
        self
          .db
          .delete_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY)
          .map_err(read_logs_err)?;
        sys_data.last_applied = None;
      }
    }
    Ok(())
  }

  /// Get all nodes from the state machine
  pub fn get_nodes(&self) -> Result<BTreeMap<NodeId, Node>, io::Error> {
    Ok(self.lock_sys_data()?.nodes.clone())
  }

  pub fn contains_node(&self, node_id: NodeId) -> Result<bool, io::Error> {
    Ok(self.lock_sys_data()?.nodes.contains_key(&node_id))
  }

  pub fn add_node(&self, node: Node) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    sys_data.nodes.insert(node.node_id, node);

    let data = encode(&sys_data.nodes).map_err(read_logs_err)?;
    self
      .db
      .put_cf(&self.cf_sm_meta(), NODES_KEY, data)
      .map_err(read_logs_err)?;

    Ok(())
  }

  fn remove_node(&self, node_id: NodeId) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    sys_data.nodes.remove(&node_id);

    let data = encode(&sys_data.nodes).map_err(read_logs_err)?;
    self
      .db
      .put_cf(&self.cf_sm_meta(), NODES_KEY, data)
      .map_err(read_logs_err)?;

    Ok(())
  }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
  async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
    let last_applied_log = self.get_last_applied_log_id()?;
    let last_membership = self.get_last_membership()?;

    build_snapshot(
      &self.db,
      &self.snapshot_dir,
      last_applied_log,
      last_membership,
    )
    .await
  }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
  type SnapshotBuilder = Self;

  async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
    let last_applied_log = self.get_last_applied_log_id()?;
    let last_membership = self.get_last_membership()?;

    Ok((last_applied_log, last_membership))
  }

  async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
  where
    Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
  {
    let mut batch = rocksdb::WriteBatch::default();
    let mut last_applied_log_id = None;
    let mut responses = Vec::new();

    while let Some((entry, responder)) = entries.try_next().await? {
      last_applied_log_id = Some(entry.log_id);

      let response = match entry.payload {
        EntryPayload::Blank => AppliedState::None,
        EntryPayload::Normal(req) => {
          match req.cmd {
            Cmd::UpsertKV(kv) => {
              let cf_data = &self.cf_sm_data();
              match kv.value {
                Operation::Update(value) => {
                  batch.put_cf(cf_data, kv.key.as_bytes(), value);
                }
                Operation::Delete => {
                  batch.delete_cf(cf_data, kv.key.as_bytes());
                }
              }
              AppliedState::None
            }
            Cmd::BatchUpsertKV { entries } => {
              let cf_data = &self.cf_sm_data();
              for kv in entries {
                match kv.value {
                  Operation::Update(value) => {
                    batch.put_cf(cf_data, kv.key.as_bytes(), value);
                  }
                  Operation::Delete => {
                    batch.delete_cf(cf_data, kv.key.as_bytes());
                  }
                }
              }
              AppliedState::None
            }
            Cmd::AddNode { node, .. } => {
              let node_id = node.node_id;
              info!(
                "Applying AddNode command for node {} in state machine",
                node_id
              );
              self.add_node(node)?;
              info!("AddNode command applied successfully for node {}", node_id);
              AppliedState::None
            }
            Cmd::RemoveNode { node_id } => {
              self.remove_node(node_id)?;
              AppliedState::None
            }
            Cmd::Txn { req, .. } => {
              // Execute transaction: check conditions and apply operations
              let cf_data = &self.cf_sm_data();
              let mut all_conditions_met = true;

              // Check all conditions (AND logic)
              for condition in &req.condition {
                let actual_value = self
                  .db
                  .get_cf(cf_data, condition.key.as_bytes())
                  .map_err(read_logs_err)?;
                let condition_met = evaluate_condition(&condition.expected, actual_value.as_ref());
                if !condition_met {
                  all_conditions_met = false;
                  break;
                }
              }

              // Determine which operations to execute
              let ops_to_execute = if all_conditions_met {
                &req.if_then
              } else {
                &req.else_then
              };

              // Collect previous values if requested
              let prev_values = if req.return_previous {
                let mut values = Vec::with_capacity(ops_to_execute.len());
                for kv in ops_to_execute {
                  let old_value = self
                    .db
                    .get_cf(cf_data, kv.key.as_bytes())
                    .map_err(read_logs_err)?;
                  values.push(old_value);
                }
                values
              } else {
                Vec::new()
              };

              // Execute operations
              for kv in ops_to_execute {
                match &kv.value {
                  Operation::Update(value) => {
                    batch.put_cf(cf_data, kv.key.as_bytes(), value);
                  }
                  Operation::Delete => {
                    batch.delete_cf(cf_data, kv.key.as_bytes());
                  }
                }
              }

              info!(
                "Applied transaction: conditions_met={}, if_then_ops={}, else_then_ops={}",
                all_conditions_met,
                req.if_then.len(),
                req.else_then.len()
              );

              // Return transaction result with branch info and optional previous values
              AppliedState::Txn(crate::raft::types::TxnReply::Success {
                branch: all_conditions_met,
                prev_values,
              })
            }
          }
        }
        EntryPayload::Membership(membership) => {
          // Membership changes are handled by AddNode/RemoveNode commands
          // which update the nodes map directly
          info!("applying membership: {:?}", membership);
          AppliedState::None
        }
      };

      if let Some(responder) = responder {
        responses.push((responder, response));
      }
    }

    // Atomic write of all data
    self.db.write(batch).map_err(read_logs_err)?;

    if let Some(last_applied_log_id) = last_applied_log_id {
      self.set_last_applied_log_id(Some(last_applied_log_id))?;
    }

    // Only send responses after successful write
    for (responder, response) in responses {
      responder.send(response);
    }

    Ok(())
  }

  async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
    self.clone()
  }

  async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TypeConfig>, io::Error> {
    let data = get_current_snapshot(&self.snapshot_dir).await?;
    match data {
      Some(da) => Ok(da.snapshot),
      None => Err(io::Error::other("Cannot find current snapshot")),
    }
  }

  async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta,
    snapshot: SnapshotDataOf<TypeConfig>,
  ) -> Result<(), io::Error> {
    recover_snapshot(
      &self.db,
      Snapshot {
        meta: meta.clone(),
        snapshot,
      },
    )
    .await
  }

  async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
    let data = get_current_snapshot(&self.snapshot_dir).await?;

    if let Some(snapshot) = data
      && let Some(id) = self.get_last_applied_log_id()?
      && let Some(snapshot_id) = snapshot.meta.last_log_id
      && snapshot_id >= id
    {
      return Ok(Some(snapshot));
    }

    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::HashMap;

  use crate::engine::RocksDBEngine;
  use crate::raft::types::{Endpoint, Node};
  use crate::utils::test::{create_log_id, create_test_state_machine};

  #[tokio::test]
  async fn test_set_and_get_last_applied() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    assert!(sm.get_last_applied_log_id()?.is_none());

    let log_id = create_log_id(1, 1, 100);
    sm.set_last_applied_log_id(Some(log_id))?;

    let retrieved = sm.get_last_applied_log_id()?.unwrap();
    assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
    assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(retrieved.index, log_id.index);

    let new_log_id = create_log_id(2, 2, 200);
    sm.set_last_applied_log_id(Some(new_log_id))?;

    let updated = sm.get_last_applied_log_id()?.unwrap();
    assert_eq!(updated.leader_id.term, 2);
    assert_eq!(updated.leader_id.node_id, 2);
    assert_eq!(updated.index, 200);

    sm.set_last_applied_log_id(None)?;
    assert!(sm.get_last_applied_log_id()?.is_none());

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_last_applied() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and write data
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let log_id = create_log_id(3, 5, 300);
    sm1.set_last_applied_log_id(Some(log_id))?;

    // Create a new state machine instance from the same database
    // This should recover the data from the database
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let recovered = sm2.get_last_applied_log_id()?;
    assert!(recovered.is_some());

    let recovered_log_id = recovered.unwrap();
    assert_eq!(recovered_log_id.leader_id.term, log_id.leader_id.term);
    assert_eq!(recovered_log_id.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(recovered_log_id.index, log_id.index);

    // Update the data again
    let new_log_id = create_log_id(4, 6, 400);
    sm2.set_last_applied_log_id(Some(new_log_id))?;

    // Create another instance and verify the updated data
    let sm3 = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    let updated = sm3.get_last_applied_log_id()?.unwrap();
    assert_eq!(updated.leader_id.term, 4);
    assert_eq!(updated.leader_id.node_id, 6);
    assert_eq!(updated.index, 400);

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_nodes() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and add nodes
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let node1 = Node {
      node_id: 1,
      endpoint: Endpoint::new("127.0.0.1", 8081),
    };
    sm1.add_node(node1.clone())?;

    let node2 = Node {
      node_id: 2,
      endpoint: Endpoint::new("127.0.0.1", 8082),
    };
    sm1.add_node(node2)?;

    // Create a new state machine instance from the same database
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Verify nodes are recovered
    let sys_data = sm2
      .sys_data
      .lock()
      .expect("Mutex lock should not be poisoned");
    assert_eq!(sys_data.nodes.len(), 2);
    assert!(sys_data.nodes.contains_key(&1));
    assert!(sys_data.nodes.contains_key(&2));

    let recovered_node1 = sys_data.nodes.get(&1).unwrap();
    assert_eq!(recovered_node1.node_id, 1);
    assert_eq!(recovered_node1.endpoint.addr(), "127.0.0.1");
    assert_eq!(recovered_node1.endpoint.port(), 8081);

    let recovered_node2 = sys_data.nodes.get(&2).unwrap();
    assert_eq!(recovered_node2.node_id, 2);
    assert_eq!(recovered_node2.endpoint.addr(), "127.0.0.1");
    assert_eq!(recovered_node2.endpoint.port(), 8082);
    drop(sys_data);

    // Remove a node
    sm2.remove_node(1)?;

    // Create another instance and verify
    let sm3 = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    let sys_data = sm3
      .sys_data
      .lock()
      .expect("Mutex lock should not be poisoned");
    assert_eq!(sys_data.nodes.len(), 1);
    assert!(!sys_data.nodes.contains_key(&1));
    assert!(sys_data.nodes.contains_key(&2));

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_all_fields() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and write all fields
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Write last_applied
    let log_id = create_log_id(7, 9, 700);
    sm1.set_last_applied_log_id(Some(log_id))?;

    // Write nodes
    let node1 = Node {
      node_id: 1,
      endpoint: Endpoint::new("127.0.0.1", 9091),
    };
    sm1.add_node(node1)?;

    let node2 = Node {
      node_id: 2,
      endpoint: Endpoint::new("127.0.0.1", 9092),
    };
    sm1.add_node(node2)?;

    let node3 = Node {
      node_id: 3,
      endpoint: Endpoint::new("127.0.0.1", 9093),
    };
    sm1.add_node(node3)?;

    // Create a new state machine instance and verify all data is recovered
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Verify last_applied
    let recovered_log_id = sm2.get_last_applied_log_id()?.unwrap();
    assert_eq!(recovered_log_id.leader_id.term, 7);
    assert_eq!(recovered_log_id.leader_id.node_id, 9);
    assert_eq!(recovered_log_id.index, 700);

    // Verify membership is constructed from nodes
    let recovered_membership = sm2.get_last_membership()?;
    let voter_ids: BTreeSet<_> = recovered_membership.membership().voter_ids().collect();
    assert_eq!(voter_ids.len(), 3);
    assert!(voter_ids.contains(&1));
    assert!(voter_ids.contains(&2));
    assert!(voter_ids.contains(&3));

    // Verify nodes
    let sys_data = sm2
      .sys_data
      .lock()
      .expect("Mutex lock should not be poisoned");
    assert_eq!(sys_data.nodes.len(), 3);
    assert!(sys_data.nodes.contains_key(&1));
    assert!(sys_data.nodes.contains_key(&2));
    assert!(sys_data.nodes.contains_key(&3));

    // Verify node 3
    let recovered_node3 = sys_data.nodes.get(&3).unwrap();
    assert_eq!(recovered_node3.node_id, 3);
    assert_eq!(recovered_node3.endpoint.addr(), "127.0.0.1");
    assert_eq!(recovered_node3.endpoint.port(), 9093);

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_basic() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data with different prefixes
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"user:1:name", b"Alice")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:1:age", b"25")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:2:name", b"Bob")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:2:age", b"30")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"product:1:name", b"Laptop")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"product:1:price", b"999")
      .map_err(read_logs_err)?;

    // Scan with "user:" prefix
    let user_results = sm.scan_prefix(b"user:")?;
    assert_eq!(user_results.len(), 4);

    // Verify all user keys are returned
    let keys: Vec<_> = user_results
      .iter()
      .map(|(k, _)| String::from_utf8_lossy(k).to_string())
      .collect();
    assert!(keys.contains(&"user:1:name".to_string()));
    assert!(keys.contains(&"user:1:age".to_string()));
    assert!(keys.contains(&"user:2:name".to_string()));
    assert!(keys.contains(&"user:2:age".to_string()));

    // Scan with "user:1:" prefix
    let user1_results = sm.scan_prefix(b"user:1:")?;
    assert_eq!(user1_results.len(), 2);

    let user1_keys: Vec<_> = user1_results
      .iter()
      .map(|(k, _)| String::from_utf8_lossy(k).to_string())
      .collect();
    assert!(user1_keys.contains(&"user:1:name".to_string()));
    assert!(user1_keys.contains(&"user:1:age".to_string()));

    // Scan with "product:" prefix
    let product_results = sm.scan_prefix(b"product:")?;
    assert_eq!(product_results.len(), 2);

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_empty_result() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"key1", b"value1")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"key2", b"value2")
      .map_err(read_logs_err)?;

    // Scan with non-existent prefix
    let results = sm.scan_prefix(b"nonexistent:")?;
    assert!(results.is_empty());

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_binary_keys() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert binary data with prefixes
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, &[0x01, 0x00, 0x01], b"data1")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, &[0x01, 0x00, 0x02], b"data2")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, &[0x01, 0x01, 0x01], b"data3")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, &[0x02, 0x00, 0x01], b"data4")
      .map_err(read_logs_err)?;

    // Scan with prefix [0x01, 0x00]
    let results = sm.scan_prefix(&[0x01, 0x00])?;
    assert_eq!(results.len(), 2);

    // Verify correct data is returned
    let values: Vec<_> = results.iter().map(|(_, v)| v.clone()).collect();
    assert!(values.contains(&b"data1".to_vec()));
    assert!(values.contains(&b"data2".to_vec()));

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_values_match() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"config:host", b"localhost")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"config:port", b"8080")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"config:timeout", b"30")
      .map_err(read_logs_err)?;

    // Scan and verify values
    let results = sm.scan_prefix(b"config:")?;
    assert_eq!(results.len(), 3);

    // Convert to a map for easier verification
    let map: HashMap<_, _> = results.into_iter().collect();

    assert_eq!(
      map.get(b"config:host".as_slice()),
      Some(&b"localhost".to_vec())
    );
    assert_eq!(map.get(b"config:port".as_slice()), Some(&b"8080".to_vec()));
    assert_eq!(map.get(b"config:timeout".as_slice()), Some(&b"30".to_vec()));

    Ok(())
  }
}
