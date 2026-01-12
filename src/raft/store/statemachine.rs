use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use bincode::deserialize;
use bincode::serialize;
use futures::Stream;
use futures::TryStreamExt;
use openraft::alias::SnapshotDataOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use rocksdb::BoundColumnFamily;
use rocksdb::DB;

use super::keys::LAST_APPLIED_LOG_KEY;
use super::keys::LAST_MEMBERSHIP_KEY;
use super::keys::NODES_KEY;
use super::keys::SM_DATA_FAMILY;
use super::keys::SM_META_FAMILY;
use super::snapshot::build_snapshot;
use super::snapshot::get_current_snapshot;
use crate::raft::store::snapshot::recover_snapshot;
use crate::raft::types::read_logs_err;
use crate::raft::types::AppResponseData;
use crate::raft::types::Cmd;
use crate::raft::types::LogId;
use crate::raft::types::Node;
use crate::raft::types::NodeId;
use crate::raft::types::Operation;
use crate::raft::types::RaftCodec as _;
use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotMeta;
use crate::raft::types::StoredMembership;
use crate::raft::types::SysData;
use crate::raft::types::TypeConfig;

#[derive(Debug)]
pub struct RocksStateMachine {
  db: Arc<DB>,
  snapshot_dir: PathBuf,

  sys_data: Mutex<SysData>,
}

impl Clone for RocksStateMachine {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      snapshot_dir: self.snapshot_dir.clone(),
      sys_data: Mutex::new(self.sys_data.lock().unwrap().clone()),
    }
  }
}

impl RocksStateMachine {
  pub async fn new(db: Arc<DB>, data_dir: PathBuf) -> Result<RocksStateMachine, std::io::Error> {
    db.cf_handle(SM_META_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_meta` not found"))?;
    db.cf_handle(SM_DATA_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_data` not found"))?;

    let snapshot_dir = data_dir.join("snapshot");
    let sys_data = Self::recover_sys_data(&db)?;

    Ok(Self {
      db,
      snapshot_dir,
      sys_data: Mutex::new(sys_data),
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
      .ok_or_else(|| std::io::Error::other("column family `_sm_meta` not found"))?;

    // Recover last_applied
    let last_applied = match db.get_cf(&cf_meta, LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let log_id = deserialize(&v).map_err(read_logs_err)?;
        Some(log_id)
      }
      Ok(None) => None,
      Err(e) => return Err(io::Error::other(e)),
    };

    // Recover last_membership
    let last_membership = db
      .get_cf(&cf_meta, LAST_MEMBERSHIP_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| StoredMembership::decode_from(&bytes))
      .transpose()?
      .unwrap_or_default();

    // Recover nodes
    let nodes = db
      .get_cf(&cf_meta, NODES_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| deserialize(&bytes).map_err(read_logs_err))
      .transpose()?
      .unwrap_or_default();

    Ok(SysData {
      last_applied,
      last_membership,
      nodes,
    })
  }

  fn get_last_applied_log_id(&self) -> Result<Option<LogId>, io::Error> {
    return Ok(self.sys_data.lock().unwrap().last_applied);
    /*
    match self.db.get_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let log_id = deserialize(&v).map_err(read_logs_err)?;
        Ok(Some(log_id))
      }
      Ok(None) => Ok(None),
      Err(e) => Err(io::Error::other(e)),
    }
    */
  }

  fn get_last_membership(&self) -> Result<StoredMembership, io::Error> {
    return Ok(self.sys_data.lock().unwrap().last_membership.clone());
    /*
    Ok(
      self
        .db
        .get_cf(&self.cf_sm_meta(), LAST_MEMBERSHIP_KEY)
        .map_err(read_logs_err)?
        .map(|bytes| StoredMembership::decode_from(&bytes))
        .transpose()?
        .unwrap_or_default(),
    )
    */
  }

  fn set_last_applied_log_id(&self, log_id: Option<LogId>) -> Result<(), io::Error> {
    let mut sys_data = self.sys_data.lock().unwrap();

    match log_id {
      Some(id) => {
        let data = serialize(&id).map_err(read_logs_err)?;
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

  fn set_last_membership(&self, membership: &StoredMembership) -> Result<(), io::Error> {
    let mut sys_data = self.sys_data.lock().unwrap();

    let data = serialize(membership).map_err(read_logs_err)?;
    self
      .db
      .put_cf(&self.cf_sm_meta(), LAST_MEMBERSHIP_KEY, data)
      .map_err(read_logs_err)?;
    sys_data.last_membership = membership.clone();
    Ok(())
  }

  fn add_node(&self, node: Node) -> Result<(), io::Error> {
    let mut sys_data = self.sys_data.lock().unwrap();

    sys_data.nodes.insert(node.node_id, node);

    let data = serialize(&sys_data.nodes).map_err(read_logs_err)?;
    self
      .db
      .put_cf(&self.cf_sm_meta(), NODES_KEY, data)
      .map_err(read_logs_err)?;

    Ok(())
  }

  fn remove_node(&self, node_id: NodeId) -> Result<(), io::Error> {
    let mut sys_data = self.sys_data.lock().unwrap();

    sys_data.nodes.remove(&node_id);

    let data = serialize(&sys_data.nodes).map_err(read_logs_err)?;
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
    let mut last_membership = None;
    let mut responses = Vec::new();

    while let Some((entry, responder)) = entries.try_next().await? {
      last_applied_log_id = Some(entry.log_id);

      let response = match entry.payload {
        EntryPayload::Blank => AppResponseData { value: None },
        EntryPayload::Normal(req) => {
          match req {
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
            }
            Cmd::AddNode { node, .. } => {
              self.add_node(node)?;
            }
            Cmd::RemoveNode { node_id } => {
              self.remove_node(node_id)?;
            }
            _ => {}
          }

          AppResponseData { value: None }
        }
        EntryPayload::Membership(mem) => {
          last_membership = Some(StoredMembership::new(Some(entry.log_id), mem));
          AppResponseData { value: None }
        }
      };

      if let Some(responder) = responder {
        responses.push((responder, response));
      }
    }

    // Atomic write of all data + metadata - fail fast before sending any responses
    // Add metadata writes to the batch for atomic commit
    self
      .db
      .write(batch)
      .map_err(|e| io::Error::other(e.to_string()))?;

    if let Some(last_applied_log_id) = last_applied_log_id {
      self.set_last_applied_log_id(Some(last_applied_log_id))?;
    }

    if let Some(last_membership) = last_membership {
      self.set_last_membership(&last_membership)?;
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

    if let Some(snapshot) = data {
      if let Some(id) = self.get_last_applied_log_id()? {
        if let Some(snapshot_id) = snapshot.meta.last_log_id {
          if snapshot_id >= id {
            return Ok(Some(snapshot));
          }
        }
      }
    }

    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use std::collections::{BTreeMap, BTreeSet};

  use openraft::Membership;

  use crate::{
    engine::RocksDBEngine,
    raft::types::{Endpoint, LeaderId, Node},
  };

  use super::*;

  async fn create_test_state_machine() -> RocksStateMachine {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    );

    RocksStateMachine::new(engine.db.clone(), temp_data_dir)
      .await
      .unwrap()
  }

  fn create_log_id(term: u64, node_id: u64, index: u64) -> LogId {
    LogId {
      leader_id: LeaderId { term, node_id },
      index,
    }
  }

  fn create_stored_membership(log_id: LogId) -> StoredMembership {
    let mut nodes = BTreeSet::new();
    nodes.insert(1);

    let mut node_map = BTreeMap::new();
    node_map.insert(
      1,
      Node {
        node_id: 1,
        endpoint: Endpoint::new("127.0.0.1", 1228),
      },
    );

    let membership = Membership::new(vec![nodes], node_map).unwrap();
    StoredMembership::new(Some(log_id), membership)
  }

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
  async fn test_set_and_get_last_membership() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    assert!(sm.get_last_membership()?.log_id().is_none());

    let log_id = create_log_id(1, 1, 100);
    let membership = create_stored_membership(log_id);
    sm.set_last_membership(&membership)?;

    let retrieved = sm.get_last_membership()?;
    assert_eq!(retrieved.log_id(), membership.log_id());
    assert_eq!(
      retrieved.membership().get_joint_config().len(),
      membership.membership().get_joint_config().len()
    );

    let new_log_id = create_log_id(2, 2, 200);
    let new_membership = create_stored_membership(new_log_id);
    sm.set_last_membership(&new_membership)?;

    let updated = sm.get_last_membership()?;
    assert_eq!(updated.log_id().unwrap().leader_id.term, 2);
    assert_eq!(updated.log_id().unwrap().index, 200);

    Ok(())
  }
}
