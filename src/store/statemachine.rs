use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use futures::Stream;
use futures::TryStreamExt;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use rocksdb::DB;

use super::keys::LAST_APPLIED_LOG_KEY;
use super::keys::LAST_MEMBERSHIP_KEY;
use super::keys::SM_DATA_FAMILY;
use super::keys::SM_META_FAMILY;
use super::snapshot::build_snapshot;
use super::snapshot::get_current_snapshot;
use crate::config::Config;
use crate::raft::types::AppResponseData;
use crate::raft::types::RaftCodec as _;
use crate::raft::types::SnapshotData;
use crate::raft::types::TypeConfig;
use crate::raft::types::read_logs_err;
use crate::store::snapshot::recover_snapshot;

#[derive(Debug, Clone)]
pub struct RocksStateMachine {
  db: Arc<DB>,
  snapshot_dir: PathBuf,
}

impl RocksStateMachine {
  pub async fn new(db: Arc<DB>, config: &Config) -> Result<RocksStateMachine, std::io::Error> {
    db.cf_handle(SM_META_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_meta` not found"))?;
    db.cf_handle(SM_DATA_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_data` not found"))?;

    let snapshot_dir = PathBuf::from(config.rocksdb.data_path.clone()).join("snapshot");

    Ok(Self { db, snapshot_dir })
  }

  fn cf_sm_meta(&self) -> &rocksdb::ColumnFamily {
    self.db.cf_handle(SM_META_FAMILY).unwrap()
  }

  fn cf_sm_data(&self) -> &rocksdb::ColumnFamily {
    self.db.cf_handle(SM_DATA_FAMILY).unwrap()
  }

  pub fn get_meta(
    &self,
  ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), io::Error> {
    let cf = self.cf_sm_meta();

    let last_applied_log = self
      .db
      .get_cf(cf, LAST_APPLIED_LOG_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| LogIdOf::<TypeConfig>::decode_from(&bytes))
      .transpose()?;

    let last_membership = self
      .db
      .get_cf(cf, LAST_MEMBERSHIP_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| StoredMembership::<TypeConfig>::decode_from(&bytes))
      .transpose()?
      .unwrap_or_default();

    Ok((last_applied_log, last_membership))
  }

  fn get_last_applied_log_id(&self) -> Result<Option<LogId<TypeConfig>>, io::Error> {
    match self.db.get_cf(self.cf_sm_meta(), LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let log_id = deserialize(&v).map_err(read_logs_err)?;
        Ok(Some(log_id))
      }
      Ok(None) => Ok(None),
      Err(e) => Err(io::Error::other(e)),
    }
  }

  fn set_last_applied_log_id(&self, log_id: Option<LogId<TypeConfig>>) -> Result<(), io::Error> {
    match log_id {
      Some(id) => {
        let data = serialize(&id).map_err(read_logs_err)?;
        self
          .db
          .put_cf(self.cf_sm_meta(), LAST_APPLIED_LOG_KEY, data)
          .map_err(read_logs_err)
      }
      None => self
        .db
        .delete_cf(self.cf_sm_meta(), LAST_APPLIED_LOG_KEY)
        .map_err(read_logs_err),
    }
  }

  fn set_last_membership(
    &self,
    membership: &StoredMembership<TypeConfig>,
  ) -> Result<(), io::Error> {
    let data = serialize(membership).map_err(read_logs_err)?;
    self
      .db
      .put_cf(self.cf_sm_meta(), LAST_MEMBERSHIP_KEY, data)
      .map_err(read_logs_err)
  }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
  async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, io::Error> {
    let (last_applied_log, last_membership) = self.get_meta()?;

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

  async fn applied_state(
    &mut self,
  ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), io::Error> {
    self.get_meta()
  }

  async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
  where Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend
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
          let cf_data = self.cf_sm_data();
          batch.put_cf(cf_data, req.key.as_bytes(), req.value);
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
    meta: &SnapshotMeta<TypeConfig>,
    snapshot: SnapshotDataOf<TypeConfig>,
  ) -> Result<(), io::Error> {
    recover_snapshot(&self.db, Snapshot {
      meta: meta.clone(),
      snapshot,
    })
    .await
  }

  async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
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
