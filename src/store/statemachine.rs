use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bincode::deserialize;
use futures::Stream;
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
use crate::store::snapshot::recover_snapshot;
use crate::types::RaftCodec as _;
use crate::types::SnapshotData;
use crate::types::TypeConfig;
use crate::types::read_logs_err;

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
      return Ok(Some(snapshot));
      // if let Some(id) = self.data.last_applied_log_id {
      // if let Some(snapshot_id) = snapshot.meta.last_log_id {
      // if snapshot_id >= id {
      // return Ok(Some(snapshot));
      // }
      // }
      // }
    }

    Ok(None)
  }
}
