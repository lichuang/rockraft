use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::LogId;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::StoredMembership;
use openraft::alias::LogIdOf;
use rocksdb::DB;

use super::keys::LAST_APPLIED_LOG_KEY;
use super::keys::LAST_MEMBERSHIP_KEY;
use super::keys::SM_DATA_FAMILY;
use super::keys::SM_META_FAMILY;
use super::snapshot::build_snapshot;
use crate::config::Config;
use crate::types::RaftCodec as _;
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

    build_snapshot(&self.db, last_applied_log, last_membership).await
  }
}
