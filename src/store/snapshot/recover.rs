use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::Snapshot;
use rocksdb::DB;

use crate::types::TypeConfig;

pub async fn recover_snapshot(
  db: &Arc<DB>,
  snapshot: Snapshot<TypeConfig>,
) -> Result<(), io::Error> {
  Ok(())
}
