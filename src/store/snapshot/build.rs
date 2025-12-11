use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::LogId;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use rocksdb::DB;
use tokio::fs::File;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tracing::error;
use tracing::info;

use super::util::save_last_snapshot_id_file;
use super::util::save_snapshot_meta;
use super::util::snapshot_data_file;
use super::util::snapshot_dump_file;
use crate::store::keys::SM_DATA_FAMILY;
use crate::store::snapshot::util::snapshot_id_dir;
use crate::types::TypeConfig;
use crate::types::read_logs_err;
use crate::utils::now_millis;

pub async fn build_snapshot(
  db: &Arc<DB>,
  snapshot_dir: &PathBuf,
  last_applied_log_id: Option<LogId<TypeConfig>>,
  last_membership: StoredMembership<TypeConfig>,
) -> Result<Snapshot<TypeConfig>, io::Error> {
  let snapshot_idx = now_millis();

  let snapshot_id = if let Some(last) = last_applied_log_id {
    format!(
      "{}-{}-{}",
      last.committed_leader_id(),
      last.index(),
      snapshot_idx
    )
  } else {
    format!("0-0-{}", snapshot_idx)
  };
  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);
  tokio::fs::create_dir_all(snapshot_id_dir.clone()).await?;

  let meta = SnapshotMeta {
    last_log_id: last_applied_log_id,
    last_membership,
    snapshot_id: snapshot_id.clone(),
  };

  let db = db.clone();
  let snapshot_id_dir_clone = snapshot_id_dir.clone();

  let res = spawn_blocking(move || {
    let snapshot_id_dir = snapshot_id_dir_clone;
    let snapshot = db.snapshot();
    let cf_data = db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family `sm_data` not found");

    let dump_file_name = snapshot_dump_file(&snapshot_id_dir);
    let dump_file = std::fs::File::create(&dump_file_name)?;
    let mut encoder = zstd::Encoder::new(dump_file, 3)?;
    let iter = snapshot.iterator_cf(cf_data, rocksdb::IteratorMode::Start);

    for item in iter {
      let (key, value) = item.map_err(read_logs_err)?;

      encoder.write_all(&(key.len() as u32).to_le_bytes())?;
      encoder.write_all(&key)?;
      encoder.write_all(&(value.len() as u32).to_le_bytes())?;
      encoder.write_all(&value)?;
    }

    encoder.finish()?;

    let snapshot_file_name = snapshot_data_file(&snapshot_id_dir);
    std::fs::rename(&dump_file_name, &snapshot_file_name)?;

    Ok(())
  })
  .await
  .map_err(read_logs_err)?;

  if let Err(e) = res {
    error!(
      "Fail to build snapshot data file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }

  // save snapshot meta file and last_snapshot_id file
  if let Err(e) = save_snapshot_meta(&snapshot_id_dir, meta.clone()).await {
    error!(
      "Fail to save snapshot meta file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }
  // only when save snapshot data file and meta file success, save last_snapshot_id file
  if let Err(e) = save_last_snapshot_id_file(&snapshot_dir, &snapshot_id).await {
    error!(
      "Fail to save last snapshot id file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }

  let res = File::open(&snapshot_data_file(&snapshot_id_dir)).await?;

  let snapshot_dir_owned = snapshot_dir.to_path_buf();
  let snapshot_id_clone = snapshot_id.clone();
  spawn(async move {
    if let Err(e) = vacuum_snapshot_files(snapshot_dir_owned, snapshot_id_clone) {
      error!("Fail to cleanup old snapshot files: {}", e);
    }
  });

  info!(
    "Snapshot build completed successfully for snapshot_id={}",
    snapshot_id
  );

  Ok(Snapshot {
    meta,
    snapshot: res,
  })
}

fn vacuum_snapshot_files(snapshot_dir: PathBuf, last_snapshot_id: String) -> Result<(), io::Error> {
  Ok(())
}
