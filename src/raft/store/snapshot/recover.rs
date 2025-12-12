use std::io::ErrorKind;
use std::io::Read;
use std::io::{self};
use std::sync::Arc;

use openraft::Snapshot;
use rocksdb::DB;
use tracing::error;
use tracing::info;

use crate::raft::store::keys::SM_DATA_FAMILY;
use crate::raft::types::TypeConfig;

pub async fn recover_snapshot(
  db: &Arc<DB>,
  snapshot: Snapshot<TypeConfig>,
) -> Result<(), io::Error> {
  let snapshot_id = snapshot.meta.snapshot_id.clone();

  info!(
    "Starting to recover from snapshot, snapshot_id={}",
    snapshot_id
  );

  let db_clone = db.clone();
  tokio::spawn(async move {
    let ret = do_recover_snapshot(&db_clone, snapshot).await;

    if let Err(e) = ret {
      error!(
        "Failed to recover snapshot from snapshot_id={}: {:?}",
        snapshot_id, e
      );
    } else {
      info!(
        "Snapshot recovery completed successfully for snapshot_id={}",
        snapshot_id
      );
    }
  });

  Ok(())
}

async fn do_recover_snapshot(
  db: &Arc<DB>,
  snapshot: Snapshot<TypeConfig>,
) -> Result<(), io::Error> {
  let snapshot_file = snapshot.snapshot.into_std().await;

  let cf_handle = db.cf_handle(SM_DATA_FAMILY).ok_or_else(|| {
    io::Error::new(
      ErrorKind::NotFound,
      format!("Column family {} not found", SM_DATA_FAMILY),
    )
  })?;

  let mut decoder =
    zstd::Decoder::new(snapshot_file).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

  let mut batch = rocksdb::WriteBatch::default();
  let mut count = 0u64;
  let mut len_buf = [0u8; 4];

  loop {
    match decoder.read_exact(&mut len_buf) {
      Ok(_) => {}
      Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
      Err(e) => return Err(e),
    }
    let key_len = u32::from_le_bytes(len_buf) as usize;

    let mut key = vec![0u8; key_len];
    decoder.read_exact(&mut key)?;

    decoder.read_exact(&mut len_buf)?;
    let value_len = u32::from_le_bytes(len_buf) as usize;

    let mut value = vec![0u8; value_len];
    decoder.read_exact(&mut value)?;

    batch.put_cf(&cf_handle, key, value);
    count += 1;

    if count.is_multiple_of(1000) {
      db.write(batch)
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
      batch = rocksdb::WriteBatch::default();
      info!("[metadata] Recovered {} entries so far...", count);
    }
  }

  if !batch.is_empty() {
    db.write(batch)
      .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
  }

  info!("Successfully recovered {} entries from snapshot", count);

  Ok(())
}
