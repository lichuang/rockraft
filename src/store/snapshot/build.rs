use std::io;
use std::sync::Arc;

use bytes::Bytes;
use openraft::LogId;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StoredMembership;
use rand::Rng;
use rocksdb::DB;
use tokio::task::spawn_blocking;

use crate::store::keys::SM_DATA_FAMILY;
use crate::types::KeyValue;
use crate::types::SnapshotData;
use crate::types::TypeConfig;
use crate::types::read_logs_err;

pub async fn build_snapshot(
  db: &Arc<DB>,
  last_applied_log_id: Option<LogId<TypeConfig>>,
  last_membership: StoredMembership<TypeConfig>,
) -> Result<Snapshot<TypeConfig>, io::Error> {
  // Generate a random snapshot index.
  let snapshot_idx: u64 = rand::rng().random_range(0..1000);

  let snapshot_id = if let Some(last) = last_applied_log_id {
    format!(
      "{}-{}-{}",
      last.committed_leader_id(),
      last.index(),
      snapshot_idx
    )
  } else {
    format!("--{}", snapshot_idx)
  };

  let meta = SnapshotMeta {
    last_log_id: last_applied_log_id,
    last_membership,
    snapshot_id: snapshot_id.clone(),
  };

  let db = db.clone();

  let data = spawn_blocking(move || {
    let snapshot = db.snapshot();
    let cf_data = db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family `sm_data` not found");

    let mut snapshot_data = Vec::new();
    let iter = snapshot.iterator_cf(cf_data, rocksdb::IteratorMode::Start);

    for item in iter {
      let (key, value) = item.map_err(read_logs_err)?;
      let kv = KeyValue {
        key: Bytes::from(key.into_vec()),
        value: Bytes::from(value.into_vec()),
      };
      snapshot_data.push(kv);
    }

    Ok(snapshot_data)
  })
  .await
  .map_err(read_logs_err)?;

  match data {
    Err(e) => Err(e),
    Ok(data) => Ok(Snapshot {
      meta,
      snapshot: SnapshotData { data },
    }),
  }
}
