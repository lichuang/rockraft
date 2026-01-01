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
use crate::raft::store::keys::SM_DATA_FAMILY;
use crate::raft::store::snapshot::util::snapshot_id_dir;
use crate::raft::types::TypeConfig;
use crate::raft::types::read_logs_err;
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
    let iter = snapshot.iterator_cf(&cf_data, rocksdb::IteratorMode::Start);

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

#[cfg(test)]
mod tests {
  use super::*;
  use openraft::Membership;
  use openraft::RaftTypeConfig;
  use tempfile::tempdir;

  fn create_test_db_with_data() -> Arc<DB> {
    let temp_dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY]).unwrap();

    db.put_cf(&db.cf_handle(SM_DATA_FAMILY).unwrap(), b"key1", b"value1")
      .unwrap();
    db.put_cf(&db.cf_handle(SM_DATA_FAMILY).unwrap(), b"key2", b"value2")
      .unwrap();
    db.put_cf(&db.cf_handle(SM_DATA_FAMILY).unwrap(), b"key3", b"value3")
      .unwrap();

    // Keep temp_dir alive for the test duration
    std::mem::forget(temp_dir);

    Arc::new(db)
  }

  fn create_test_db_empty() -> Arc<DB> {
    let temp_dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY]).unwrap();

    // Keep temp_dir alive for the test duration
    std::mem::forget(temp_dir);

    Arc::new(db)
  }

  #[tokio::test]
  async fn test_build_snapshot_with_data() {
    let db = create_test_db_with_data();
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().join("snapshots");

    let last_applied_log_id = Some(LogId::<TypeConfig> {
      leader_id: crate::raft::types::LeaderId {
        term: 2,
        node_id: 1,
      },
      index: 10,
    });

    let last_membership = StoredMembership::new(
      Some(LogId::<TypeConfig> {
        leader_id: crate::raft::types::LeaderId {
          term: 2,
          node_id: 1,
        },
        index: 5,
      }),
      Membership::default(),
    );

    let result = build_snapshot(
      &db,
      &snapshot_dir,
      last_applied_log_id.clone(),
      last_membership,
    )
    .await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert_eq!(snapshot.meta.last_log_id, last_applied_log_id);
    assert!(!snapshot.meta.snapshot_id.is_empty());

    // Verify snapshot directory was created
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, &snapshot.meta.snapshot_id);
    assert!(snapshot_id_dir.exists());

    // Verify snapshot data file exists
    let snapshot_data = snapshot_data_file(&snapshot_id_dir);
    assert!(std::path::Path::new(&snapshot_data).exists());

    // Verify meta file exists
    let meta_file = snapshot_id_dir.join("meta");
    assert!(meta_file.exists());

    // Verify last_snapshot_id file exists
    let last_id_file = snapshot_dir.join("last_snapshot_id");
    assert!(last_id_file.exists());
  }

  #[tokio::test]
  async fn test_build_snapshot_empty_db() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("snapshots");

    let last_applied_log_id = None;

    let last_membership = StoredMembership::new(None, Membership::default());

    let result = build_snapshot(&db, &snapshot_dir, last_applied_log_id, last_membership).await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.meta.last_log_id.is_none());
    assert!(!snapshot.meta.snapshot_id.is_empty());
    assert!(snapshot.meta.snapshot_id.starts_with("0-0-"));

    // Verify snapshot directory was created
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, &snapshot.meta.snapshot_id);
    assert!(snapshot_id_dir.exists());
  }

  #[tokio::test]
  async fn test_build_snapshot_without_log_id() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("snapshots");

    let last_applied_log_id: Option<LogId<TypeConfig>> = None;

    let last_membership = StoredMembership::new(None, Membership::default());

    let result = build_snapshot(&db, &snapshot_dir, last_applied_log_id, last_membership).await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.meta.last_log_id.is_none());
    assert!(snapshot.meta.snapshot_id.starts_with("0-0-"));
  }

  #[tokio::test]
  async fn test_snapshot_file_creation() {
    let db = create_test_db_with_data();
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().join("snapshots");

    let last_applied_log_id = Some(LogId::<TypeConfig> {
      leader_id: crate::raft::types::LeaderId {
        term: 1,
        node_id: 2,
      },
      index: 5,
    });

    let last_membership = StoredMembership::new(
      Some(LogId::<TypeConfig> {
        leader_id: crate::raft::types::LeaderId {
          term: 1,
          node_id: 2,
        },
        index: 3,
      }),
      Membership::default(),
    );

    let result = build_snapshot(&db, &snapshot_dir, last_applied_log_id, last_membership).await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, &snapshot.meta.snapshot_id);

    // Verify all expected files exist
    let snapshot_file = snapshot_data_file(&snapshot_id_dir);
    let snapshot_path = std::path::Path::new(&snapshot_file);

    assert!(snapshot_path.exists());
    assert!(snapshot_path.is_file());

    // Check file size is reasonable (should have compressed data)
    let metadata = std::fs::metadata(snapshot_path).unwrap();
    assert!(metadata.len() > 0, "Snapshot file should not be empty");
  }

  #[tokio::test]
  async fn test_build_snapshot_creates_directory() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("deeply/nested/snapshots");

    let last_applied_log_id = Some(LogId::<TypeConfig> {
      leader_id: crate::raft::types::LeaderId {
        term: 1,
        node_id: 1,
      },
      index: 1,
    });

    let last_membership = StoredMembership::new(
      Some(LogId::<TypeConfig> {
        leader_id: crate::raft::types::LeaderId {
          term: 1,
          node_id: 1,
        },
        index: 1,
      }),
      Membership::default(),
    );

    // Ensure directory doesn't exist yet
    assert!(!snapshot_dir.exists());

    let result = build_snapshot(&db, &snapshot_dir, last_applied_log_id, last_membership).await;

    assert!(result.is_ok());

    // Verify the nested directory structure was created
    assert!(snapshot_dir.exists());
    assert!(snapshot_dir.is_dir());
  }

  #[tokio::test]
  async fn test_snapshot_meta_content() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("snapshots");

    let last_applied_log_id = Some(LogId::<TypeConfig> {
      leader_id: crate::raft::types::LeaderId {
        term: 5,
        node_id: 3,
      },
      index: 50,
    });

    let last_membership = StoredMembership::new(
      Some(LogId::<TypeConfig> {
        leader_id: crate::raft::types::LeaderId {
          term: 4,
          node_id: 2,
        },
        index: 40,
      }),
      Membership::default(),
    );

    let result = build_snapshot(
      &db,
      &snapshot_dir,
      last_applied_log_id.clone(),
      last_membership.clone(),
    )
    .await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();

    // Verify meta content
    assert_eq!(snapshot.meta.last_log_id, last_applied_log_id);
    assert_eq!(snapshot.meta.last_membership, last_membership);
    assert!(!snapshot.meta.snapshot_id.is_empty());
  }

  #[test]
  fn test_snapshot_id_format_with_log_id() {
    let last_applied_log_id = Some(LogId::<TypeConfig> {
      leader_id: crate::raft::types::LeaderId {
        term: 3,
        node_id: 5,
      },
      index: 100,
    });
    let snapshot_idx = 1234567890;

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

    assert_eq!(snapshot_id, "T3-N5-100-1234567890");
  }
}
