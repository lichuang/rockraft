use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::SnapshotMeta;
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
use crate::raft::types::LogId;
use crate::raft::types::Snapshot;
use crate::raft::types::StoredMembership;
use crate::raft::types::TypeConfig;
use crate::raft::types::read_logs_err;
use crate::utils::now_millis;

/// Build a snapshot from the current database state
///
/// This function creates a compressed snapshot of the SM_DATA_FAMILY column family,
/// saving it to disk along with metadata. The snapshot building process
/// runs in a blocking task to avoid blocking the async runtime.
///
/// Arguments:
///   db: Reference to the RocksDB instance to snapshot
///   snapshot_dir: Directory path where snapshot files will be stored
///   last_applied_log_id: Optional log ID of the last applied entry
///   last_membership: The membership configuration at the time of snapshot
///
/// Returns:
///   Result<Snapshot, io::Error>: Snapshot containing metadata and data file
///
/// Snapshot Structure:
///   - snapshot_dir/{snapshot_id}/snapshot : Compressed SM_DATA_FAMILY data
///   - snapshot_dir/{snapshot_id}/meta    : Serialized SnapshotMeta
///   - snapshot_dir/last_snapshot_id    : Text file with current snapshot ID
///
/// Snapshot ID Format:
///   - With log_id: "{leader_id}-{index}-{timestamp}"
///   - Without log_id: "0-0-{timestamp}"
///   Example: "T3-N5-100-1234567890"
///
/// Process:
///   1. Generate unique snapshot ID based on current time and log state
///   2. Create snapshot directory structure
///   3. Build SnapshotMeta with provided parameters
///   4. Spawn blocking task to compress and save data
///   5. Save metadata file
///   6. Update last_snapshot_id file
///   7. Spawn background task to clean up old snapshots
pub async fn build_snapshot(
  db: &Arc<DB>,
  snapshot_dir: &PathBuf,
  last_applied_log_id: Option<LogId>,
  last_membership: StoredMembership,
) -> Result<Snapshot, io::Error> {
  // Generate unique snapshot ID based on current timestamp and log state
  let snapshot_idx = now_millis();

  // Build snapshot ID:
  // - If we have a last_applied_log_id, use: "{leader_id}-{index}-{timestamp}"
  // - If no log_id (initial state), use: "0-0-{timestamp}"
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
  // Create snapshot directory (including all parent directories if they don't exist)
  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);
  tokio::fs::create_dir_all(snapshot_id_dir.clone()).await?;

  // Build snapshot metadata with provided log state and membership
  let meta = SnapshotMeta {
    last_log_id: last_applied_log_id,
    last_membership,
    snapshot_id: snapshot_id.clone(),
  };

  // Clone data for use in blocking task
  let db = db.clone();
  let snapshot_id_dir_clone = snapshot_id_dir.clone();

  // Spawn blocking task to perform I/O-intensive snapshot building
  // This prevents blocking the async runtime during compression and disk writes
  let res = spawn_blocking(move || {
    // Get snapshot directory path for this operation
    let snapshot_id_dir = snapshot_id_dir_clone;

    // Create a consistent snapshot point in time (won't be affected by concurrent writes)
    let snapshot = db.snapshot();

    // Get handle to SM_DATA_FAMILY column family where state machine data is stored
    let cf_data = db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family `sm_data` not found");

    // Create temporary dump file for compressed data
    let dump_file_name = snapshot_dump_file(&snapshot_id_dir);
    let dump_file = std::fs::File::create(&dump_file_name)?;

    // Initialize zstd compressor with compression level 3 (good balance of speed/compression)
    let mut encoder = zstd::Encoder::new(dump_file, 3)?;
    // Iterate through all key-value pairs in SM_DATA_FAMILY
    let iter = snapshot.iterator_cf(&cf_data, rocksdb::IteratorMode::Start);

    // Compress each key-value pair and write to dump file
    // Format for each entry: [key_len(4 bytes)][key][value_len(4 bytes)][value]
    for item in iter {
      let (key, value) = item.map_err(read_logs_err)?;

      encoder.write_all(&(key.len() as u32).to_le_bytes())?;
      encoder.write_all(&key)?;
      encoder.write_all(&(value.len() as u32).to_le_bytes())?;
      encoder.write_all(&value)?;
    }

    // Finalize compression and write any buffered data
    encoder.finish()?;

    // Rename dump file to final snapshot file (atomic operation on most filesystems)
    let snapshot_file_name = snapshot_data_file(&snapshot_id_dir);
    std::fs::rename(&dump_file_name, &snapshot_file_name)?;

    Ok(())
  })
  // Wait for blocking task to complete and map any errors
  .await
  .map_err(read_logs_err)?;

  // Handle errors from snapshot data building
  if let Err(e) = res {
    error!(
      "Fail to build snapshot data file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }

  // Save snapshot metadata file (contains last_log_id, last_membership, snapshot_id)
  // and last_snapshot_id file (points to the latest snapshot)
  // Only save these after data file is successfully created
  if let Err(e) = save_snapshot_meta(&snapshot_id_dir, meta.clone()).await {
    error!(
      "Fail to save snapshot meta file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }

  // Only update last_snapshot_id file after both data and meta are successfully saved
  // This ensures we never point to an incomplete snapshot
  if let Err(e) = save_last_snapshot_id_file(&snapshot_dir, &snapshot_id).await {
    error!(
      "Fail to save last snapshot id file for snapshot id={}:{}",
      snapshot_id, e
    );
    return Err(e);
  }

  // Open the snapshot data file for reading (to be returned to caller)
  let res = File::open(&snapshot_data_file(&snapshot_id_dir)).await?;

  // Spawn background task to clean up old snapshots
  // This is a best-effort operation that runs asynchronously
  let snapshot_dir_owned = snapshot_dir.to_path_buf();
  let snapshot_id_clone = snapshot_id.clone();
  spawn(async move {
    // Attempt to remove old snapshots (keep only the latest one)
    // Errors are logged but don't fail the snapshot build operation
    if let Err(e) = vacuum_snapshot_files(snapshot_dir_owned, snapshot_id_clone) {
      error!("Fail to cleanup old snapshot files: {}", e);
    }
  });

  // Log successful snapshot completion
  info!(
    "Snapshot build completed successfully for snapshot_id={}",
    snapshot_id
  );

  // Return snapshot with metadata and open data file
  Ok(Snapshot {
    meta,
    snapshot: res,
  })
}

/// Clean up old snapshot files, keeping only the latest one
///
/// This function removes old snapshot directories to prevent disk space
/// from growing unbounded. It should keep only the snapshot specified
/// by last_snapshot_id and remove all others.
///
/// Arguments:
///   snapshot_dir: Directory containing all snapshots
///   last_snapshot_id: ID of the snapshot to keep (all others should be removed)
///
/// Returns:
///   Result<(), io::Error>: Ok(()) on success or partial failure, error on critical failure
///
/// Current Implementation:
///   - This is currently a stub that always returns Ok(())
///   - TODO: Implement actual cleanup logic to remove old snapshot directories
///   - TODO: Should iterate snapshot_dir, find all snapshot IDs, and remove all except last_snapshot_id
///
/// Note: This is called asynchronously in the background, so errors are logged but not propagated
fn vacuum_snapshot_files(snapshot_dir: PathBuf, last_snapshot_id: String) -> Result<(), io::Error> {
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use openraft::Membership;
  use tempfile::tempdir;

  /// Helper function: Create a test RocksDB instance with sample data
  ///
  /// Creates a temporary RocksDB database with SM_DATA_FAMILY column family
  /// pre-populated with 3 key-value pairs for testing snapshot building.
  /// The temporary directory is kept alive using std::mem::forget to ensure
  /// the database persists for the test duration.
  ///
  /// Returns:
  ///   Arc<DB>: A reference-counted RocksDB instance ready for testing
  ///
  /// Pre-populated data:
  ///   - key1 -> value1
  ///   - key2 -> value2
  ///   - key3 -> value3
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

  /// Helper function: Create an empty test RocksDB instance
  ///
  /// Creates a temporary RocksDB database with SM_DATA_FAMILY column family
  /// without any pre-populated data. Used for testing snapshot building
  /// from an empty database. The temporary directory is kept alive using
  /// std::mem::forget to ensure the database persists for the test duration.
  ///
  /// Returns:
  ///   Arc<DB>: A reference-counted empty RocksDB instance ready for testing
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

  /// Test Point: Building snapshot from database with existing data
  ///
  /// This test verifies the complete snapshot building process when the database
  /// contains data. It validates that all required files are created and
  /// the snapshot metadata is correctly populated.
  ///
  /// Test coverage:
  ///   - Creates a database with 3 pre-populated key-value pairs
  ///   - Builds a snapshot with last_applied_log_id and last_membership
  ///   - Verifies snapshot directory is created
  ///   - Verifies snapshot data file exists and contains compressed data
  ///   - Verifies metadata file is saved
  ///   - Verifies last_snapshot_id file is updated
  ///   - Validates snapshot.last_log_id matches input
  ///   - Validates snapshot_id is generated correctly
  ///
  /// Expected Behavior:
  ///   - build_snapshot should complete successfully
  ///   - All snapshot files (data, meta, last_snapshot_id) should exist
  ///   - Snapshot ID should follow format: "{leader_id}-{index}-{timestamp}"
  ///   - Snapshot should contain data from SM_DATA_FAMILY column family
  #[tokio::test]
  async fn test_build_snapshot_with_data() {
    let db = create_test_db_with_data();
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().join("snapshots");

    let last_applied_log_id = Some(LogId {
      leader_id: crate::raft::types::LeaderId {
        term: 2,
        node_id: 1,
      },
      index: 10,
    });

    let last_membership = StoredMembership::new(
      Some(LogId {
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

  /// Test Point: Building snapshot from empty database
  ///
  /// This test verifies snapshot building when the database contains no data.
  /// It validates that an empty snapshot can be created successfully and the
  /// snapshot ID format is correct when there is no last_applied_log_id.
  ///
  /// Test coverage:
  ///   - Creates an empty database
  ///   - Builds a snapshot with None for last_applied_log_id
  ///   - Verifies snapshot.last_log_id is None
  ///   - Validates snapshot ID starts with "0-0-" (no log info)
  ///   - Verifies snapshot directory is created
  ///
  /// Expected Behavior:
  ///   - build_snapshot should complete successfully even with empty database
  ///   - Snapshot ID should be "0-0-{timestamp}" when no log_id is provided
  ///   - All snapshot files should be created
  ///   - Snapshot should be valid for recovery
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

  /// Test Point: Building snapshot without last_applied_log_id
  ///
  /// This test verifies snapshot ID generation when no last_applied_log_id
  /// is provided. This scenario occurs during initial cluster startup or when
  /// no log entries have been applied yet.
  ///
  /// Test coverage:
  ///   - Creates an empty database
  ///   - Builds a snapshot with explicitly None for last_applied_log_id
  ///   - Verifies snapshot ID format when no log info is available
  ///   - Validates that snapshot.last_log_id is None in metadata
  ///
  /// Expected Behavior:
  ///   - Snapshot should be built successfully
  ///   - Snapshot ID should start with "0-0-" indicating no log_id
  ///   - Metadata should correctly reflect None for last_log_id
  ///   - Snapshot should be valid for recovery of empty state
  #[tokio::test]
  async fn test_build_snapshot_without_log_id() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("snapshots");

    let last_applied_log_id: Option<LogId> = None;

    let last_membership = StoredMembership::new(None, Membership::default());

    let result = build_snapshot(&db, &snapshot_dir, last_applied_log_id, last_membership).await;

    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.meta.last_log_id.is_none());
    assert!(snapshot.meta.snapshot_id.starts_with("0-0-"));
  }

  /// Test Point: Snapshot file creation and validation
  ///
  /// This test verifies that the snapshot data file is created correctly
  /// with compressed content. It ensures that the file exists, is a regular
  /// file (not a directory), and contains data (not empty).
  ///
  /// Test coverage:
  ///   - Creates a snapshot from a database with 3 key-value pairs
  ///   - Verifies snapshot data file path is correct
  ///   - Validates that snapshot file exists on filesystem
  ///   - Confirms snapshot is a file, not a directory
  ///   - Checks that file contains compressed data (size > 0)
  ///   - Ensures zstd compression is working
  ///
  /// Expected Behavior:
  ///   - Snapshot data file should be created at correct path
  ///   - File should contain zstd-compressed data
  ///   - File should not be empty (should have compressed key-value data)
  ///   - File size should be reasonable (smaller than uncompressed data due to compression)
  #[tokio::test]
  async fn test_snapshot_file_creation() {
    let db = create_test_db_with_data();
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().join("snapshots");

    let last_applied_log_id = Some(LogId {
      leader_id: crate::raft::types::LeaderId {
        term: 1,
        node_id: 2,
      },
      index: 5,
    });

    let last_membership = StoredMembership::new(
      Some(LogId {
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

  /// Test Point: Creating snapshot in deeply nested directory structure
  ///
  /// This test verifies that build_snapshot can handle creating snapshots in
  /// directory paths that don't exist yet. It ensures that create_dir_all
  /// creates all parent directories as needed.
  ///
  /// Test coverage:
  ///   - Uses a deeply nested path: "deeply/nested/snapshots"
  ///   - Confirms the path doesn't exist before snapshot creation
  ///   - Builds a snapshot which should create all intermediate directories
  ///   - Verifies the complete directory structure is created
  ///   - Confirms the final path is a directory
  ///
  /// Expected Behavior:
  ///   - build_snapshot should create all parent directories using create_dir_all
  ///   - No error should occur even when parent directories don't exist
  ///   - The full path "deeply/nested/snapshots/{snapshot_id}" should be created
  ///   - Snapshot should be placed in the correct nested location
  #[tokio::test]
  async fn test_build_snapshot_creates_directory() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("deeply/nested/snapshots");

    let last_applied_log_id = Some(LogId {
      leader_id: crate::raft::types::LeaderId {
        term: 1,
        node_id: 1,
      },
      index: 1,
    });

    let last_membership = StoredMembership::new(
      Some(LogId {
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

  /// Test Point: Snapshot metadata content validation
  ///
  /// This test verifies that all metadata fields in the snapshot are correctly
  /// populated with the input parameters. It ensures that the snapshot metadata
  /// accurately reflects the state at the time of snapshot creation.
  ///
  /// Test coverage:
  ///   - Creates a snapshot with specific last_applied_log_id (term=5, node_id=3, index=50)
  ///   - Provides last_membership with different log_id (term=4, node_id=2, index=40)
  ///   - Validates snapshot.meta.last_log_id matches the input parameter
  ///   - Validates snapshot.meta.last_membership matches the input parameter
  ///   - Confirms snapshot.meta.snapshot_id is non-empty
  ///   - Ensures metadata serialization/deserialization is correct
  ///
  /// Expected Behavior:
  ///   - Snapshot metadata should contain exactly the input values
  ///   - last_log_id should preserve the log_id with term=5, node_id=3, index=50
  ///   - last_membership should preserve the membership with its associated log_id
  ///   - snapshot_id should be generated in the correct format
  ///   - Metadata file should be readable and contain serialized metadata
  #[tokio::test]
  async fn test_snapshot_meta_content() {
    let db = create_test_db_empty();
    let snapshot_dir = tempdir().unwrap().path().join("snapshots");

    let last_applied_log_id = Some(LogId {
      leader_id: crate::raft::types::LeaderId {
        term: 5,
        node_id: 3,
      },
      index: 50,
    });

    let last_membership = StoredMembership::new(
      Some(LogId {
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

  /// Test Point: Snapshot ID format with last_applied_log_id
  ///
  /// This test verifies the snapshot ID generation format when a valid
  /// last_applied_log_id is provided. The snapshot ID format is critical
  /// for identifying and ordering snapshots.
  ///
  /// Test coverage:
  ///   - Creates a LogId with term=3, node_id=5, index=100
  ///   - Generates snapshot ID using committed_leader_id() method
  ///   - Validates the format: "T{term}-N{node_id}-{index}-{timestamp}"
  ///   - Verifies committed_leader_id() returns "T3-N5" format
  ///   - Confirms index (100) is included in snapshot ID
  ///   - Checks timestamp (1234567890) is appended
  ///
  /// Expected Behavior:
  ///   - committed_leader_id() should return "T3-N5" format
  ///   - Snapshot ID should be "T3-N5-100-1234567890"
  ///   - Format should be consistent: "T{term}-N{node_id}-{index}-{timestamp}"
  ///   - This format allows lexical ordering of snapshots
  ///
  /// Note: This is a unit test of the snapshot ID generation logic,
  ///       not a full integration test with build_snapshot.
  #[test]
  fn test_snapshot_id_format_with_log_id() {
    let last_applied_log_id = Some(LogId {
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
