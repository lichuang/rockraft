use std::io::ErrorKind;
use std::io::Read;
use std::io::{self};
use std::sync::Arc;

use rocksdb::DB;
use tracing::error;
use tracing::info;

use crate::raft::store::keys::SM_DATA_FAMILY;
use crate::raft::types::Snapshot;

/// Recover from a snapshot asynchronously
///
/// This function initiates snapshot recovery by spawning a background task.
/// The recovery process runs asynchronously and does not block the caller.
/// Errors during recovery are logged but not propagated to the caller.
///
/// Arguments:
///   db: Reference to the RocksDB instance where data will be recovered
///   snapshot: The snapshot containing data to recover
///
/// Returns:
///   Result<(), io::Error>: Always returns Ok(()) as recovery runs in background
///
/// Behavior:
///   - Spawns a background tokio task for actual recovery
///   - Logs success or failure of the recovery operation
///   - Recovery happens asynchronously; caller does not wait for completion
///
/// Note: TODO: add recover complete callback for notification when recovery finishes
pub async fn recover_snapshot(db: &Arc<DB>, snapshot: Snapshot) -> Result<(), io::Error> {
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

/// Internal function: Perform the actual snapshot recovery
///
/// This function handles the core recovery logic:
/// - Reads and decompresses snapshot data
/// - Restores key-value pairs to the database in batches
/// - Uses WriteBatch for efficient bulk writes (every 1000 entries)
///
/// Arguments:
///   db: Reference to the RocksDB instance for data recovery
///   snapshot: The snapshot containing compressed data to recover
///
/// Returns:
///   Result<(), io::Error>: Ok(()) on success, error on failure
///
/// Recovery Process:
///   1. Convert tokio::File to std::fs::File for zstd decoder
///   2. Decompress snapshot data using zstd
///   3. Read entries in format: [key_len][key][value_len][value]
///   4. Accumulate entries in WriteBatch (max 1000 per batch)
///   5. Write batch to database when batch is full
///   6. Write any remaining entries in final batch
///
/// Performance:
///   - Batch writing reduces database I/O operations
///   - Compression reduces disk I/O for snapshot files
///   - Streaming approach minimizes memory usage
async fn do_recover_snapshot(db: &Arc<DB>, snapshot: Snapshot) -> Result<(), io::Error> {
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

#[cfg(test)]
mod tests {
  use super::*;
  use openraft::Membership;
  use openraft::SnapshotMeta;
  use openraft::StoredMembership;
  use tempfile::tempdir;

  /// Helper function: Create a test RocksDB instance
  ///
  /// Creates a temporary RocksDB database with the SM_DATA_FAMILY column family
  /// for testing purposes. The temporary directory is kept alive using
  /// std::mem::forget to ensure the database persists for the test duration.
  ///
  /// Returns:
  ///   Arc<DB>: A reference-counted RocksDB instance ready for testing
  fn create_test_db() -> Arc<DB> {
    let temp_dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY]).unwrap();

    std::mem::forget(temp_dir);
    Arc::new(db)
  }

  /// Helper function: Create a test snapshot file with specified data count
  ///
  /// Creates a compressed snapshot file using zstd compression format.
  /// Each entry in the snapshot follows the format:
  /// - key length (4 bytes, little-endian u32)
  /// - key bytes
  /// - value length (4 bytes, little-endian u32)
  /// - value bytes
  ///
  /// Arguments:
  ///   path: The file path where the snapshot should be created
  ///   data_count: Number of key-value pairs to include in the snapshot
  ///
  /// Behavior:
  ///   - Creates a compressed snapshot file at the specified path
  ///   - Generates sequential keys (key-0, key-1, ...) and values (value-0, value-1, ...)
  ///   - Uses zstd compression level 3
  async fn create_test_snapshot_file(path: &str, data_count: u32) {
    use std::io::Write;

    let file = std::fs::File::create(path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();

    for i in 0..data_count {
      let key = format!("key-{}", i);
      let value = format!("value-{}", i);

      encoder
        .write_all(&(key.len() as u32).to_le_bytes())
        .unwrap();
      encoder.write_all(key.as_bytes()).unwrap();
      encoder
        .write_all(&(value.len() as u32).to_le_bytes())
        .unwrap();
      encoder.write_all(value.as_bytes()).unwrap();
    }

    encoder.finish().unwrap();
  }

  /// Helper function: Count the number of entries in the database
  ///
  /// Iterates through all key-value pairs in the SM_DATA_FAMILY column family
  /// and returns the total count of entries.
  ///
  /// Arguments:
  ///   db: Reference to the RocksDB instance
  ///
  /// Returns:
  ///   u64: The total number of key-value pairs in the SM_DATA_FAMILY column family
  ///
  /// Note: This is a synchronous operation that may be slow for large databases.
  /// Consider using for testing purposes only.
  fn count_db_entries(db: &Arc<DB>) -> u64 {
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();
    let mut count = 0u64;
    let iter = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
    for _ in iter {
      count += 1;
    }
    count
  }

  /// Test Point: Basic snapshot recovery functionality
  ///
  /// This test verifies the fundamental snapshot recovery operation:
  /// - Creates a snapshot file with 3 key-value pairs
  /// - Recovers the snapshot into the database
  /// - Verifies the correct number of entries (3) are recovered
  /// - Validates the content of each recovered key-value pair
  ///
  /// Expected Behavior:
  /// - Snapshot recovery should complete successfully
  /// - All 3 entries should be present in the database
  /// - Each recovered value should match the original data
  #[tokio::test]
  async fn test_recover_snapshot_basic() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("test.snapshot");

    // Create a test snapshot file with 3 key-value pairs
    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 3).await;

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("test-snapshot-1"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot(&db, snapshot).await;

    assert!(result.is_ok());

    // Verify the correct number of entries were recovered
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 3,
      "Expected 3 entries in database after recovery"
    );

    // Verify data was recovered correctly
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    let value1 = db.get_cf(&cf_handle, b"key-0").unwrap().unwrap();
    assert_eq!(String::from_utf8(value1).unwrap(), "value-0");

    let value2 = db.get_cf(&cf_handle, b"key-1").unwrap().unwrap();
    assert_eq!(String::from_utf8(value2).unwrap(), "value-1");

    let value3 = db.get_cf(&cf_handle, b"key-2").unwrap().unwrap();
    assert_eq!(String::from_utf8(value3).unwrap(), "value-2");
  }

  /// Test Point: Empty snapshot recovery
  ///
  /// This test verifies handling of empty snapshots:
  /// - Creates a snapshot file with 0 key-value pairs
  /// - Attempts to recover the empty snapshot
  /// - Verifies the database remains empty after recovery
  ///
  /// Expected Behavior:
  /// - Recovery should complete successfully without errors
  /// - Database should contain 0 entries after recovery
  /// - No data should be corrupted or created erroneously
  #[tokio::test]
  async fn test_recover_snapshot_empty() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("empty.snapshot");

    // Create an empty snapshot file
    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 0).await;

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("empty-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot(&db, snapshot).await;

    assert!(result.is_ok());

    // Verify the database is empty after recovering an empty snapshot
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 0,
      "Expected 0 entries in database after recovering empty snapshot"
    );
  }

  /// Test Point: Batch writing mechanism for large snapshots
  ///
  /// This test verifies the batch writing optimization:
  /// - Creates a snapshot with 1500 entries (exceeds batch size of 1000)
  /// - Tests that data is written in batches (every 1000 entries)
  /// - Validates all data is recovered correctly across multiple batches
  /// - Checks entries from first batch (0), middle batch (999), and last batch (1499)
  ///
  /// Expected Behavior:
  /// - Recovery should complete successfully
  /// - All 1500 entries should be present in the database
  /// - Batch writing mechanism should not lose any data
  /// - Data from different batches should be consistent
  #[tokio::test]
  async fn test_recover_snapshot_batch_writing() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("batch.snapshot");

    // Create a snapshot file with 1500 entries (more than one batch)
    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 1500).await;

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("batch-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot(&db, snapshot).await;

    assert!(result.is_ok());

    // Verify the correct number of entries were recovered (1500)
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 1500,
      "Expected 1500 entries in database after recovery"
    );

    // Verify multiple entries were recovered
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    let value_first = db.get_cf(&cf_handle, b"key-0").unwrap().unwrap();
    assert_eq!(String::from_utf8(value_first).unwrap(), "value-0");

    let value_mid = db.get_cf(&cf_handle, b"key-999").unwrap().unwrap();
    assert_eq!(String::from_utf8(value_mid).unwrap(), "value-999");

    let value_last = db.get_cf(&cf_handle, b"key-1499").unwrap().unwrap();
    assert_eq!(String::from_utf8(value_last).unwrap(), "value-1499");
  }

  /// Test Point: Asynchronous snapshot recovery function
  ///
  /// This test verifies the async recover_snapshot function:
  /// - Creates a snapshot with 5 key-value pairs
  /// - Calls recover_snapshot which spawns a background task
  /// - Waits for the background task to complete
  /// - Verifies the data is recovered asynchronously
  ///
  /// Expected Behavior:
  /// - recover_snapshot should return immediately (not wait for completion)
  /// - Background task should complete successfully
  /// - All 5 entries should be present in the database after task completes
  /// - Async operation should not interfere with data integrity
  #[tokio::test]
  async fn test_recover_snapshot_function() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("async.snapshot");

    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 5).await;

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("async-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    // recover_snapshot spawns a task, so we need to wait a bit
    let result = recover_snapshot(&db, snapshot).await;
    assert!(result.is_ok());

    // Give spawned task time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify the correct number of entries were recovered (5)
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 5,
      "Expected 5 entries in database after async recovery"
    );

    // Verify data was recovered
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();
    let value = db.get_cf(&cf_handle, b"key-2").unwrap().unwrap();
    assert_eq!(String::from_utf8(value).unwrap(), "value-2");
  }

  /// Test Point: Large data recovery with compression/decompression
  ///
  /// This test verifies handling of large data values:
  /// - Creates a snapshot with 10 entries, each containing 1KB values
  /// - Tests zstd compression/decompression with large buffers
  /// - Verifies memory handling for large value recovery
  /// - Validates data integrity after decompression
  ///
  /// Expected Behavior:
  /// - Recovery should complete successfully
  /// - All 10 entries should be present in the database
  /// - Large values (1KB each) should be recovered without corruption
  /// - Decompression should handle large buffers correctly
  #[tokio::test]
  async fn test_recover_snapshot_large_data() {
    use std::io::Write;

    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("large.snapshot");

    // Create a snapshot file with larger values
    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();

    let large_value = "x".repeat(1024); // 1KB value
    for i in 0..10 {
      let key = format!("large-key-{}", i);
      encoder
        .write_all(&(key.len() as u32).to_le_bytes())
        .unwrap();
      encoder.write_all(key.as_bytes()).unwrap();
      encoder
        .write_all(&(large_value.len() as u32).to_le_bytes())
        .unwrap();
      encoder.write_all(large_value.as_bytes()).unwrap();
    }

    encoder.finish().unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("large-data-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot(&db, snapshot).await;

    assert!(result.is_ok());

    // Verify the correct number of entries were recovered (10)
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 10,
      "Expected 10 entries in database after recovering large data"
    );

    // Verify large data was recovered
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();
    let value = db.get_cf(&cf_handle, b"large-key-5").unwrap().unwrap();
    assert_eq!(String::from_utf8(value).unwrap(), large_value);
  }

  /// Test Point: Data overwrite behavior during recovery
  ///
  /// This test verifies snapshot recovery overwrites existing data:
  /// - Pre-populates the database with 2 key-value pairs
  /// - Creates a snapshot with 3 entries (including keys that already exist)
  /// - Verifies that snapshot data overwrites existing database entries
  /// - Confirms that non-overlapping data from snapshot is added
  ///
  /// Expected Behavior:
  /// - Recovery should complete successfully
  /// - Database should contain exactly 3 entries (from snapshot, not pre-existing)
  /// - Existing values for key-0 and key-1 should be overwritten
  /// - New value for key-2 should be added
  /// - Only snapshot data should remain, pre-existing data should be replaced
  #[tokio::test]
  async fn test_recover_snapshot_overwrites() {
    let db = create_test_db();
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    // Pre-populate the database
    db.put_cf(&cf_handle, b"key-0", b"old-value").unwrap();
    db.put_cf(&cf_handle, b"key-1", b"old-value-2").unwrap();

    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("overwrite.snapshot");

    // Create snapshot with same keys but different values
    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 3).await;

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("overwrite-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot(&db, snapshot).await;

    assert!(result.is_ok());

    // Verify the correct number of entries were recovered (3)
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 3,
      "Expected 3 entries in database after overwrite recovery"
    );

    // Verify values were overwritten
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();
    let value0 = db.get_cf(&cf_handle, b"key-0").unwrap().unwrap();
    assert_eq!(String::from_utf8(value0).unwrap(), "value-0");

    let value1 = db.get_cf(&cf_handle, b"key-1").unwrap().unwrap();
    assert_eq!(String::from_utf8(value1).unwrap(), "value-1");
  }
}
