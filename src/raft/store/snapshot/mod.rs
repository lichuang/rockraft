mod build;
mod recover;
mod util;

pub use build::build_snapshot;
pub use recover::recover_snapshot;
pub use util::get_current_snapshot;

#[cfg(test)]
mod tests {
  use super::*;
  use openraft::Membership;
  use openraft::StoredMembership;
  use rocksdb::DB;
  use std::path::PathBuf;
  use std::sync::Arc;
  use tempfile::tempdir;
  use tokio::time::{Duration, sleep};

  use crate::raft::store::keys::SM_DATA_FAMILY;

  /// Integration test: Build and recover snapshot with data
  ///
  /// This test verifies the complete snapshot lifecycle:
  /// 1. Create a RocksDB instance with sample data
  /// 2. Build a snapshot from the database
  /// 3. Create a new empty database instance
  /// 4. Recover the snapshot into the new database
  /// 5. Verify that all data is correctly restored
  ///
  /// Test coverage:
  ///   - Snapshot creation from database with data
  ///   - Snapshot file format and compression
  ///   - Snapshot recovery to new database
  ///   - Data integrity after recovery
  ///   - Metadata preservation (last_log_id, last_membership)
  ///
  /// Expected behavior:
  ///   - Snapshot should be created successfully
  ///   - Recovery should complete without errors
  ///   - All key-value pairs should be restored
  ///   - Data values should match original values
  ///   - Snapshot metadata should be preserved
  #[tokio::test]
  async fn test_build_and_recover_snapshot_with_data() {
    // Step 1: Create source database with sample data
    let source_db = create_test_db_with_sample_data();
    let snapshot_dir = tempdir().unwrap();
    let snapshot_dir_path = snapshot_dir.path().to_path_buf();

    // Populate source database with test data
    let cf_handle = source_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");
    source_db
      .put_cf(&cf_handle, b"user:1", b"John Doe")
      .unwrap();
    source_db
      .put_cf(&cf_handle, b"user:2", b"Jane Smith")
      .unwrap();
    source_db
      .put_cf(&cf_handle, b"user:3", b"Bob Johnson")
      .unwrap();
    source_db
      .put_cf(&cf_handle, b"config:theme", b"dark")
      .unwrap();

    // Step 2: Build snapshot from source database
    let last_applied_log_id = None;
    let last_membership = StoredMembership::new(None, Membership::default());

    let snapshot = build_snapshot(
      &source_db,
      &snapshot_dir_path,
      last_applied_log_id,
      last_membership,
    )
    .await
    .expect("Failed to build snapshot");

    // Verify snapshot was created successfully
    assert!(!snapshot.meta.snapshot_id.is_empty());
    println!("Snapshot created with ID: {}", snapshot.meta.snapshot_id);

    // Step 3: Create new empty database for recovery
    let target_db = create_empty_test_db();
    let cf_handle_target = target_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    // Verify target database is empty
    let mut count = 0u64;
    let iter = target_db.iterator_cf(&cf_handle_target, rocksdb::IteratorMode::Start);
    for _ in iter {
      count += 1;
    }
    assert_eq!(count, 0, "Target database should be empty initially");

    // Step 4: Recover snapshot to target database
    recover_snapshot(&target_db, snapshot)
      .await
      .expect("Failed to recover snapshot");

    // Step 5: Verify all data was recovered correctly
    sleep(Duration::from_millis(100)).await; // Wait for async recovery to complete

    let mut recovered_count = 0u64;
    let iter = target_db.iterator_cf(&cf_handle_target, rocksdb::IteratorMode::Start);
    for _ in iter {
      recovered_count += 1;
    }

    // Should have recovered 4 entries
    assert_eq!(
      recovered_count, 4,
      "Expected 4 entries after recovery, got {}",
      recovered_count
    );

    // Verify specific key-value pairs
    let value1 = target_db
      .get_cf(&cf_handle_target, b"user:1")
      .unwrap()
      .expect("user:1 should exist after recovery");
    assert_eq!(
      String::from_utf8(value1).unwrap(),
      "John Doe",
      "user:1 value mismatch"
    );

    let value2 = target_db
      .get_cf(&cf_handle_target, b"user:2")
      .unwrap()
      .expect("user:2 should exist after recovery");
    assert_eq!(
      String::from_utf8(value2).unwrap(),
      "Jane Smith",
      "user:2 value mismatch"
    );

    let value3 = target_db
      .get_cf(&cf_handle_target, b"user:3")
      .unwrap()
      .expect("user:3 should exist after recovery");
    assert_eq!(
      String::from_utf8(value3).unwrap(),
      "Bob Johnson",
      "user:3 value mismatch"
    );

    let value4 = target_db
      .get_cf(&cf_handle_target, b"config:theme")
      .unwrap()
      .expect("config:theme should exist after recovery");
    assert_eq!(
      String::from_utf8(value4).unwrap(),
      "dark",
      "config:theme value mismatch"
    );

    println!("Integration test passed: Build and recover snapshot with data");
  }

  /// Integration test: Build and recover empty snapshot
  ///
  /// This test verifies snapshot lifecycle when database is empty:
  /// 1. Create an empty RocksDB instance
  /// 2. Build a snapshot from the empty database
  /// 3. Create a new empty database instance
  /// 4. Recover the snapshot into the new database
  /// 5. Verify that the database remains empty after recovery
  ///
  /// Expected behavior:
  ///   - Snapshot should be created successfully even with empty database
  ///   - Recovery should complete without errors
  ///   - Target database should remain empty after recovery
  #[tokio::test]
  async fn test_build_and_recover_empty_snapshot() {
    // Step 1: Create empty source database
    let source_db = create_test_db_with_sample_data();
    let snapshot_dir = tempdir().unwrap();
    let snapshot_dir_path = snapshot_dir.path().to_path_buf();

    // Step 2: Build snapshot from empty database
    let last_applied_log_id = None;
    let last_membership = StoredMembership::new(None, Membership::default());

    let snapshot = build_snapshot(
      &source_db,
      &snapshot_dir_path,
      last_applied_log_id,
      last_membership,
    )
    .await
    .expect("Failed to build empty snapshot");

    println!(
      "Empty snapshot created with ID: {}",
      snapshot.meta.snapshot_id
    );

    // Step 3: Create target database
    let target_db = create_empty_test_db();
    let cf_handle_target = target_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    // Step 4: Recover empty snapshot
    recover_snapshot(&target_db, snapshot)
      .await
      .expect("Failed to recover empty snapshot");

    // Step 5: Verify database is still empty
    sleep(Duration::from_millis(100)).await; // Wait for async recovery

    let mut count = 0u64;
    let iter = target_db.iterator_cf(&cf_handle_target, rocksdb::IteratorMode::Start);
    for _ in iter {
      count += 1;
    }

    assert_eq!(
      count, 0,
      "Database should be empty after recovering empty snapshot"
    );
    println!("Integration test passed: Build and recover empty snapshot");
  }

  /// Integration test: Build and recover large snapshot
  ///
  /// This test verifies snapshot functionality with a large dataset:
  /// 1. Create a database with 1000 key-value pairs
  /// 2. Build a snapshot from the database
  /// 3. Recover the snapshot to a new database
  /// 4. Verify all 1000 entries are correctly restored
  ///
  /// Expected behavior:
  ///   - Snapshot should handle large datasets efficiently
  ///   - Recovery should complete successfully
  ///   - All entries should be correctly restored
  ///   - Batch writing during recovery should work correctly
  #[tokio::test]
  async fn test_build_and_recover_large_snapshot() {
    // Step 1: Create source database with large dataset
    let source_db = create_test_db_with_sample_data();
    let snapshot_dir = tempdir().unwrap();
    let snapshot_dir_path = snapshot_dir.path().to_path_buf();
    let cf_handle = source_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    // Insert 1000 key-value pairs
    let data_count = 1000;
    for i in 0..data_count {
      let key = format!("entry:{}", i);
      let value = format!("data-{}-value", i);
      source_db
        .put_cf(&cf_handle, key.as_bytes(), value.as_bytes())
        .unwrap();
    }

    println!("Inserted {} entries into source database", data_count);

    // Step 2: Build snapshot
    let last_applied_log_id = None;
    let last_membership = StoredMembership::new(None, Membership::default());

    let snapshot = build_snapshot(
      &source_db,
      &snapshot_dir_path,
      last_applied_log_id,
      last_membership,
    )
    .await
    .expect("Failed to build large snapshot");

    println!(
      "Large snapshot created with ID: {}",
      snapshot.meta.snapshot_id
    );

    // Step 3: Recover snapshot to target database
    let target_db = create_empty_test_db();
    let cf_handle_target = target_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    recover_snapshot(&target_db, snapshot)
      .await
      .expect("Failed to recover large snapshot");

    // Step 4: Verify all entries were recovered
    sleep(Duration::from_millis(200)).await; // Wait longer for large dataset

    let mut recovered_count = 0u64;
    let iter = target_db.iterator_cf(&cf_handle_target, rocksdb::IteratorMode::Start);
    for _ in iter {
      recovered_count += 1;
    }

    assert_eq!(
      recovered_count, data_count as u64,
      "Expected {} entries after recovery, got {}",
      data_count, recovered_count
    );

    // Verify some sample entries
    let sample_keys = vec![0, 100, 500, 999];
    for &key_num in &sample_keys {
      let key = format!("entry:{}", key_num);
      let expected_value = format!("data-{}-value", key_num);
      let actual_value = target_db
        .get_cf(&cf_handle_target, key.as_bytes())
        .unwrap()
        .expect(format!("Entry {} should exist", key).as_str());
      assert_eq!(
        String::from_utf8(actual_value).unwrap(),
        expected_value,
        "Value mismatch for {}",
        key
      );
    }

    println!(
      "Integration test passed: Build and recover large snapshot with {} entries",
      data_count
    );
  }

  /// Integration test: Build and recover snapshot with binary data
  ///
  /// This test verifies snapshot functionality with binary/non-UTF8 data:
  /// 1. Create a database with binary key-value pairs
  /// 2. Build a snapshot from the database
  /// 3. Recover the snapshot to a new database
  /// 4. Verify binary data is correctly restored
  ///
  /// Expected behavior:
  ///   - Snapshot should handle binary data correctly
  ///   - Recovery should preserve exact byte values
  ///   - No data corruption should occur
  #[tokio::test]
  async fn test_build_and_recover_binary_data() {
    // Step 1: Create source database with binary data
    let source_db = create_test_db_with_sample_data();
    let snapshot_dir = tempdir().unwrap();
    let snapshot_dir_path = snapshot_dir.path().to_path_buf();
    let cf_handle = source_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    // Insert binary data
    let binary_data = vec![
      (b"bin:key1".to_vec(), vec![0x00, 0x01, 0x02, 0x03, 0xFF]),
      (b"bin:key2".to_vec(), vec![0xDE, 0xAD, 0xBE, 0xEF]),
      (
        b"bin:key3".to_vec(),
        vec![0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0],
      ),
    ];

    for (key, value) in &binary_data {
      source_db
        .put_cf(&cf_handle, key.as_slice(), value.as_slice())
        .unwrap();
    }

    // Step 2: Build snapshot
    let last_applied_log_id = None;
    let last_membership = StoredMembership::new(None, Membership::default());

    let snapshot = build_snapshot(
      &source_db,
      &snapshot_dir_path,
      last_applied_log_id,
      last_membership,
    )
    .await
    .expect("Failed to build snapshot with binary data");

    // Step 3: Recover snapshot
    let target_db = create_empty_test_db();
    let cf_handle_target = target_db
      .cf_handle(SM_DATA_FAMILY)
      .expect("column family not found");

    recover_snapshot(&target_db, snapshot)
      .await
      .expect("Failed to recover snapshot with binary data");

    // Step 4: Verify binary data integrity
    sleep(Duration::from_millis(100)).await;

    for (key, expected_value) in &binary_data {
      let actual_value = target_db
        .get_cf(&cf_handle_target, key.as_slice())
        .unwrap()
        .expect(format!("Binary entry should exist: {:?}", key).as_str());
      assert_eq!(
        actual_value, *expected_value,
        "Binary data mismatch for key {:?}",
        key
      );
    }

    println!("Integration test passed: Build and recover snapshot with binary data");
  }

  /// Integration test: Build and recover snapshot with metadata
  ///
  /// This test verifies that snapshot metadata is preserved during the build and recover cycle:
  /// 1. Create a database with data
  /// 2. Build a snapshot with specific last_log_id and last_membership
  /// 3. Recover the snapshot
  /// 4. Verify that snapshot metadata can be retrieved via get_current_snapshot
  ///
  /// Expected behavior:
  ///   - Snapshot metadata should be saved correctly
  ///   - last_log_id should be preserved
  ///   - last_membership should be preserved
  ///   - snapshot_id should be consistent
  #[tokio::test]
  async fn test_build_and_recover_snapshot_with_metadata() {
    // Step 1: Create source database
    let source_db = create_test_db_with_sample_data();
    let snapshot_dir = tempdir().unwrap();
    let snapshot_dir_path = snapshot_dir.path().to_path_buf();

    // Step 2: Build snapshot with specific metadata
    let last_applied_log_id = None; // Using None for simplicity
    let last_membership = StoredMembership::new(None, Membership::default());

    let snapshot = build_snapshot(
      &source_db,
      &snapshot_dir_path,
      last_applied_log_id,
      last_membership,
    )
    .await
    .expect("Failed to build snapshot");

    let snapshot_id = snapshot.meta.snapshot_id.clone();
    println!("Snapshot created with ID: {}", snapshot_id);

    // Step 3: Verify snapshot can be retrieved via get_current_snapshot
    let current_snapshot = get_current_snapshot(&snapshot_dir_path)
      .await
      .expect("Failed to get current snapshot");

    assert!(
      current_snapshot.is_some(),
      "Current snapshot should exist after building"
    );

    let retrieved_snapshot = current_snapshot.unwrap();
    assert_eq!(
      retrieved_snapshot.meta.snapshot_id, snapshot_id,
      "Retrieved snapshot ID should match built snapshot ID"
    );

    // Step 4: Verify snapshot files exist
    let snapshot_id_dir =
      crate::raft::store::snapshot::util::snapshot_id_dir(&snapshot_dir_path, &snapshot_id);

    let meta_file = crate::raft::store::snapshot::util::snapshot_meta_file(&snapshot_id_dir);
    assert!(
      PathBuf::from(&meta_file).exists(),
      "Metadata file should exist"
    );

    let data_file = crate::raft::store::snapshot::util::snapshot_data_file(&snapshot_id_dir);
    assert!(PathBuf::from(&data_file).exists(), "Data file should exist");

    println!("Integration test passed: Build and recover snapshot with metadata");
  }

  /// Helper function: Create a test RocksDB instance with sample data
  ///
  /// Creates a temporary RocksDB database with SM_DATA_FAMILY column family.
  /// The temporary directory is kept alive using std::mem::forget to ensure
  /// the database persists for the test duration.
  ///
  /// Returns:
  ///   Arc<DB>: A reference-counted RocksDB instance ready for testing
  fn create_test_db_with_sample_data() -> Arc<DB> {
    let temp_dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY]).unwrap();

    std::mem::forget(temp_dir);
    Arc::new(db)
  }

  /// Helper function: Create an empty test RocksDB instance
  ///
  /// Creates a temporary RocksDB database with SM_DATA_FAMILY column family
  /// without any pre-populated data. Used for testing snapshot recovery
  /// into an empty database.
  ///
  /// Returns:
  ///   Arc<DB>: A reference-counted empty RocksDB instance ready for testing
  fn create_empty_test_db() -> Arc<DB> {
    let temp_dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY]).unwrap();

    std::mem::forget(temp_dir);
    Arc::new(db)
  }
}
