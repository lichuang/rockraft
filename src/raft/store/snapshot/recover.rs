use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::io::Read;
use std::io::{self};
use std::sync::Arc;

use rocksdb::DB;
use tokio::task::spawn_blocking;
use tracing::info;

use crate::config::DEFAULT_GRPC_MAX_MESSAGE_SIZE;
use crate::raft::store::keys::LAST_APPLIED_LOG_KEY;
use crate::raft::store::keys::LAST_MEMBERSHIP_KEY;
use crate::raft::store::keys::NODES_KEY;
use crate::raft::store::keys::SM_DATA_FAMILY;
use crate::raft::store::keys::SM_META_FAMILY;
use crate::raft::types::Node;
use crate::raft::types::NodeId;
use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotMeta;
use crate::raft::types::encode;
use crate::raft::types::read_logs_err;

/// Default upper bound for one snapshot entry (its key or its value).
///
/// The length prefix is a `u32` read from a stream that may be corrupt: without
/// a bound, one bad prefix asks for a multi-gigabyte allocation and can abort
/// the receiving node. A raft log entry has to fit the gRPC message cap to be
/// replicated at all, so twice the cap leaves headroom for a legitimate entry
/// while refusing an absurd one.
///
/// This is the value used by [`RocksStateMachine::new`]; the node itself passes
/// the limit derived from its own `grpc_max_message_size`, because an entry
/// this node accepted for replication must also be installable from a snapshot.
pub(crate) const DEFAULT_SNAPSHOT_ENTRY_LIMIT: usize = DEFAULT_GRPC_MAX_MESSAGE_SIZE * 2;

/// Read an entry key or value into `out`, refusing implausible lengths before
/// allocating.
///
/// `scratch` is a caller-owned read buffer and `out` is a caller-owned
/// destination, so the decode loop allocates nothing after its first iteration:
/// `WriteBatch::put_cf` copies the bytes before the next entry is read, which
/// makes reusing both buffers safe. `read_exact` overwrites exactly the prefix
/// it uses, so neither needs to be cleared between entries.
fn read_entry_into(
  decoder: &mut impl Read,
  len: usize,
  limit: usize,
  scratch: &mut [u8],
  out: &mut Vec<u8>,
) -> io::Result<()> {
  if len > limit {
    return Err(io::Error::new(
      ErrorKind::InvalidData,
      format!(
        "snapshot entry length {} exceeds the {} byte limit",
        len, limit
      ),
    ));
  }

  out.clear();
  out.try_reserve_exact(len).map_err(|e| {
    io::Error::new(
      ErrorKind::InvalidData,
      format!("cannot allocate {} bytes for a snapshot entry: {}", len, e),
    )
  })?;

  // Fill the output in bounded steps: a corrupt length with little data behind
  // it fails on the first short read, having touched only this chunk. Reading
  // through the reusable scratch buffer means the destination is written once
  // (by `extend_from_slice`) instead of being zero-filled and then overwritten.
  let mut remaining = len;
  while remaining > 0 {
    let n = remaining.min(scratch.len());
    decoder.read_exact(&mut scratch[..n])?;
    out.extend_from_slice(&scratch[..n]);
    remaining -= n;
  }

  Ok(())
}

/// Recover the state machine from a snapshot, blocking until it completes.
///
/// OpenRaft's contract for `RaftStateMachine::install_snapshot` requires the
/// state machine to hold the snapshot contents before the call returns —
/// recovery runs to completion here so that log entries applied afterwards
/// build on fully restored data.
///
/// Arguments:
///   db: Reference to the RocksDB instance where data will be recovered
///   snapshot: The snapshot containing data to recover
///
/// Returns:
///   Result<(), io::Error>: Ok(()) once the snapshot state and its metadata are
///   committed to the DB in one atomic batch
pub async fn recover_snapshot(
  db: &Arc<DB>,
  snapshot: Snapshot,
  max_entry_bytes: usize,
) -> Result<(), io::Error> {
  let snapshot_id = snapshot.meta.snapshot_id.clone();
  let meta = snapshot.meta.clone();

  info!(
    "Starting to recover from snapshot, snapshot_id={}",
    snapshot_id
  );

  let db_clone = db.clone();
  // The zstd decode + RocksDB bulk write is blocking work; run it off the
  // async runtime but await its completion before returning.
  spawn_blocking(move || {
    let snapshot_file = futures::executor::block_on(snapshot.snapshot.into_std());
    do_recover_snapshot_sync(&db_clone, snapshot_file, &meta, max_entry_bytes)
  })
  .await
  .map_err(|e| io::Error::other(format!("Snapshot recovery task failed: {}", e)))??;

  info!(
    "Snapshot recovery completed successfully for snapshot_id={}",
    snapshot_id
  );

  Ok(())
}

/// Internal function: Perform the actual snapshot recovery
///
/// This function handles the core recovery logic:
/// - Reads and decompresses snapshot data
/// - Clears the KV column family, then restores snapshot key-value pairs
/// - Stages the clear, every restored entry, and the snapshot metadata
///   (apply progress, membership, nodes) into a single `WriteBatch`
///
/// Arguments:
///   db: Reference to the RocksDB instance for data recovery
///   snapshot_file: The decompressed snapshot payload
///   meta: Metadata of the snapshot being installed
///
/// Returns:
///   Result<(), io::Error>: Ok(()) on success, error on failure
///
/// Recovery Process:
///   1. Decompress snapshot data using zstd
///   2. Read entries in format: [key_len][key][value_len][value]
///   3. Accumulate the whole snapshot -- the `delete_range` that clears the KV
///      column family, every restored entry, and the metadata `applied_state()`
///      reports -- into one `WriteBatch`
///   4. Commit that batch once
///
/// Atomicity:
///   A single `WriteBatch` is the only way RocksDB offers to switch the column
///   family from one state to another without an intermediate state being
///   observable. Committing in chunks would mean the `delete_range` lands
///   first: a decode error or a crash after that commit leaves the node with
///   part of the snapshot and no way to tell, and a restart would replay logs
///   on top of it. The metadata goes into the same batch for the same reason:
///   `applied_state()` must never describe a position the KV data has not
///   reached (or vice versa). The trade-off is memory: the batch holds the
///   whole snapshot until it is committed.
///
/// Performance:
///   - A single commit keeps the write path short
///   - Compression reduces disk I/O for snapshot files
fn do_recover_snapshot_sync(
  db: &Arc<DB>,
  snapshot_file: std::fs::File,
  meta: &SnapshotMeta,
  max_entry_bytes: usize,
) -> Result<(), io::Error> {
  let cf_handle = db.cf_handle(SM_DATA_FAMILY).ok_or_else(|| {
    io::Error::new(
      ErrorKind::NotFound,
      format!("Column family {} not found", SM_DATA_FAMILY),
    )
  })?;
  let cf_meta = db.cf_handle(SM_META_FAMILY).ok_or_else(|| {
    io::Error::new(
      ErrorKind::NotFound,
      format!("Column family {} not found", SM_META_FAMILY),
    )
  })?;

  let mut decoder =
    zstd::Decoder::new(snapshot_file).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

  let mut batch = rocksdb::WriteBatch::default();
  // Drop every pre-existing key in the KV column family in the same batch as
  // the restored entries, so the target node ends up holding exactly the
  // snapshot state — no stale keys from previously applied logs or older
  // snapshots can survive the install, and a failure before the commit leaves
  // the old state untouched.
  // Bounds: empty begin = smallest key; a 64-byte 0xFF end covers every
  // realistic key (only a key of 64+ consecutive 0xFF bytes would exceed it).
  let empty_key: Vec<u8> = Vec::new();
  let max_key = vec![0xFF; 64];
  batch.delete_range_cf(&cf_handle, &empty_key, &max_key);
  let mut count = 0u64;
  let mut bytes = 0u64;
  let started = std::time::Instant::now();
  let mut len_buf = [0u8; 4];
  // Allocated once and reused by every entry: `put_cf` copies the bytes into
  // the batch before the next entry is decoded, and `read_exact` overwrites
  // only the prefix it uses, so nothing has to be cleared between entries.
  let mut scratch = vec![0u8; 64 * 1024];
  let mut key_buf = Vec::new();
  let mut value_buf = Vec::new();

  loop {
    match decoder.read_exact(&mut len_buf) {
      Ok(_) => {}
      Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
      Err(e) => return Err(e),
    }
    let key_len = u32::from_le_bytes(len_buf) as usize;
    read_entry_into(
      &mut decoder,
      key_len,
      max_entry_bytes,
      &mut scratch,
      &mut key_buf,
    )?;

    decoder.read_exact(&mut len_buf)?;
    let value_len = u32::from_le_bytes(len_buf) as usize;
    read_entry_into(
      &mut decoder,
      value_len,
      max_entry_bytes,
      &mut scratch,
      &mut value_buf,
    )?;

    batch.put_cf(&cf_handle, &key_buf, &value_buf);
    count += 1;
    bytes += (key_len + value_len) as u64;
  }

  // The metadata `applied_state()` reports goes into the same batch as the KV
  // data: a crash must not be able to install one without the other.
  match &meta.last_log_id {
    Some(last_log_id) => {
      let data = encode(last_log_id).map_err(read_logs_err)?;
      batch.put_cf(&cf_meta, LAST_APPLIED_LOG_KEY, data);
    }
    None => batch.delete_cf(&cf_meta, LAST_APPLIED_LOG_KEY),
  }

  let membership_data = encode(&meta.last_membership).map_err(read_logs_err)?;
  batch.put_cf(&cf_meta, LAST_MEMBERSHIP_KEY, membership_data);

  // Rebuild the nodes table from the snapshot membership so both views agree
  // (same invariant as applying a Membership log entry).
  let nodes: BTreeMap<NodeId, Node> = meta
    .last_membership
    .membership()
    .nodes()
    .map(|(id, node)| (*id, node.clone()))
    .collect();
  let nodes_data = encode(&nodes).map_err(read_logs_err)?;
  batch.put_cf(&cf_meta, NODES_KEY, nodes_data);

  // One commit for the whole snapshot: the clear, the restore and the
  // metadata are one atomic switch, so a truncated or corrupt transfer never
  // leaves a half-restored state behind.
  db.write(batch).map_err(io::Error::other)?;

  info!(
    "Recovered {} entries ({} bytes) from snapshot in {:?}",
    count,
    bytes,
    started.elapsed()
  );

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::raft::types::LogId;
  use crate::raft::types::TypeConfig;
  use crate::raft::types::decode;
  use crate::utils::test::create_log_id;
  use openraft::Membership;
  use openraft::SnapshotMeta;
  use openraft::StoredMembership;
  use std::io::Write;
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
    let db = DB::open_cf(&opts, temp_dir.path(), vec![SM_DATA_FAMILY, SM_META_FAMILY]).unwrap();

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

  /// Test-only wrapper: converts the tokio File and runs the synchronous recovery.
  async fn do_recover_snapshot_for_test(db: &Arc<DB>, snapshot: Snapshot) -> Result<(), io::Error> {
    let meta = snapshot.meta.clone();
    let snapshot_file = snapshot.snapshot.into_std().await;
    do_recover_snapshot_sync(db, snapshot_file, &meta, DEFAULT_SNAPSHOT_ENTRY_LIMIT)
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

    let result = do_recover_snapshot_for_test(&db, snapshot).await;

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

    let result = do_recover_snapshot_for_test(&db, snapshot).await;

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

    let result = do_recover_snapshot_for_test(&db, snapshot).await;

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

  /// Test Point: Synchronous snapshot recovery function
  ///
  /// This test verifies that recover_snapshot blocks until recovery completes:
  /// - Creates a snapshot with 5 key-value pairs
  /// - Calls recover_snapshot and awaits it
  /// - Verifies the data is immediately visible once the call returns
  ///
  /// Expected Behavior:
  /// - recover_snapshot should return only after recovery is complete
  /// - All 5 entries should be present in the database as soon as it returns
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

    // recover_snapshot awaits recovery completion; data is available immediately
    let result = recover_snapshot(&db, snapshot, DEFAULT_SNAPSHOT_ENTRY_LIMIT).await;
    assert!(result.is_ok());

    // Verify the correct number of entries were recovered (5)
    let entry_count = count_db_entries(&db);
    assert_eq!(
      entry_count, 5,
      "Expected 5 entries in database after recovery"
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

    let result = do_recover_snapshot_for_test(&db, snapshot).await;

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

    let result = do_recover_snapshot_for_test(&db, snapshot).await;

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

  /// Test Point: Recovery removes keys that the snapshot does not contain
  ///
  /// Keys left in the target database from previously applied logs or an
  /// older snapshot must not survive an install: the recovered state must be
  /// an exact copy of the snapshot, otherwise the node diverges from the
  /// source node permanently.
  #[tokio::test]
  async fn test_recover_snapshot_removes_stale_keys() {
    let db = create_test_db();
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    // Pre-populate with keys the snapshot will NOT contain
    db.put_cf(&cf_handle, b"stale-key-1", b"stale-value-1")
      .unwrap();
    db.put_cf(&cf_handle, b"stale-key-2", b"stale-value-2")
      .unwrap();

    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("stale.snapshot");

    // Snapshot contains only "fresh-key"
    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();
    encoder
      .write_all(&("fresh-key".len() as u32).to_le_bytes())
      .unwrap();
    encoder.write_all(b"fresh-key").unwrap();
    encoder
      .write_all(&("fresh-value".len() as u32).to_le_bytes())
      .unwrap();
    encoder.write_all(b"fresh-value").unwrap();
    encoder.finish().unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("stale-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_ok());

    // The snapshot state replaced the old one entirely
    let entry_count = count_db_entries(&db);
    assert_eq!(entry_count, 1, "Only the snapshot key should remain");

    assert!(db.get_cf(&cf_handle, b"stale-key-1").unwrap().is_none());
    assert!(db.get_cf(&cf_handle, b"stale-key-2").unwrap().is_none());

    let value = db.get_cf(&cf_handle, b"fresh-key").unwrap().unwrap();
    assert_eq!(String::from_utf8(value).unwrap(), "fresh-value");
  }

  #[tokio::test]
  async fn test_recover_snapshot_truncated_during_transfer() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("truncated.snapshot");

    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 10).await;

    let original_metadata = std::fs::metadata(&snapshot_file_path).unwrap();
    let truncated_size = original_metadata.len() / 2;

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    file.set_len(truncated_size).unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("truncated-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "Expected error for truncated snapshot");
  }

  #[tokio::test]
  async fn test_recover_snapshot_corrupted_key_length() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("corrupted.snapshot");

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();

    let key = "valid-key";
    let value = "valid-value";
    encoder
      .write_all(&(key.len() as u32).to_le_bytes())
      .unwrap();
    encoder.write_all(key.as_bytes()).unwrap();
    encoder
      .write_all(&(value.len() as u32).to_le_bytes())
      .unwrap();
    encoder.write_all(value.as_bytes()).unwrap();

    encoder.write_all(&(0xFFFFFFFFu32).to_le_bytes()).unwrap();

    encoder.finish().unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("corrupted-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "Expected error for corrupted key length");
  }

  /// A large snapshot that fails to decode must leave the target database
  /// exactly as it was.
  ///
  /// Committing the clear plus the entries in chunks would make the first
  /// commit wipe the column family and restore only the entries decoded so
  /// far; the error then surfaces after the damage is done, and a restart
  /// replays logs on top of a half-restored state. The payload below is larger
  /// than the old 1000-entry commit boundary on purpose: it holds 3000 valid
  /// entries followed by a length prefix no file can satisfy, so the failure
  /// lands after the old code would have committed twice.
  #[tokio::test]
  async fn test_recover_snapshot_decode_error_leaves_db_untouched() {
    let db = create_test_db();
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    db.put_cf(&cf_handle, b"existing-key", b"existing-value")
      .unwrap();
    db.put_cf(&cf_handle, b"existing-key-2", b"existing-value-2")
      .unwrap();

    // Old state-machine metadata: it is staged into the same atomic batch as
    // the KV data, so a failed install must leave it untouched too.
    let cf_meta = db.cf_handle(SM_META_FAMILY).unwrap();
    let old_log_id = create_log_id(1, 1, 1);
    let old_membership = StoredMembership::new(Some(old_log_id), Membership::default());
    db.put_cf(&cf_meta, LAST_APPLIED_LOG_KEY, encode(&old_log_id).unwrap())
      .unwrap();
    db.put_cf(
      &cf_meta,
      LAST_MEMBERSHIP_KEY,
      encode(&old_membership).unwrap(),
    )
    .unwrap();
    db.put_cf(
      &cf_meta,
      NODES_KEY,
      encode(&BTreeMap::<NodeId, Node>::new()).unwrap(),
    )
    .unwrap();

    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("corrupt-large.snapshot");

    {
      let file = std::fs::File::create(&snapshot_file_path).unwrap();
      let mut encoder = zstd::Encoder::new(file, 3).unwrap();
      for i in 0..3000 {
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
      // Longer than the four remaining bytes: the decoder fails here, after
      // the 3000 entries above, without asking for an absurd allocation.
      encoder.write_all(&1024u32.to_le_bytes()).unwrap();
      encoder.finish().unwrap();
    }

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: "corrupt-large-snapshot".to_string(),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "a corrupt snapshot must not install");

    assert_eq!(
      count_db_entries(&db),
      2,
      "the old state must survive intact"
    );
    let value = db.get_cf(&cf_handle, b"existing-key").unwrap().unwrap();
    assert_eq!(String::from_utf8(value).unwrap(), "existing-value");

    let stored_log_id: LogId =
      decode(&db.get_cf(&cf_meta, LAST_APPLIED_LOG_KEY).unwrap().unwrap()).unwrap();
    assert_eq!(
      stored_log_id, old_log_id,
      "the old apply position must survive a failed install"
    );
    let stored_membership: StoredMembership<TypeConfig> =
      decode(&db.get_cf(&cf_meta, LAST_MEMBERSHIP_KEY).unwrap().unwrap()).unwrap();
    assert_eq!(
      stored_membership, old_membership,
      "the old membership must survive a failed install"
    );
  }

  /// `read_entry_into` reuses the caller's buffers. A longer entry followed by
  /// a shorter one must not leak any of the previous bytes: on success
  /// `read_exact` fills exactly the prefix that is then copied, so no per-call
  /// clear is needed.
  #[test]
  fn test_read_entry_into_does_not_leak_stale_buffer_data() {
    use std::io::Cursor;

    // Simulate buffers left over from a previous, longer entry.
    let mut scratch = [0xAAu8; 128];
    let mut out = b"0123456789".to_vec();

    let mut first = Cursor::new(b"first-value".to_vec());
    read_entry_into(&mut first, 11, 1024, &mut scratch, &mut out).unwrap();
    assert_eq!(out, b"first-value");

    // Only these two bytes may be copied; the previous tail must not leak.
    let mut second = Cursor::new(b"xy".to_vec());
    read_entry_into(&mut second, 2, 1024, &mut scratch, &mut out).unwrap();
    assert_eq!(out, b"xy");
  }

  /// The bound comes from the caller, so a node that raised its gRPC message
  /// cap can install entries the default limit would refuse.
  #[test]
  fn test_read_entry_into_honours_the_caller_limit() {
    use std::io::Cursor;

    let mut scratch = [0u8; 16];
    let mut out = Vec::new();

    let mut data = Cursor::new(b"abcde".to_vec());
    read_entry_into(&mut data, 5, 4, &mut scratch, &mut out).unwrap_err();

    let mut data = Cursor::new(b"abcde".to_vec());
    read_entry_into(&mut data, 5, 5, &mut scratch, &mut out).unwrap();
    assert_eq!(out, b"abcde");
  }

  /// A length prefix that claims an implausible entry size is rejected before
  /// allocating, leaving the database untouched.
  #[tokio::test]
  async fn test_recover_snapshot_rejects_oversized_entry() {
    let db = create_test_db();
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();
    db.put_cf(&cf_handle, b"existing-key", b"existing-value")
      .unwrap();

    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("oversized.snapshot");

    {
      let file = std::fs::File::create(&snapshot_file_path).unwrap();
      let mut encoder = zstd::Encoder::new(file, 3).unwrap();
      let too_big = (DEFAULT_SNAPSHOT_ENTRY_LIMIT + 1) as u32;
      encoder.write_all(&too_big.to_le_bytes()).unwrap();
      encoder.finish().unwrap();
    }

    let snapshot = Snapshot {
      meta: SnapshotMeta {
        last_log_id: None,
        last_membership: StoredMembership::new(None, Membership::default()),
        snapshot_id: "oversized-entry-snapshot".to_string(),
      },
      snapshot: tokio::fs::File::open(&snapshot_file_path).await.unwrap(),
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "an oversized entry must be rejected");
    assert_eq!(
      count_db_entries(&db),
      1,
      "the old state must survive the rejected install"
    );
  }

  #[tokio::test]
  async fn test_recover_snapshot_partial_write_consistency() {
    let db = create_test_db();
    let cf_handle = db.cf_handle(SM_DATA_FAMILY).unwrap();

    db.put_cf(&cf_handle, b"existing-key", b"existing-value")
      .unwrap();

    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("partial.snapshot");

    create_test_snapshot_file(snapshot_file_path.to_str().unwrap(), 10).await;

    let original_metadata = std::fs::metadata(&snapshot_file_path).unwrap();
    let truncated_size = original_metadata.len() / 3;

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    file.set_len(truncated_size).unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("partial-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "Expected error for partial snapshot");

    let existing_value = db.get_cf(&cf_handle, b"existing-key").unwrap().unwrap();
    assert_eq!(
      String::from_utf8(existing_value).unwrap(),
      "existing-value",
      "Existing data should not be corrupted"
    );
  }

  #[tokio::test]
  async fn test_recover_snapshot_value_truncated() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("value-truncated.snapshot");

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();

    let key = "test-key";
    let _value = "this-is-a-long-value";
    encoder
      .write_all(&(key.len() as u32).to_le_bytes())
      .unwrap();
    encoder.write_all(key.as_bytes()).unwrap();
    encoder.write_all(&(100u32).to_le_bytes()).unwrap();
    encoder.write_all(key.as_bytes()).unwrap();
    encoder.write_all(&(100u32).to_le_bytes()).unwrap();
    encoder.write_all(b"partial").unwrap();

    encoder.finish().unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("value-truncated-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(result.is_err(), "Expected error for truncated value");
  }

  #[tokio::test]
  async fn test_recover_snapshot_large_data_interrupted() {
    let db = create_test_db();
    let temp_dir = tempdir().unwrap();
    let snapshot_file_path = temp_dir.path().join("large-interrupted.snapshot");

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    let mut encoder = zstd::Encoder::new(file, 3).unwrap();

    let large_value = "x".repeat(10240);
    for i in 0..100 {
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

    let original_metadata = std::fs::metadata(&snapshot_file_path).unwrap();
    let truncated_size = (original_metadata.len() as f64 * 0.7) as u64;

    let file = std::fs::File::create(&snapshot_file_path).unwrap();
    file.set_len(truncated_size).unwrap();

    let snapshot_meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::new(None, Membership::default()),
      snapshot_id: String::from("large-interrupted-snapshot"),
    };

    let file = tokio::fs::File::open(&snapshot_file_path).await.unwrap();
    let snapshot = Snapshot {
      meta: snapshot_meta,
      snapshot: file,
    };

    let result = do_recover_snapshot_for_test(&db, snapshot).await;
    assert!(
      result.is_err(),
      "Expected error for interrupted large data transfer"
    );
  }
}
