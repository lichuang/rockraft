use std::io::ErrorKind;
use std::io::{self};
use std::path::PathBuf;

use bincode::deserialize;
use bincode::serialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt as _;
use tracing::info;

use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotMeta;
use crate::raft::types::TypeConfig;

/// Construct the path to the snapshot dump file for a given snapshot ID directory.
///
/// This function takes a snapshot ID directory path and joins it with the "dump" filename,
/// returning the full path as a string. The dump file is used to store snapshot data in
/// a serialized format.
///
/// # Arguments
///
/// * `snapshot_id_dir` - A reference to the `PathBuf` representing the directory for a specific snapshot ID
///
/// # Returns
///
/// A `String` containing the full path to the dump file within the snapshot ID directory
///
/// # Panics
///
/// This function will panic if the resulting path cannot be converted to a string representation.
/// This typically occurs with paths containing invalid UTF-8 sequences.
pub fn snapshot_dump_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("dump")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

/// Construct the path to the snapshot metadata file for a given snapshot ID directory.
///
/// This function takes a snapshot ID directory path and joins it with the "meta" filename,
/// returning the full path as a string. The metadata file stores serialized snapshot metadata
/// including snapshot ID, term, membership, and other Raft-related information.
///
/// # Arguments
///
/// * `snapshot_id_dir` - A reference to the `PathBuf` representing the directory for a specific snapshot ID
///
/// # Returns
///
/// A `String` containing the full path to the metadata file within the snapshot ID directory
///
/// # Panics
///
/// This function will panic if the resulting path cannot be converted to a string representation.
/// This typically occurs with paths containing invalid UTF-8 sequences.
pub fn snapshot_meta_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("meta")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

/// Construct the path to the snapshot data file for a given snapshot ID directory.
///
/// This function takes a snapshot ID directory path and joins it with the "snapshot" filename,
/// returning the full path as a string. The snapshot data file contains the actual state machine
/// data that was captured at the time the snapshot was created.
///
/// # Arguments
///
/// * `snapshot_id_dir` - A reference to the `PathBuf` representing the directory for a specific snapshot ID
///
/// # Returns
///
/// A `String` containing the full path to the snapshot data file within the snapshot ID directory
///
/// # Panics
///
/// This function will panic if the resulting path cannot be converted to a string representation.
/// This typically occurs with paths containing invalid UTF-8 sequences.
pub fn snapshot_data_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("snapshot")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

/// Construct the path to the file storing the last snapshot ID in the snapshot directory.
///
/// This function takes the main snapshot directory path and joins it with the "last_snapshot_id"
/// filename. This file tracks the ID of the most recent snapshot, which is used to determine
/// which snapshot to recover from when the system restarts.
///
/// # Arguments
///
/// * `snapshot_dir` - A reference to the `PathBuf` representing the main snapshot directory
///
/// # Returns
///
/// A `String` containing the full path to the last snapshot ID file
///
/// # Panics
///
/// This function will panic if the resulting path cannot be converted to a string representation.
/// This typically occurs with paths containing invalid UTF-8 sequences.
pub fn snapshot_last_snapshot_id_file(snapshot_dir: &PathBuf) -> String {
  snapshot_dir
    .join("last_snapshot_id")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

/// Construct the directory path for a specific snapshot ID within the snapshot directory.
///
/// This function joins the main snapshot directory with a snapshot ID to create the
/// full path to the directory that contains all files for that specific snapshot
/// (meta, snapshot data, dump, etc.).
///
/// # Arguments
///
/// * `snapshot_dir` - A reference to the `PathBuf` representing the main snapshot directory
/// * `snapshot_id` - The unique identifier for the snapshot
///
/// # Returns
///
/// A `PathBuf` representing the full path to the snapshot ID directory
pub fn snapshot_id_dir(snapshot_dir: &PathBuf, snapshot_id: &str) -> PathBuf {
  snapshot_dir.join(snapshot_id)
}

/// Save the last snapshot ID to the snapshot directory.
///
/// This function writes the provided snapshot ID to the "last_snapshot_id" file in the
/// snapshot directory. This is used to track the most recent snapshot for recovery purposes.
/// The file is created if it doesn't exist, or overwritten if it does.
///
/// # Arguments
///
/// * `snapshot_dir` - A reference to the `PathBuf` representing the main snapshot directory
/// * `last_snapshot_id` - The snapshot ID to save as the last snapshot
///
/// # Returns
///
/// Returns `Ok(())` if the snapshot ID was successfully written and flushed to disk
/// Returns `Err(io::Error)` if file creation, writing, or flushing fails
///
/// # Errors
///
/// This function will return an error if:
/// - The snapshot directory doesn't exist and cannot be created
/// - There are insufficient permissions to create or write the file
/// - The disk is full
/// - The file system experiences an I/O error
pub async fn save_last_snapshot_id_file(
  snapshot_dir: &PathBuf,
  last_snapshot_id: &str,
) -> std::io::Result<()> {
  let last_snapshot_id_file = snapshot_last_snapshot_id_file(snapshot_dir);

  let mut file = File::create(&last_snapshot_id_file).await?;
  file.write_all(last_snapshot_id.as_bytes()).await?;
  file.flush().await?;

  Ok(())
}

/// Retrieve the last snapshot ID from the snapshot directory.
///
/// This function reads and returns the snapshot ID stored in the "last_snapshot_id" file.
/// This is a crate-private function used internally to determine which snapshot is the most recent.
///
/// # Arguments
///
/// * `snapshot_dir` - A reference to the `PathBuf` representing the main snapshot directory
///
/// # Returns
///
/// Returns `Ok(String)` containing the last snapshot ID if successful
/// Returns `Err(io::Error)` if the file cannot be opened or read
///
/// # Errors
///
/// This function will return an error if:
/// - The "last_snapshot_id" file doesn't exist (ErrorKind::NotFound)
/// - There are insufficient permissions to read the file
/// - The file contains invalid UTF-8 data
/// - The file system experiences an I/O error
pub(crate) async fn get_last_snapshot_id(snapshot_dir: &PathBuf) -> std::io::Result<String> {
  let last_snapshot_file = snapshot_last_snapshot_id_file(snapshot_dir);

  let mut file = File::open(&last_snapshot_file).await?;
  let mut content = String::new();
  file.read_to_string(&mut content).await?;
  Ok(content)
}

/// Save snapshot metadata to the snapshot ID directory.
///
/// This function serializes the provided snapshot metadata using bincode and writes it
/// to the "meta" file in the specified snapshot ID directory. The metadata includes information
/// such as the snapshot ID, term, membership configuration, and other Raft-related metadata.
///
/// # Arguments
///
/// * `snapshot_id_dir` - A reference to the `PathBuf` representing the directory for a specific snapshot ID
/// * `meta` - The `SnapshotMeta` object containing the snapshot metadata to save
///
/// # Returns
///
/// Returns `Ok(())` if the metadata was successfully serialized, written, and flushed to disk
/// Returns `Err(io::Error)` if serialization, file creation, writing, or flushing fails
///
/// # Errors
///
/// This function will return an error if:
/// - The snapshot metadata cannot be serialized (ErrorKind::InvalidData)
/// - The snapshot ID directory doesn't exist or cannot be created
/// - There are insufficient permissions to create or write the file
/// - The disk is full
/// - The file system experiences an I/O error
pub async fn save_snapshot_meta(
  snapshot_id_dir: &PathBuf,
  meta: SnapshotMeta,
) -> std::io::Result<()> {
  let meta_file = snapshot_meta_file(snapshot_id_dir);

  let data = serialize(&meta).map_err(|e| {
    std::io::Error::new(
      ErrorKind::InvalidData,
      format!("Serialize meta data error: {}", e),
    )
  })?;

  let mut file = File::create(&meta_file).await?;
  file.write_all(&data).await?;
  file.flush().await?;

  Ok(())
}

/// Retrieve and deserialize snapshot metadata from a file.
///
/// This function reads the serialized snapshot metadata from the specified file path,
/// deserializes it using bincode, and returns the `SnapshotMeta` object.
///
/// # Arguments
///
/// * `snapshot_id_dir` - A reference to the `PathBuf` pointing to the metadata file
///
/// # Returns
///
/// Returns `Ok(SnapshotMeta)` containing the deserialized snapshot metadata if successful
/// Returns `Err(io::Error)` if the file cannot be opened, read, or deserialized
///
/// # Errors
///
/// This function will return an error if:
/// - The metadata file doesn't exist (ErrorKind::NotFound)
/// - There are insufficient permissions to read the file
/// - The file contains invalid or corrupted bincode data (ErrorKind::InvalidData)
/// - The file system experiences an I/O error
pub async fn get_snapshot_meta(snapshot_id_dir: &PathBuf) -> std::io::Result<SnapshotMeta> {
  let mut file = File::open(snapshot_id_dir).await?;
  let mut data = Vec::new();
  file.read_to_end(&mut data).await?;

  deserialize(&data)
    .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, format!("Deserialize error: {}", e)))
}

/// Retrieve the current (most recent) snapshot from the snapshot directory.
///
/// This function attempts to load the most recent snapshot by:
/// 1. Reading the last snapshot ID from the "last_snapshot_id" file
/// 2. Loading the snapshot metadata for that snapshot ID
/// 3. Opening the snapshot data file
/// 4. Returning a complete `Snapshot` object containing both metadata and data file handle
///
/// If any component of the snapshot is missing (no snapshot ID file, no metadata, or no data file),
/// the function returns `Ok(None)` instead of an error, as this is a valid state when no snapshot exists.
///
/// # Arguments
///
/// * `snapshot_dir` - A reference to the `PathBuf` representing the main snapshot directory
///
/// # Returns
///
/// Returns `Ok(Some(Snapshot))` if the snapshot was successfully loaded with both metadata and data file
/// Returns `Ok(None)` if no snapshot exists (missing last_snapshot_id file, metadata, or data file)
/// Returns `Err(io::Error)` if there's an I/O error reading files or deserializing data
///
/// # Errors
///
/// This function will return an error if:
/// - The last_snapshot_id file exists but cannot be read
/// - The snapshot metadata file exists but cannot be deserialized
/// - The snapshot data file exists but cannot be opened due to permission issues
/// - Any unexpected I/O error occurs
///
/// Note that missing files are not considered errors and result in `Ok(None)`.
pub async fn get_current_snapshot(snapshot_dir: &PathBuf) -> std::io::Result<Option<Snapshot>> {
  let snapshot_id = match get_last_snapshot_id(snapshot_dir).await {
    Ok(id) => id,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!("No snapshot found, returning None");
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);

  let snapshot_meta_file = snapshot_meta_file(&snapshot_id_dir);
  let snapshot_meta = match get_snapshot_meta(&PathBuf::from(snapshot_meta_file)).await {
    Ok(meta) => meta,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!(
        "Snapshot metadata not found for snapshot_id={}, returning None",
        snapshot_id
      );
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  let snapshot_data_file = snapshot_data_file(&snapshot_id_dir);
  let res = match File::open(&snapshot_data_file).await {
    Ok(file) => file,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!(
        "Snapshot file not found for snapshot_id={}, returning None",
        snapshot_id
      );
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  Ok(Some(Snapshot {
    meta: snapshot_meta,
    snapshot: res,
  }))
}

#[cfg(test)]
mod tests {
  use super::*;
  use openraft::Membership;
  use openraft::SnapshotMeta;
  use openraft::StoredMembership;
  use tempfile::tempdir;

  /// Helper function to create a complete test snapshot structure
  ///
  /// Creates a temporary directory with all necessary files for a snapshot:
  /// - last_snapshot_id file containing the snapshot ID
  /// - Snapshot ID directory with meta file (serialized SnapshotMeta)
  /// - Snapshot data file (empty file for testing)
  ///
  /// # Arguments
  ///
  /// * `snapshot_dir` - The base snapshot directory path
  /// * `snapshot_id` - The snapshot ID to use
  async fn create_test_snapshot(snapshot_dir: &PathBuf, snapshot_id: &str) {
    // Create last_snapshot_id file
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, snapshot_id)
      .await
      .unwrap();

    // Create snapshot ID directory
    let snapshot_id_dir = snapshot_id_dir(snapshot_dir, snapshot_id);
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    // Create snapshot metadata
    let meta = SnapshotMeta::<TypeConfig> {
      snapshot_id: snapshot_id.to_string(),
      ..Default::default()
    };
    save_snapshot_meta(&snapshot_id_dir, meta).await.unwrap();

    // Create empty snapshot data file
    let data_file = snapshot_data_file(&snapshot_id_dir);
    tokio::fs::File::create(&data_file).await.unwrap();
  }

  /// Test successful retrieval of a complete snapshot
  ///
  /// Verifies that when all snapshot files exist (last_snapshot_id, meta, and data),
  /// the function successfully loads and returns the snapshot with correct metadata.
  #[tokio::test]
  async fn test_get_current_snapshot_success() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();

    // Create a complete snapshot structure
    create_test_snapshot(&snapshot_dir, "test_snapshot_001").await;

    // Retrieve the snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Verify the snapshot was loaded successfully
    assert!(result.is_ok());
    let snapshot = result.unwrap();
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();

    // Verify the snapshot metadata
    assert_eq!(snapshot.meta.snapshot_id, "test_snapshot_001");
  }

  /// Test behavior when snapshot directory doesn't exist
  ///
  /// Verifies that get_current_snapshot returns Ok(None) when the entire
  /// snapshot directory structure doesn't exist, as this is a valid state
  /// for a system that hasn't created any snapshots yet.
  #[tokio::test]
  async fn test_get_current_snapshot_no_directory() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().join("nonexistent_snapshots");

    // Attempt to get snapshot from non-existent directory
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should return Ok(None) - no error, just no snapshot
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
  }

  /// Test behavior when last_snapshot_id file is missing
  ///
  /// Verifies that get_current_snapshot returns Ok(None) when the snapshot
  /// directory exists but doesn't contain a last_snapshot_id file.
  #[tokio::test]
  async fn test_get_current_snapshot_no_last_snapshot_id_file() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();

    // Create the snapshot directory but no files
    tokio::fs::create_dir_all(&snapshot_dir).await.unwrap();

    // Attempt to get snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should return Ok(None) since last_snapshot_id file is missing
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
  }

  /// Test behavior when snapshot metadata file is missing
  ///
  /// Verifies that get_current_snapshot returns Ok(None) when last_snapshot_id
  /// exists but the snapshot metadata file is missing.
  #[tokio::test]
  async fn test_get_current_snapshot_no_meta_file() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();
    let snapshot_id = "test_snapshot_002";

    // Create last_snapshot_id file
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, snapshot_id)
      .await
      .unwrap();

    // Create snapshot directory but no meta file or data file
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, snapshot_id);
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    // Attempt to get snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should return Ok(None) since meta file is missing
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
  }

  /// Test behavior when snapshot data file is missing
  ///
  /// Verifies that get_current_snapshot returns Ok(None) when last_snapshot_id
  /// and meta files exist but the snapshot data file is missing.
  #[tokio::test]
  async fn test_get_current_snapshot_no_data_file() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();
    let snapshot_id = "test_snapshot_003";

    // Create last_snapshot_id file
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, snapshot_id)
      .await
      .unwrap();

    // Create snapshot directory and meta file but no data file
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, snapshot_id);
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    let meta = SnapshotMeta::<TypeConfig>::default();
    save_snapshot_meta(&snapshot_id_dir, meta).await.unwrap();

    // Attempt to get snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should return Ok(None) since data file is missing
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
  }

  /// Test behavior with corrupted metadata file
  ///
  /// Verifies that get_current_snapshot returns an error when the metadata
  /// file exists but contains invalid/corrupted data that cannot be deserialized.
  #[tokio::test]
  async fn test_get_current_snapshot_corrupted_meta() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();
    let snapshot_id = "test_snapshot_004";

    // Create last_snapshot_id file
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, snapshot_id)
      .await
      .unwrap();

    // Create snapshot directory with corrupted meta file
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, snapshot_id);
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    let meta_file = snapshot_meta_file(&snapshot_id_dir);
    tokio::fs::write(&meta_file, b"invalid corrupted data")
      .await
      .unwrap();

    // Create snapshot data file
    let data_file = snapshot_data_file(&snapshot_id_dir);
    tokio::fs::File::create(&data_file).await.unwrap();

    // Attempt to get snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should return an error due to corrupted meta data
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
  }

  /// Test behavior with empty last_snapshot_id file
  ///
  /// Verifies that get_current_snapshot handles an empty last_snapshot_id file
  /// by creating a snapshot ID directory with an empty string and proceeding
  /// with normal snapshot loading logic.
  #[tokio::test]
  async fn test_get_current_snapshot_empty_snapshot_id() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();

    // Create last_snapshot_id file with empty content
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, "").await.unwrap();

    // Create snapshot directory with empty name (edge case)
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, "");
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    let meta = SnapshotMeta::<TypeConfig>::default();
    save_snapshot_meta(&snapshot_id_dir, meta).await.unwrap();

    let data_file = snapshot_data_file(&snapshot_id_dir);
    tokio::fs::File::create(&data_file).await.unwrap();

    // Attempt to get snapshot
    let result = get_current_snapshot(&snapshot_dir).await;

    // Should successfully load snapshot (though with empty ID)
    assert!(result.is_ok());
    let snapshot = result.unwrap();
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.meta.snapshot_id, "");
  }

  /// Test that snapshot file handle is valid
  ///
  /// Verifies that the returned snapshot contains a valid file handle
  /// to the snapshot data file that can be used for reading.
  #[tokio::test]
  async fn test_get_current_snapshot_file_handle_valid() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();

    // Create a complete snapshot structure
    create_test_snapshot(&snapshot_dir, "test_snapshot_005").await;

    // Retrieve the snapshot
    let result = get_current_snapshot(&snapshot_dir).await;
    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();

    // Verify the snapshot data file can be accessed
    // The file handle should be valid
    let metadata = snapshot.snapshot.metadata().await;
    assert!(metadata.is_ok());
    let file_metadata = metadata.unwrap();
    assert!(file_metadata.is_file());
  }

  /// Test with multiple snapshots (verifies it only loads the latest)
  ///
  /// Verifies that get_current_snapshot loads the snapshot referenced by
  /// last_snapshot_id, even if multiple snapshot directories exist.
  #[tokio::test]
  async fn test_get_current_snapshot_multiple_snapshots() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();

    // Create multiple snapshots
    create_test_snapshot(&snapshot_dir, "snapshot_001").await;
    create_test_snapshot(&snapshot_dir, "snapshot_002").await;
    create_test_snapshot(&snapshot_dir, "snapshot_003").await;

    // Set last_snapshot_id to point to snapshot_002
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, "snapshot_002")
      .await
      .unwrap();

    // Retrieve the snapshot
    let result = get_current_snapshot(&snapshot_dir).await;
    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();

    // Verify it loaded snapshot_002, not the others
    assert_eq!(snapshot.meta.snapshot_id, "snapshot_002");
  }

  /// Test that snapshot metadata is correctly deserialized
  ///
  /// Verifies that when loading a snapshot, the metadata is correctly
  /// deserialized and matches what was originally saved.
  #[tokio::test]
  async fn test_get_current_snapshot_metadata_integrity() {
    let temp_dir = tempdir().unwrap();
    let snapshot_dir = temp_dir.path().to_path_buf();
    let snapshot_id = "test_snapshot_006";

    // Create last_snapshot_id file
    let last_snapshot_id_file = snapshot_last_snapshot_id_file(&snapshot_dir);
    tokio::fs::write(&last_snapshot_id_file, snapshot_id)
      .await
      .unwrap();

    // Create snapshot directory
    let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, snapshot_id);
    tokio::fs::create_dir_all(&snapshot_id_dir).await.unwrap();

    // Create snapshot metadata with specific values
    let membership = Membership::<TypeConfig>::default();
    let stored_membership = StoredMembership::new(None, membership);

    let meta = SnapshotMeta::<TypeConfig> {
      snapshot_id: snapshot_id.to_string(),
      last_membership: stored_membership.clone(),
      last_log_id: None,
    };
    save_snapshot_meta(&snapshot_id_dir, meta.clone())
      .await
      .unwrap();

    // Create snapshot data file
    let data_file = snapshot_data_file(&snapshot_id_dir);
    tokio::fs::File::create(&data_file).await.unwrap();

    // Retrieve the snapshot
    let result = get_current_snapshot(&snapshot_dir).await;
    assert!(result.is_ok());

    let snapshot = result.unwrap();
    assert!(snapshot.is_some());
    let loaded_snapshot = snapshot.unwrap();

    // Verify metadata integrity
    assert_eq!(loaded_snapshot.meta.snapshot_id, snapshot_id);
    assert_eq!(loaded_snapshot.meta.last_membership, stored_membership);
    assert_eq!(loaded_snapshot.meta.last_log_id, None);
  }
}
