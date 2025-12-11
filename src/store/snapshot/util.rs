use std::io::ErrorKind;
use std::path::PathBuf;

use bincode::serialize;
use openraft::SnapshotMeta;
use tokio::fs::File;
use tokio::io::AsyncWriteExt as _;

use crate::types::TypeConfig;

pub fn snapshot_dump_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("dump")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

pub fn snapshot_meta_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("meta")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

pub fn snapshot_data_file(snapshot_id_dir: &PathBuf) -> String {
  snapshot_id_dir
    .join("snapshot")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

pub fn snapshot_last_snapshot_id_file(snapshot_dir: &PathBuf) -> String {
  snapshot_dir
    .join("last_snapshot_id")
    .as_path()
    .to_str()
    .unwrap()
    .to_string()
}

pub async fn save_last_snapshot_id_file(
  snapshot_dir: &PathBuf,
  snapshot_id: &str,
) -> std::io::Result<()> {
  let last_snapshot_id_file = snapshot_last_snapshot_id_file(snapshot_dir);

  let mut file = File::create(&last_snapshot_id_file).await?;
  file.write_all(snapshot_id.as_bytes()).await?;
  file.flush().await?;

  Ok(())
}

pub async fn save_snapshot_meta(
  snapshot_id_dir: &PathBuf,
  meta: SnapshotMeta<TypeConfig>,
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
