use std::io::ErrorKind;
use std::io::{self};
use std::path::PathBuf;

use bincode::deserialize;
use bincode::serialize;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt as _;
use tracing::info;

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

pub fn snapshot_id_dir(snapshot_dir: &PathBuf, snapshot_id: &str) -> PathBuf {
  snapshot_dir.join(snapshot_id)
}

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

pub(crate) async fn get_last_snapshot_id(snapshot_dir: &PathBuf) -> std::io::Result<String> {
  let last_snapshot_file = snapshot_last_snapshot_id_file(snapshot_dir);

  let mut file = File::open(&last_snapshot_file).await?;
  let mut content = String::new();
  file.read_to_string(&mut content).await?;
  Ok(content)
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

pub async fn get_snapshot_meta(
  snapshot_id_dir: &PathBuf,
) -> std::io::Result<SnapshotMeta<TypeConfig>> {
  let mut file = File::open(snapshot_id_dir).await?;
  let mut data = Vec::new();
  file.read_to_end(&mut data).await?;

  deserialize(&data)
    .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, format!("Deserialize error: {}", e)))
}

pub async fn get_current_snapshot(
  snapshot_dir: &PathBuf,
) -> std::io::Result<Option<Snapshot<TypeConfig>>> {
  let snapshot_id = match get_last_snapshot_id(snapshot_dir).await {
    Ok(id) => id,
    Err(e) if e.kind() == ErrorKind::NotFound => {
      info!("No snapshot found, returning None");
      return Ok(None);
    }
    Err(e) => return Err(e),
  };

  let snapshot_id_dir = snapshot_id_dir(snapshot_dir, &snapshot_id);

  let snapshot_meta = match get_snapshot_meta(&snapshot_id_dir).await {
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
