use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use byteorder::BigEndian;
use byteorder::ReadBytesExt as _;
use byteorder::WriteBytesExt as _;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use rocksdb::BoundColumnFamily;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::DB;
use tokio::task::spawn_blocking;

use super::keys::LOG_DATA_FAMILY;
use super::keys::LOG_META_FAMILY;
use super::meta::LastPurged;
use super::StoreMeta;
use crate::raft::types::read_logs_err;
use crate::raft::types::Entry;
use crate::raft::types::LogId;
use crate::raft::types::LogState;
use crate::raft::types::RaftCodec;
use crate::raft::types::TypeConfig;

#[derive(Debug, Clone)]
pub struct RocksLogStore<C>
where
  C: RaftTypeConfig,
{
  pub db: Arc<DB>,
  _p: PhantomData<C>,
}

impl RocksLogStore<TypeConfig> {
  pub fn create(db: Arc<DB>) -> Result<Self, std::io::Error> {
    db.cf_handle(LOG_META_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_meta` not found"))?;
    db.cf_handle(LOG_DATA_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_data` not found"))?;

    Ok(Self {
      db,
      _p: Default::default(),
    })
  }

  fn cf_meta(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(LOG_META_FAMILY).unwrap()
  }

  fn cf_logs(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(LOG_DATA_FAMILY).unwrap()
  }

  /// Get a store metadata.
  ///
  /// It returns `None` if the store does not have such a metadata stored.
  fn get_meta<M: StoreMeta>(&self) -> Result<Option<M::Value>, io::Error> {
    let bytes = self
      .db
      .get_cf(&self.cf_meta(), M::KEY)
      .map_err(M::read_err)?;

    let Some(bytes) = bytes else {
      return Ok(None);
    };

    let entry = M::Value::decode_from(bytes.as_ref()).map_err(read_logs_err)?;

    Ok(Some(entry))
  }

  /// Save a store metadata.
  fn put_meta<M: StoreMeta>(&self, value: &M::Value) -> Result<(), io::Error> {
    let encode_valude = value.encode_to()?;

    self
      .db
      .put_cf(&self.cf_meta(), M::KEY, encode_valude)
      .map_err(|e| M::write_err(value, e))?;

    Ok(())
  }

  fn delete_meta<M: StoreMeta>(&self) -> Result<(), io::Error> {
    self
      .db
      .delete_cf(&self.cf_meta(), M::KEY)
      .map_err(|e| M::delete_err(e))?;

    Ok(())
  }

  fn get_last_purged_log_id(&self) -> Result<Option<LogId>, io::Error> {
    self.get_meta::<LastPurged>()
  }

  fn set_last_purged_log_id(&self, log_id: &LogId) -> Result<(), io::Error> {
    self.put_meta::<LastPurged>(log_id)
  }

  fn get_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
    self.get_meta::<super::meta::Vote>()
  }

  fn set_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
    self.put_meta::<super::meta::Vote>(vote)
  }

  fn set_committed(&self, committed: &Option<LogIdOf<TypeConfig>>) -> Result<(), io::Error> {
    if let Some(committed) = committed {
      self.put_meta::<super::meta::Committed>(committed)?;
    } else {
      self.delete_meta::<super::meta::Committed>()?;
    }
    Ok(())
  }

  fn get_committed(&self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
    self.get_meta::<super::meta::Committed>()
  }
}

impl RaftLogReader<TypeConfig> for RocksLogStore<TypeConfig> {
  async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
  ) -> Result<Vec<Entry>, io::Error> {
    let start = match range.start_bound() {
      std::ops::Bound::Included(x) => id_to_bin(*x),
      std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
      std::ops::Bound::Unbounded => id_to_bin(0),
    };

    let mut entries = Vec::new();

    for item in self.db.iterator_cf(
      &self.cf_logs(),
      IteratorMode::From(&start, Direction::Forward),
    ) {
      let (key, val) = item.map_err(read_logs_err)?;

      let id = bin_to_id(&key)?;

      if !range.contains(&id) {
        break;
      }

      let entry: Entry = deserialize(&val).map_err(read_logs_err)?;

      entries.push(entry);
    }

    Ok(entries)
  }

  async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
    self.get_vote()
  }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore<TypeConfig> {
  type LogReader = Self;

  async fn get_log_state(&mut self) -> Result<LogState, io::Error> {
    let last = self
      .db
      .iterator_cf(&self.cf_logs(), rocksdb::IteratorMode::End)
      .next();

    let last_log_id = match last {
      None => None,
      Some(res) => {
        let (_log_index, val) = res.map_err(read_logs_err)?;
        let entry: Entry = deserialize(val.as_ref()).map_err(read_logs_err)?;
        Some(entry.log_id)
      }
    };

    let last_purged_log_id = self.get_last_purged_log_id()?;

    let last_log_id = match last_log_id {
      None => last_purged_log_id.clone(),
      Some(x) => Some(x),
    };

    Ok(LogState {
      last_purged_log_id,
      last_log_id,
    })
  }

  async fn save_committed(
    &mut self,
    committed: Option<LogIdOf<TypeConfig>>,
  ) -> Result<(), io::Error> {
    self.set_committed(&committed)
  }

  async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
    self.get_committed()
  }

  async fn get_log_reader(&mut self) -> Self::LogReader {
    self.clone()
  }

  async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
    self.put_meta::<super::meta::Vote>(vote)?;

    // Vote must be persisted to disk before returning.
    let db = self.db.clone();
    spawn_blocking(move || db.flush_wal(true))
      .await
      .map_err(read_logs_err)?
      .map_err(read_logs_err)?;

    Ok(())
  }

  async fn append<I>(
    &mut self,
    entries: I,
    callback: IOFlushed<TypeConfig>,
  ) -> Result<(), io::Error>
  where
    I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
  {
    for entry in entries {
      let id = id_to_bin(entry.index());
      self
        .db
        .put_cf(
          &self.cf_logs(),
          id,
          serialize(&entry).map_err(read_logs_err)?,
        )
        .map_err(read_logs_err)?;
    }

    // Make sure the logs are persisted to disk before invoking the callback.
    //
    // But the above `pub_cf()` must be called in this function, not in another task.
    // Because when the function returns, it requires the log entries can be read.
    let db = self.db.clone();
    let handle = spawn_blocking(move || {
      let res = db.flush_wal(true).map_err(std::io::Error::other);
      callback.io_completed(res);
    });
    drop(handle);

    // Return now, and the callback will be invoked later when IO is done.
    Ok(())
  }

  async fn truncate(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
    tracing::debug!("truncate: [{:?}, +oo)", log_id);

    let from = id_to_bin(log_id.index());
    let to = id_to_bin(u64::MAX);
    self
      .db
      .delete_range_cf(&self.cf_logs(), &from, &to)
      .map_err(read_logs_err)?;

    // Truncating does not need to be persisted.
    Ok(())
  }

  async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
    tracing::debug!("delete_log: [0, {:?}]", log_id);

    // Write the last-purged log id before purging the logs.
    // The logs at and before last-purged log id will be ignored by openraft.
    // Therefore, there is no need to do it in a transaction.
    self.set_last_purged_log_id(&log_id)?;

    let from = id_to_bin(0);
    let to = id_to_bin(log_id.index() + 1);
    self
      .db
      .delete_range_cf(&self.cf_logs(), &from, &to)
      .map_err(read_logs_err)?;

    // Purging does not need to be persistent.
    Ok(())
  }
}

fn id_to_bin(id: u64) -> Vec<u8> {
  let mut buf = Vec::with_capacity(8);
  buf.write_u64::<BigEndian>(id).unwrap();
  buf
}

fn bin_to_id(buf: &[u8]) -> std::io::Result<u64> {
  (&buf[0..8]).read_u64::<BigEndian>()
}

#[cfg(test)]
mod tests {
  use bytes::Bytes;
  use openraft::Vote;
  use rocksdb::Options;

  use crate::raft::types::{Cmd, KeyValue, LeaderId, LogId, Operation, UpsertKV};

  use super::*;

  fn create_test_log_store() -> RocksLogStore<TypeConfig> {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(
      &opts,
      temp_dir.path(),
      vec![LOG_META_FAMILY, LOG_DATA_FAMILY],
    )
    .unwrap();

    RocksLogStore::create(Arc::new(db)).unwrap()
  }

  fn create_log_id(term: u64, node_id: u64, index: u64) -> LogId {
    LogId {
      leader_id: LeaderId { term, node_id },
      index,
    }
  }

  fn create_entry(term: u64, node_id: u64, index: u64) -> Entry {
    Entry {
      log_id: create_log_id(term, node_id, index),
      payload: openraft::EntryPayload::Normal(Cmd::UpsertKV(UpsertKV::new(
        format!("key_{}_{}", term, index),
        Operation::Update(format!("data_{}_{}", term, index).into_bytes()),
        None,
      ))),
    }
  }

  async fn append_entries(
    log_store: &mut RocksLogStore<TypeConfig>,
    entries: Vec<Entry>,
  ) -> Result<(), io::Error> {
    log_store.append(entries, IOFlushed::noop()).await
  }

  #[tokio::test]
  async fn test_raft_log_operations() -> Result<(), io::Error> {
    let mut log_store = create_test_log_store();

    let entries: Vec<_> = (1..=10).map(|i| create_entry(1, 1, i)).collect();
    append_entries(&mut log_store, entries).await?;

    let all = log_store.try_get_log_entries(1..=10).await?;
    assert_eq!(all.len(), 10);
    assert_eq!(all[all.len() - 1].log_id.index, 10);

    let range = log_store.try_get_log_entries(3..=7).await?;
    assert_eq!(range.len(), 5);
    assert_eq!(range[0].log_id.index, 3);

    let more: Vec<_> = (11..=15).map(|i| create_entry(1, 1, i)).collect();
    append_entries(&mut log_store, more).await?;
    assert_eq!(log_store.try_get_log_entries(1..=15).await?.len(), 15);

    log_store.truncate(create_log_id(1, 1, 11)).await?;
    assert_eq!(log_store.try_get_log_entries(1..=15).await?.len(), 10);
    assert_eq!(log_store.try_get_log_entries(11..=15).await?.len(), 0);

    log_store.purge(create_log_id(1, 1, 5)).await?;
    assert_eq!(log_store.get_last_purged_log_id()?.unwrap().index, 5);
    let after_purge = log_store.try_get_log_entries(1..=10).await?;
    assert_eq!(after_purge.len(), 5);
    assert_eq!(after_purge[0].log_id.index, 6);

    let new: Vec<_> = (11..=13).map(|i| create_entry(2, 1, i)).collect();
    append_entries(&mut log_store, new).await?;
    let final_logs = log_store.try_get_log_entries(6..=13).await?;
    assert_eq!(final_logs.len(), 8);
    assert_eq!(final_logs[0].log_id.leader_id.term, 1);
    assert_eq!(final_logs[5].log_id.leader_id.term, 2);

    Ok(())
  }

  #[test]
  fn test_set_and_get_last_purged() -> Result<(), io::Error> {
    let log_store = create_test_log_store();

    assert!(log_store.get_last_purged_log_id()?.is_none());

    let log_id = create_log_id(1, 1, 100);
    log_store.set_last_purged_log_id(&log_id)?;

    let retrieved = log_store.get_last_purged_log_id()?.unwrap();
    assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
    assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(retrieved.index, log_id.index);

    let new_log_id = create_log_id(2, 2, 200);
    log_store.set_last_purged_log_id(&new_log_id).unwrap();

    let updated = log_store.get_last_purged_log_id()?.unwrap();
    assert_eq!(updated.index, new_log_id.index);

    Ok(())
  }

  #[test]
  fn test_set_and_get_committed() -> Result<(), io::Error> {
    let log_store = create_test_log_store();

    assert!(log_store.get_committed()?.is_none());

    let log_id = create_log_id(1, 1, 100);
    log_store.set_committed(&Some(log_id))?;

    let retrieved = log_store.get_committed()?.unwrap();
    assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
    assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(retrieved.index, log_id.index);

    let new_log_id = create_log_id(2, 2, 200);
    log_store.set_committed(&Some(new_log_id))?;

    let updated = log_store.get_committed()?.unwrap();
    assert_eq!(updated.index, new_log_id.index);

    log_store.set_committed(&None)?;
    assert!(log_store.get_committed()?.is_none());

    Ok(())
  }

  #[test]
  fn test_set_and_get_vote() -> Result<(), io::Error> {
    let mut log_store = create_test_log_store();

    assert!(log_store.get_vote()?.is_none());

    let vote = Vote::new(1, 1);
    log_store.set_vote(&vote)?;

    let retrieved = log_store.get_vote()?.unwrap();
    assert_eq!(retrieved.leader_id().term, vote.leader_id().term);
    assert_eq!(retrieved.leader_id().node_id, vote.leader_id().node_id);

    let new_vote = Vote::new(2, 2);
    log_store.set_vote(&new_vote)?;

    let updated = log_store.get_vote()?.unwrap();
    assert_eq!(updated.leader_id().term, 2);
    assert_eq!(updated.leader_id().node_id, 2);

    Ok(())
  }
}
