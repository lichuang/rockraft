use std::error::Error;
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
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use rocksdb::ColumnFamily;
use rocksdb::DB;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use tokio::task::spawn_blocking;

use super::StoreMeta;
use super::keys::LOG_DATA_FAMILY;
use super::keys::LOG_META_FAMILY;
use super::meta::LastPurged;
use crate::raft::types::Entry;
use crate::raft::types::LogState;
use crate::raft::types::RaftCodec;
use crate::raft::types::TypeConfig;
use crate::raft::types::read_logs_err;

#[derive(Debug, Clone)]
pub struct RocksLogStore<C>
where C: RaftTypeConfig
{
  pub db: Arc<DB>,
  _p: PhantomData<C>,
}

impl RocksLogStore<TypeConfig> {
  pub fn new(db: Arc<DB>) -> Result<Self, std::io::Error> {
    db.cf_handle(LOG_META_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_meta` not found"))?;
    db.cf_handle(LOG_DATA_FAMILY)
      .ok_or_else(|| std::io::Error::other("column family `_log_data` not found"))?;

    Ok(Self {
      db,
      _p: Default::default(),
    })
  }

  fn cf_meta(&self) -> &ColumnFamily {
    self.db.cf_handle(LOG_META_FAMILY).unwrap()
  }

  fn cf_logs(&self) -> &ColumnFamily {
    self.db.cf_handle(LOG_DATA_FAMILY).unwrap()
  }

  /// Get a store metadata.
  ///
  /// It returns `None` if the store does not have such a metadata stored.
  fn get_meta<M: StoreMeta>(&self) -> Result<Option<M::Value>, io::Error> {
    let bytes = self
      .db
      .get_cf(self.cf_meta(), M::KEY)
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
      .put_cf(self.cf_meta(), M::KEY, encode_valude)
      .map_err(|e| M::write_err(value, e))?;

    Ok(())
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
    self.get_meta::<super::meta::Vote>()
  }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore<TypeConfig> {
  type LogReader = Self;

  async fn get_log_state(&mut self) -> Result<LogState, io::Error> {
    let last = self
      .db
      .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
      .next();

    let last_log_id = match last {
      None => None,
      Some(res) => {
        let (_log_index, val) = res.map_err(read_logs_err)?;
        let entry: Entry = deserialize(val.as_ref()).map_err(read_logs_err)?;
        Some(entry.log_id)
      }
    };

    let last_purged_log_id = self.get_meta::<LastPurged>()?;

    let last_log_id = match last_log_id {
      None => last_purged_log_id.clone(),
      Some(x) => Some(x),
    };

    Ok(LogState {
      last_purged_log_id,
      last_log_id,
    })
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
          self.cf_logs(),
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
      .delete_range_cf(self.cf_logs(), &from, &to)
      .map_err(read_logs_err)?;

    // Truncating does not need to be persisted.
    Ok(())
  }

  async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
    tracing::debug!("delete_log: [0, {:?}]", log_id);

    // Write the last-purged log id before purging the logs.
    // The logs at and before last-purged log id will be ignored by openraft.
    // Therefore, there is no need to do it in a transaction.
    self.put_meta::<LastPurged>(&log_id)?;

    let from = id_to_bin(0);
    let to = id_to_bin(log_id.index() + 1);
    self
      .db
      .delete_range_cf(self.cf_logs(), &from, &to)
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
