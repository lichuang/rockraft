use openraft::AnyError;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::StorageError;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;

use crate::types::RaftCodec;
use crate::types::TypeConfig;

/// Defines metadata key and value
pub trait StoreMeta {
  /// The key used to store in rocksdb
  const KEY: &'static str;

  /// The type of the value to store
  type Value: RaftCodec;

  /// The subject this meta belongs to, and will be embedded into the returned storage error.
  fn subject(v: Option<&Self::Value>) -> ErrorSubject<TypeConfig>;

  fn read_err(e: impl std::error::Error + 'static) -> StorageError<TypeConfig> {
    StorageError::new(Self::subject(None), ErrorVerb::Read, AnyError::new(&e))
  }

  fn write_err(v: &Self::Value, e: impl std::error::Error + 'static) -> StorageError<TypeConfig> {
    StorageError::new(Self::subject(Some(v)), ErrorVerb::Write, AnyError::new(&e))
  }
}

pub(crate) struct LastPurged {}
pub(crate) struct Vote {}

impl StoreMeta for LastPurged {
  const KEY: &'static str = "last_purged_log_id";
  type Value = LogIdOf<TypeConfig>;

  fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
    ErrorSubject::Store
  }
}

impl StoreMeta for Vote {
  const KEY: &'static str = "vote";
  type Value = VoteOf<TypeConfig>;

  fn subject(_v: Option<&Self::Value>) -> ErrorSubject<TypeConfig> {
    ErrorSubject::Vote
  }
}
