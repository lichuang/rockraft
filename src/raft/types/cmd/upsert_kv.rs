use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;
use serde::{Deserialize, Serialize};

use super::meta::MetaSpec;
use super::operation::Operation;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpsertKV {
  pub key: String,

  /// The value to set. A `None` indicates to delete it.
  pub value: Operation<Vec<u8>>,

  /// Meta data of a value.
  pub value_meta: Option<MetaSpec>,
}

impl fmt::Display for UpsertKV {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "{} = {:?} ({})",
      self.key,
      self.value,
      self.value_meta.display()
    )
  }
}

impl UpsertKV {
  pub fn new(key: impl ToString, value: Operation<Vec<u8>>, value_meta: Option<MetaSpec>) -> Self {
    Self {
      key: key.to_string(),
      value,
      value_meta,
    }
  }

  pub fn insert(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
      value_meta: None,
    }
  }

  pub fn update(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
      value_meta: None,
    }
  }

  pub fn delete(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Delete,
      value_meta: None,
    }
  }

  pub fn with_expire_sec(self, expire_at_sec: u64) -> Self {
    self.with(MetaSpec::new_expire(expire_at_sec))
  }

  /// Set the time to last for the value.
  /// When the ttl is passed, the value is deleted.
  pub fn with_ttl(self, ttl: Duration) -> Self {
    self.with(MetaSpec::new_ttl(ttl))
  }
}

/// Append something to Self.
///
/// For handily constructing message.
pub trait With<T> {
  fn with(self, sth: T) -> Self;
}

impl With<MetaSpec> for UpsertKV {
  fn with(mut self, meta: MetaSpec) -> Self {
    self.value_meta = Some(meta);
    self
  }
}
