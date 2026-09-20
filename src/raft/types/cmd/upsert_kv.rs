use std::fmt;

use serde::{Deserialize, Serialize};

use super::operation::Operation;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpsertKV {
  pub key: String,

  /// The value to set. A `None` indicates to delete it.
  pub value: Operation<Vec<u8>>,
}

impl fmt::Display for UpsertKV {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{} = {:?}", self.key, self.value)
  }
}

impl UpsertKV {
  pub fn insert(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
    }
  }

  pub fn update(key: impl ToString, value: &[u8]) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Update(value.to_vec()),
    }
  }

  pub fn delete(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      value: Operation::Delete,
    }
  }
}
