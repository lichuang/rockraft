use std::fmt;

use serde::{Deserialize, Serialize};

use super::upsert_kv::UpsertKV;

/// Transaction operation types for condition checking
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum TxnOp {
  /// Key exists
  Exists,
  /// Key does not exist
  NotExists,
  /// Value equals expected
  Equal(Vec<u8>),
  /// Value not equals expected
  NotEqual(Vec<u8>),
  /// Value greater than expected (lexicographical order)
  Greater(Vec<u8>),
  /// Value less than expected (lexicographical order)
  Less(Vec<u8>),
  /// Value greater than or equal to expected
  GreaterEqual(Vec<u8>),
  /// Value less than or equal to expected
  LessEqual(Vec<u8>),
}

impl fmt::Debug for TxnOp {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      TxnOp::Exists => write!(f, "Exists"),
      TxnOp::NotExists => write!(f, "NotExists"),
      TxnOp::Equal(_) => write!(f, "Equal([binary])"),
      TxnOp::NotEqual(_) => write!(f, "NotEqual([binary])"),
      TxnOp::Greater(_) => write!(f, "Greater([binary])"),
      TxnOp::Less(_) => write!(f, "Less([binary])"),
      TxnOp::GreaterEqual(_) => write!(f, "GreaterEqual([binary])"),
      TxnOp::LessEqual(_) => write!(f, "LessEqual([binary])"),
    }
  }
}

/// A condition to check in a transaction
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TxnCondition {
  /// The key to check
  pub key: String,
  /// The expected operation/condition
  pub expected: TxnOp,
}

impl TxnCondition {
  /// Create a new condition that checks if key exists
  pub fn exists(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::Exists,
    }
  }

  /// Create a new condition that checks if key does not exist
  pub fn not_exists(key: impl ToString) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::NotExists,
    }
  }

  /// Create a new condition that checks if value equals expected
  pub fn eq(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::Equal(value.as_ref().to_vec()),
    }
  }

  /// Create a new condition that checks if value not equals expected
  pub fn ne(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::NotEqual(value.as_ref().to_vec()),
    }
  }

  /// Create a new condition that checks if value is greater than expected
  pub fn gt(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::Greater(value.as_ref().to_vec()),
    }
  }

  /// Create a new condition that checks if value is less than expected
  pub fn lt(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::Less(value.as_ref().to_vec()),
    }
  }

  /// Create a new condition that checks if value is greater than or equal to expected
  pub fn ge(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::GreaterEqual(value.as_ref().to_vec()),
    }
  }

  /// Create a new condition that checks if value is less than or equal to expected
  pub fn le(key: impl ToString, value: impl AsRef<[u8]>) -> Self {
    Self {
      key: key.to_string(),
      expected: TxnOp::LessEqual(value.as_ref().to_vec()),
    }
  }
}

/// Transaction request
///
/// A transaction consists of:
/// - `condition`: A list of conditions to check. All conditions must be met (AND logic).
/// - `if_then`: Operations to execute if all conditions are met.
/// - `else_then`: Operations to execute if any condition is not met.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TxnReq {
  /// Conditions to check (all must be met)
  pub condition: Vec<TxnCondition>,
  /// Operations to execute if conditions are met
  pub if_then: Vec<UpsertKV>,
  /// Operations to execute if conditions are not met
  pub else_then: Vec<UpsertKV>,
}

impl TxnReq {
  /// Create a new transaction request with conditions
  pub fn new(condition: Vec<TxnCondition>) -> Self {
    Self {
      condition,
      if_then: Vec::new(),
      else_then: Vec::new(),
    }
  }

  /// Add an operation to execute if conditions are met
  pub fn if_then(mut self, op: UpsertKV) -> Self {
    self.if_then.push(op);
    self
  }

  /// Add multiple operations to execute if conditions are met
  pub fn if_then_ops(mut self, ops: Vec<UpsertKV>) -> Self {
    self.if_then.extend(ops);
    self
  }

  /// Add an operation to execute if conditions are not met
  pub fn else_then(mut self, op: UpsertKV) -> Self {
    self.else_then.push(op);
    self
  }

  /// Add multiple operations to execute if conditions are not met
  pub fn else_then_ops(mut self, ops: Vec<UpsertKV>) -> Self {
    self.else_then.extend(ops);
    self
  }
}

/// Transaction execution result
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TxnReply {
  /// Transaction succeeded
  Success {
    /// Which branch was executed: true = if_then, false = else_then
    branch: bool,
    /// Previous values of keys that were modified (in order of operations)
    prev_values: Vec<Option<Vec<u8>>>,
  },
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_txn_condition_builders() {
    let cond = TxnCondition::exists("key1");
    assert_eq!(cond.key, "key1");
    assert!(matches!(cond.expected, TxnOp::Exists));

    let cond = TxnCondition::not_exists("key2");
    assert_eq!(cond.key, "key2");
    assert!(matches!(cond.expected, TxnOp::NotExists));

    let cond = TxnCondition::eq("key3", b"value");
    assert_eq!(cond.key, "key3");
    assert!(matches!(cond.expected, TxnOp::Equal(v) if v == b"value"));

    let cond = TxnCondition::ne("key4", b"value");
    assert_eq!(cond.key, "key4");
    assert!(matches!(cond.expected, TxnOp::NotEqual(v) if v == b"value"));

    let cond = TxnCondition::gt("key5", b"value");
    assert_eq!(cond.key, "key5");
    assert!(matches!(cond.expected, TxnOp::Greater(v) if v == b"value"));

    let cond = TxnCondition::lt("key6", b"value");
    assert_eq!(cond.key, "key6");
    assert!(matches!(cond.expected, TxnOp::Less(v) if v == b"value"));
  }

  #[test]
  fn test_txn_req_builder() {
    let req = TxnReq::new(vec![TxnCondition::exists("key1")])
      .if_then(UpsertKV::insert("k1", b"v1"))
      .else_then(UpsertKV::delete("k2"));

    assert_eq!(req.condition.len(), 1);
    assert_eq!(req.if_then.len(), 1);
    assert_eq!(req.else_then.len(), 1);
  }

  #[test]
  fn test_txn_serialization() {
    use crate::raft::types::{decode, encode};

    let req = TxnReq::new(vec![
      TxnCondition::exists("key1"),
      TxnCondition::eq("key2", b"value2"),
    ])
    .if_then(UpsertKV::insert("k1", b"v1"))
    .else_then(UpsertKV::delete("k2"));

    let serialized = encode(&req).unwrap();
    let deserialized: TxnReq = decode(&serialized).unwrap();
    assert_eq!(req, deserialized);

    let reply = TxnReply::Success {
      branch: true,
      prev_values: vec![Some(b"old".to_vec()), None],
    };
    let serialized = encode(&reply).unwrap();
    let deserialized: TxnReply = decode(&serialized).unwrap();
    assert_eq!(reply, deserialized);
  }
}
