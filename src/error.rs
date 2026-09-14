//! Error handling for RockRaft
//!
//! This module provides a unified error type that hides internal complexity
//! behind a simple, user-facing interface.

use std::error::Error as StdError;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::result::Result as StdResult;

/// Public error type for RockRaft operations
///
/// This is a deep module: it presents a simple interface (just `is_retryable()`)
/// while internally handling complex error categorization and conversion.
#[derive(Debug)]
pub struct Error {
  kind: ErrorKind,
  source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
  /// Check if this error is retryable
  ///
  /// This is the primary decision point for error handling:
  /// - `true`: Temporary issue (leader election, network glitch), retry the operation
  /// - `false`: Permanent issue (config error, internal bug), don't retry
  pub fn is_retryable(&self) -> bool {
    matches!(
      self.kind,
      ErrorKind::Retryable { .. } | ErrorKind::ForwardToLeader { .. }
    )
  }

  /// Get the error kind
  pub fn kind(&self) -> &ErrorKind {
    &self.kind
  }

  /// If this error carries a redirect to a known leader, return its node id.
  ///
  /// Returns `None` for all other errors, and also for a redirect whose
  /// leader is not yet known (callers should retry and re-resolve).
  pub(crate) fn forward_leader_id(&self) -> Option<u64> {
    match self.kind {
      ErrorKind::ForwardToLeader { leader_id } => leader_id,
      _ => None,
    }
  }

  // Internal constructors

  /// Create a retryable error
  pub(crate) fn retryable<E>(source: E) -> Self
  where
    E: StdError + Send + Sync + 'static,
  {
    Self {
      kind: ErrorKind::Retryable {
        reason: RetryReason::Transient,
      },
      source: Some(Box::new(source)),
    }
  }

  /// Create a retryable error with specific reason
  pub(crate) fn retryable_with_reason(reason: RetryReason) -> Self {
    Self {
      kind: ErrorKind::Retryable { reason },
      source: None,
    }
  }

  /// Create a configuration error
  pub(crate) fn config(msg: impl Into<String>) -> Self {
    Self {
      kind: ErrorKind::InvalidConfig(msg.into()),
      source: None,
    }
  }

  /// Create an internal error
  pub(crate) fn internal(msg: impl Into<String>) -> Self {
    Self {
      kind: ErrorKind::Internal(msg.into()),
      source: None,
    }
  }

  /// Create an internal error with source
  pub(crate) fn internal_with_source<E>(msg: impl Into<String>, source: E) -> Self
  where
    E: StdError + Send + Sync + 'static,
  {
    Self {
      kind: ErrorKind::Internal(msg.into()),
      source: Some(Box::new(source)),
    }
  }

  /// Create a forward-to-leader redirect error
  ///
  /// `leader_id` is the current leader if known; `None` means the request
  /// should be retried until a leader is resolved.
  pub(crate) fn forward_to_leader(leader_id: Option<u64>) -> Self {
    Self {
      kind: ErrorKind::ForwardToLeader { leader_id },
      source: None,
    }
  }
}

impl Display for Error {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.kind)?;
    if let Some(ref source) = self.source {
      write!(f, ": {}", source)?;
    }
    Ok(())
  }
}

impl StdError for Error {
  fn source(&self) -> Option<&(dyn StdError + 'static)> {
    self.source.as_ref().map(|s| s.as_ref() as _)
  }
}

/// Error classification - tells users how to handle the error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
  /// Temporary error that may resolve on retry
  ///
  /// Examples: leader election in progress, temporary network partition,
  /// connection refused while node is starting up
  Retryable { reason: RetryReason },

  /// This node is not the leader; the request must be forwarded.
  ///
  /// Raised when a node accepted a write believing it was the leader but
  /// had already stepped down. Carries the current leader when known so
  /// callers can redirect directly instead of waiting for metrics.
  ForwardToLeader { leader_id: Option<u64> },

  /// Configuration error - check your settings
  ///
  /// Examples: invalid endpoint format, missing required config,
  /// node ID mismatch
  InvalidConfig(String),

  /// Internal error - typically indicates a bug
  ///
  /// Examples: serialization failure, storage corruption,
  /// invariant violation
  Internal(String),
}

impl Display for ErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      ErrorKind::Retryable { reason } => write!(f, "retryable error: {}", reason),
      ErrorKind::ForwardToLeader { leader_id } => {
        write!(f, "forward to leader: {:?}", leader_id)
      }
      ErrorKind::InvalidConfig(msg) => write!(f, "configuration error: {}", msg),
      ErrorKind::Internal(msg) => write!(f, "internal error: {}", msg),
    }
  }
}

/// Specific reasons for retryable errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryReason {
  /// No leader currently elected
  NoLeader,
  /// Leader is changing
  LeaderTransition,
  /// Temporary network issue
  Transient,
  /// Target node is still starting up
  NodeStarting,
}

impl Display for RetryReason {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      RetryReason::NoLeader => write!(f, "no leader available"),
      RetryReason::LeaderTransition => write!(f, "leader transition in progress"),
      RetryReason::Transient => write!(f, "temporary failure"),
      RetryReason::NodeStarting => write!(f, "target node is starting"),
    }
  }
}

/// Result type alias for RockRaft operations
pub type Result<T> = StdResult<T, Error>;

// =============================================================================
// Internal error types - not exported, used for module-internal conversions
// =============================================================================

/// Internal API errors from Raft operations
///
/// This is the single wire format for errors returned in gRPC
/// `RaftReply.error`: the server postcard-encodes it, and every client
/// site decodes it back, so redirect and retryability information
/// survives the round trip.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ApiError {
  /// The request could not be forwarded right now (transient).
  #[error("cannot forward request: {0}")]
  CannotForward(String),

  /// The receiving node is not the leader; forward to `leader_id`.
  #[error("forward to leader: {leader_id:?}")]
  ForwardToLeader { leader_id: Option<u64> },

  /// Non-retryable failure on the receiving node.
  #[error("internal error: {0}")]
  Internal(String),
}

impl ApiError {
  /// Classify a local `Error` into its wire representation.
  ///
  /// Redirects keep their leader id; transient errors become
  /// `CannotForward`; everything else becomes a non-retryable `Internal`.
  pub(crate) fn from_error(e: &Error) -> Self {
    match e.kind() {
      ErrorKind::ForwardToLeader { leader_id } => ApiError::ForwardToLeader {
        leader_id: *leader_id,
      },
      ErrorKind::Retryable { .. } => ApiError::CannotForward(e.to_string()),
      ErrorKind::InvalidConfig(_) | ErrorKind::Internal(_) => ApiError::Internal(e.to_string()),
    }
  }
}

impl From<ApiError> for Error {
  fn from(e: ApiError) -> Self {
    match e {
      // Keep the redirect target so callers can forward directly.
      ApiError::ForwardToLeader { leader_id } => Self::forward_to_leader(leader_id),
      ApiError::CannotForward(_) => Self::retryable_with_reason(RetryReason::Transient),
      ApiError::Internal(msg) => Self::internal(msg),
    }
  }
}

// =============================================================================
// Conversions from external error types
// =============================================================================

impl From<io::Error> for Error {
  fn from(e: io::Error) -> Self {
    // IO errors are generally retryable (network issues, etc.)
    Self::retryable(e)
  }
}

impl From<tonic::transport::Error> for Error {
  fn from(e: tonic::transport::Error) -> Self {
    Self::retryable(e)
  }
}

impl From<tonic::Status> for Error {
  fn from(status: tonic::Status) -> Self {
    match status.code() {
      // Unavailable = service temporarily down, retryable
      tonic::Code::Unavailable => Self::retryable_with_reason(RetryReason::Transient),
      // Invalid argument = config issue
      tonic::Code::InvalidArgument => Self::config(status.message()),
      // Everything else = internal error
      _ => Self::internal(format!("gRPC error: {}", status)),
    }
  }
}

impl From<postcard::Error> for Error {
  fn from(e: postcard::Error) -> Self {
    // Serialization errors are typically internal bugs
    Self::internal_with_source("serialization failed", e)
  }
}

// =============================================================================
// Legacy type aliases for backward compatibility during migration
// =============================================================================

/// Deprecated: Use `Error` instead
pub type RockRaftError = Error;

/// Deprecated: Use `Result<T>` instead
pub type RockRaftResult<T> = Result<T>;

#[cfg(test)]
mod tests {
  use super::*;
  use crate::raft::types::{decode, encode};

  #[test]
  fn test_forward_to_leader_kind() {
    let err = Error::forward_to_leader(Some(3));
    assert!(err.is_retryable());
    assert_eq!(err.forward_leader_id(), Some(3));

    let err = Error::forward_to_leader(None);
    assert!(err.is_retryable());
    assert_eq!(err.forward_leader_id(), None);
  }

  #[test]
  fn test_non_redirect_errors_have_no_leader_id() {
    assert_eq!(Error::internal("boom").forward_leader_id(), None);
    assert_eq!(Error::config("bad").forward_leader_id(), None);
    assert!(!Error::internal("boom").is_retryable());
  }

  #[test]
  fn test_api_error_from_error_classification() {
    let api = ApiError::from_error(&Error::forward_to_leader(Some(5)));
    assert_eq!(api, ApiError::ForwardToLeader { leader_id: Some(5) });
    assert!(Error::from(api).is_retryable());

    let api = ApiError::from_error(&Error::retryable_with_reason(RetryReason::NoLeader));
    assert!(matches!(api, ApiError::CannotForward(_)));
    assert!(Error::from(api).is_retryable());

    let api = ApiError::from_error(&Error::internal("storage corrupt"));
    assert!(matches!(api, ApiError::Internal(_)));
    assert!(!Error::from(api).is_retryable());
  }

  #[test]
  fn test_api_error_wire_roundtrip_preserves_redirect() {
    let api = ApiError::ForwardToLeader { leader_id: Some(7) };
    let bytes = encode(&api).unwrap();
    let decoded: ApiError = decode(&bytes).unwrap();
    assert_eq!(decoded, api);

    // The full server->wire->client path keeps the redirect target.
    let err = Error::from(decoded);
    assert_eq!(err.forward_leader_id(), Some(7));
    assert!(err.is_retryable());
  }
}
