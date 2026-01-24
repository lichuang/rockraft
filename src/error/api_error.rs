use super::NetworkError;
use crate::raft::types::ForwardToLeader;
use anyerror::AnyError;

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum APIError {
  /// If a request can only be dealt with by a leader, it informs the caller to forward the request to a leader.
  #[error(transparent)]
  ForwardToLeader(#[from] ForwardToLeader),

  #[error("can not forward any more: {0}")]
  CanNotForward(AnyError),

  #[error(transparent)]
  NetworkError(#[from] NetworkError),
}

impl APIError {
  pub fn is_retryable(&self) -> bool {
    match self {
      APIError::CanNotForward(_) => {
        // Leader is not ready, wait a while and retry
        true
      }
      APIError::ForwardToLeader(_) => {
        // Leader is changing, wait a while and retry
        true
      }
      _ => false,
    }
  }
}
