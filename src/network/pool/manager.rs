use mobc::Manager;
use tonic::transport::Channel;

use crate::error::Error;
use crate::raft::protobuf::raft_service_client::RaftServiceClient;

pub struct RaftServiceManager {
  pub addr: String,
  pub max_message_size: usize,
}

impl RaftServiceManager {
  pub fn new(addr: String, max_message_size: usize) -> Self {
    Self {
      addr,
      max_message_size,
    }
  }
}

#[tonic::async_trait]
impl Manager for RaftServiceManager {
  type Connection = RaftServiceClient<Channel>;
  type Error = Error;

  async fn connect(&self) -> Result<Self::Connection, Self::Error> {
    // `RaftServiceClient::connect()` does not allow configuring message
    // limits, so build the channel explicitly and raise the tonic default
    // 4MB limit: AppendEntries can carry far larger payloads.
    let channel = Channel::from_shared(format!("http://{}", self.addr))
      .map_err(|e| Error::config(format!("Invalid address {}: {}", self.addr, e)))?
      .connect()
      .await
      .map_err(Error::retryable)?;

    Ok(
      RaftServiceClient::new(channel)
        .max_decoding_message_size(self.max_message_size)
        .max_encoding_message_size(self.max_message_size),
    )
  }

  async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
    Ok(conn)
  }
}
