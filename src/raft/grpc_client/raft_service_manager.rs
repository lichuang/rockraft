use mobc::Manager;
use tonic::transport::Channel;

use crate::{error::RockRaftError, raft::protobuf::raft_service_client::RaftServiceClient};

pub struct RaftServiceManager {
  pub addr: String,
}

impl RaftServiceManager {
  pub fn new(addr: String) -> Self {
    Self { addr }
  }
}

#[tonic::async_trait]
impl Manager for RaftServiceManager {
  type Connection = RaftServiceClient<Channel>;
  type Error = RockRaftError;

  async fn connect(&self) -> Result<Self::Connection, Self::Error> {
    match RaftServiceClient::connect(format!("http://{}", self.addr.clone())).await {
      Ok(client) => {
        return Ok(client);
      }
      Err(err) => {
        return Err(RockRaftError::TonicTransport(err));
      }
    };
  }

  async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
    Ok(conn)
  }
}
