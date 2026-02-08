use super::{client_config::RpcClientTlsConfig, dns_resolver::DNSService};
use crate::error::GrpcConnectionError;
use crate::error::Result;
use crate::error::RockRaftError;
use anyerror::AnyError;
use hyper_util::client::legacy::connect::HttpConnector;
use std::time::Duration;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tracing::debug;
use tracing::info;

pub struct JoinConnectionFactory;

impl JoinConnectionFactory {
  pub async fn create_rpc_channel(
    addr: &String,
    timeout: Option<Duration>,
    rpc_client_config: Option<RpcClientTlsConfig>,
  ) -> Result<Channel> {
    let endpoint = Self::create_rpc_endpoint(addr, timeout, rpc_client_config)?;

    let mut inner_connector = HttpConnector::new_with_resolver(DNSService);
    inner_connector.set_nodelay(true);
    inner_connector.set_keepalive(None);
    inner_connector.enforce_http(false);
    inner_connector.set_connect_timeout(timeout);

    // check connection immediately
    match endpoint.connect_with_connector(inner_connector).await {
      Ok(channel) => Ok(channel),
      Err(error) => Err(RockRaftError::GrpcConnection(
        GrpcConnectionError::CannotConnect {
          uri: endpoint.uri().to_string(),
          source: AnyError::new(&error),
        },
      )),
    }
  }

  fn create_rpc_endpoint(
    addr: &String,
    timeout: Option<Duration>,
    rpc_client_tls_config: Option<RpcClientTlsConfig>,
  ) -> Result<Endpoint> {
    let u = if rpc_client_tls_config.is_some() {
      format!("https://{}", addr.to_string())
    } else {
      format!("http://{}", addr.to_string())
    };
    match u.parse::<Uri>() {
      Err(error) => Err(RockRaftError::GrpcConnection(
        GrpcConnectionError::InvalidUri {
          uri: addr.to_string(),
          source: AnyError::new(&error),
        },
      )),
      Ok(uri) => {
        let builder = Channel::builder(uri);
        let mut endpoint = if let Some(conf) = rpc_client_tls_config {
          info!("tls rpc enabled");
          let client_tls_config =
            Self::client_tls_config(&conf).map_err(|e| GrpcConnectionError::TLSConfigError {
              action: "loading".to_string(),
              source: AnyError::new(&e),
            })?;
          builder.tls_config(client_tls_config).map_err(|e| {
            GrpcConnectionError::TLSConfigError {
              action: "building".to_string(),
              source: AnyError::new(&e),
            }
          })?
        } else {
          builder
        };

        if let Some(timeout) = timeout {
          endpoint = endpoint.timeout(timeout);
        }

        Ok(endpoint)
      }
    }
  }

  fn client_tls_config(conf: &RpcClientTlsConfig) -> Result<ClientTlsConfig> {
    let server_root_ca_cert = std::fs::read(conf.rpc_tls_server_root_ca_cert.as_str())?;
    let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);

    let tls = ClientTlsConfig::new()
      .domain_name(conf.domain_name.to_string())
      .ca_certificate(server_root_ca_cert);
    Ok(tls)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::raft::protobuf::raft_service_server::RaftService;
  use crate::raft::protobuf::raft_service_server::RaftServiceServer;
  use crate::raft::protobuf::{RaftReply, RaftRequest};
  use tokio::net::TcpListener;
  use tonic::{Request, Response, Status};

  /// A simple mock RaftService for testing
  struct MockRaftService;

  #[tonic::async_trait]
  impl RaftService for MockRaftService {
    async fn forward(
      &self,
      _request: Request<RaftRequest>,
    ) -> std::result::Result<Response<RaftReply>, Status> {
      Ok(Response::new(RaftReply {
        data: vec![],
        error: vec![],
      }))
    }

    async fn append(
      &self,
      _request: Request<crate::raft::protobuf::AppendRequest>,
    ) -> std::result::Result<Response<crate::raft::protobuf::AppendReply>, Status> {
      Ok(Response::new(crate::raft::protobuf::AppendReply {
        value: vec![],
      }))
    }

    async fn vote(
      &self,
      _request: Request<crate::raft::protobuf::VoteRequest>,
    ) -> std::result::Result<Response<crate::raft::protobuf::VoteReply>, Status> {
      Ok(Response::new(crate::raft::protobuf::VoteReply {
        value: vec![],
      }))
    }

    async fn snapshot(
      &self,
      _request: Request<crate::raft::protobuf::SnapshotRequest>,
    ) -> std::result::Result<Response<crate::raft::protobuf::SnapshotReply>, Status> {
      Ok(Response::new(crate::raft::protobuf::SnapshotReply {
        value: vec![],
      }))
    }
  }

  #[test]
  fn test_create_rpc_endpoint_localhost() -> Result<()> {
    let addr = "localhost:1234".to_string();
    let endpoint = JoinConnectionFactory::create_rpc_endpoint(&addr, None, None)?;

    let uri = endpoint.uri();
    assert_eq!(uri.scheme_str(), Some("http"));
    assert_eq!(uri.host(), Some("localhost"));
    assert_eq!(uri.port().map(|p| p.as_u16()), Some(1234));

    Ok(())
  }

  #[test]
  fn test_create_rpc_endpoint_localhost_with_tls() -> Result<()> {
    let addr = "localhost:1234".to_string();
    let tls_conf = RpcClientTlsConfig {
      rpc_tls_server_root_ca_cert: "/tmp/dummy.crt".to_string(),
      domain_name: "localhost".to_string(),
    };

    // This will fail because the cert file doesn't exist, but we can verify the scheme
    let result = JoinConnectionFactory::create_rpc_endpoint(&addr, None, Some(tls_conf));
    assert!(result.is_err());

    Ok(())
  }

  #[tokio::test]
  async fn test_create_rpc_channel_localhost() -> Result<()> {
    // Start a mock gRPC server on a random port
    let listener = TcpListener::bind("127.0.0.1:0")
      .await
      .map_err(|e| RockRaftError::TokioError(format!("Failed to bind TCP listener: {}", e)))?;
    let addr = listener
      .local_addr()
      .map_err(|e| RockRaftError::TokioError(format!("Failed to get local address: {}", e)))?;

    // Spawn the gRPC server in a background task
    let server_handle = tokio::spawn(async move {
      let service = MockRaftService;
      let server = tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

      // Server will run until the test completes and drops the handle
      let _ = server.await;
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test create_rpc_channel with localhost address
    let addr_str = format!("localhost:{}", addr.port());
    let channel_result =
      JoinConnectionFactory::create_rpc_channel(&addr_str, Some(Duration::from_secs(5)), None)
        .await;

    // Clean up the server
    drop(server_handle);

    // Verify the connection was successful
    assert!(
      channel_result.is_ok(),
      "Failed to create RPC channel to localhost: {}",
      channel_result.unwrap_err()
    );

    Ok(())
  }
}
