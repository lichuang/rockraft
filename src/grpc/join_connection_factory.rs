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
use tracing::info;

pub struct JoinConnectionFactory;

impl JoinConnectionFactory {
  pub async fn create_rpc_channel(
    addr: impl ToString,
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
    addr: impl ToString,
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
