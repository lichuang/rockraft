use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct RpcClientTlsConfig {
  pub rpc_tls_server_root_ca_cert: String,
  pub domain_name: String,
}

impl RpcClientTlsConfig {
  pub fn enabled(&self) -> bool {
    !self.rpc_tls_server_root_ca_cert.is_empty() && !self.domain_name.is_empty()
  }
}

#[derive(Clone, Debug)]
pub struct RpcClientConf {
  pub embedded_dir: Option<String>,
  pub endpoints: Vec<String>,
  pub username: String,
  pub password: String,
  pub tls_conf: Option<RpcClientTlsConfig>,

  /// Timeout for an RPC
  pub timeout: Option<Duration>,
  /// AutoSyncInterval is the interval to update endpoints with its latest members.
  /// None disables auto-sync.
  pub auto_sync_interval: Option<Duration>,
  pub unhealthy_endpoint_evict_time: Duration,
}

impl RpcClientConf {
  /// Whether a remote metasrv is specified.
  ///
  /// - `endpoints` accepts multiple endpoint candidates.
  ///
  /// If endpoints is configured(non-empty), use remote metasrv.
  /// Otherwise, use a local embedded meta
  pub fn local_mode(&self) -> bool {
    self.endpoints.is_empty()
  }

  /// Returns a list of endpoints.
  pub fn get_endpoints(&self) -> Vec<String> {
    self.endpoints.clone()
  }

  pub fn empty() -> Self {
    Self {
      embedded_dir: None,
      endpoints: vec![],
      username: "".to_string(),
      password: "".to_string(),
      tls_conf: None,
      timeout: None,
      auto_sync_interval: None,
      unhealthy_endpoint_evict_time: Default::default(),
    }
  }
}
