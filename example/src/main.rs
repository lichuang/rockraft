use axum::{
  Router,
  extract::{Query, State},
  http::StatusCode,
  response::{IntoResponse, Json},
  routing::{get, post},
};
use openraft::async_runtime::watch::WatchReceiver;
use rockraft::config::Config as RockraftConfig;
use rockraft::node::RaftNodeBuilder;
use rockraft::raft::types::{
  BatchWriteReq, Cmd, GetKVReq, JoinRequest, LeaveRequest, LogEntry, ScanPrefixReq, TxnCondition,
  TxnReq, UpsertKV,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Log configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogConfig {
  /// Log file path, if not set, logs will be printed to stdout
  pub file: Option<String>,
  /// Log level, default is "info"
  #[serde(default = "default_log_level")]
  pub level: String,
}

fn default_log_level() -> String {
  "info".to_string()
}

impl Default for LogConfig {
  fn default() -> Self {
    Self {
      file: None,
      level: default_log_level(),
    }
  }
}

/// Extended configuration with HTTP address
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
  #[serde(flatten)]
  pub base: RockraftConfig,
  pub http_addr: String,
  /// Log configuration
  #[serde(default)]
  pub log: LogConfig,
}

/// Request for setting a key-value pair
#[derive(Debug, Deserialize)]
pub struct SetValueRequest {
  pub key: String,
  pub value: String,
}

/// Request for leaving the cluster
#[derive(Debug, Deserialize)]
pub struct LeaveRequestHttp {
  pub node_id: u64,
}

/// Request for joining a node to the cluster
#[derive(Debug, Deserialize)]
pub struct JoinRequestHttp {
  pub node_id: u64,
  pub endpoint: String,
}

/// Operation type for batch operations
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchOpType {
  Set,
  Delete,
}

/// Single operation in a batch write request
#[derive(Debug, Deserialize)]
pub struct BatchOp {
  /// Operation type: "set" or "delete"
  pub op: BatchOpType,
  /// Key for the operation
  pub key: String,
  /// Value for the operation (required for "set", ignored for "delete")
  pub value: Option<String>,
}

/// Request for batch write operations
#[derive(Debug, Deserialize)]
pub struct BatchWriteRequest {
  /// List of operations to perform atomically
  pub operations: Vec<BatchOp>,
}

/// Request for getset operation (get old value and set new value atomically)
#[derive(Debug, Deserialize)]
pub struct GetSetRequest {
  pub key: String,
  pub value: String,
}

/// Response for getset operation
#[derive(Debug, Serialize)]
pub struct GetSetResponse {
  pub success: bool,
  pub key: String,
  pub old_value: Option<String>,
  pub new_value: String,
}

/// Condition for transaction
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnConditionOp {
  Exists,
  NotExists,
  Equal,
  NotEqual,
  Greater,
  Less,
  GreaterEqual,
  LessEqual,
}

/// Single condition in a transaction
#[derive(Debug, Deserialize)]
pub struct TxnConditionReq {
  pub key: String,
  pub op: TxnConditionOp,
  /// Value for comparison (not needed for Exists/NotExists)
  pub value: Option<String>,
}

/// Single operation in a transaction
#[derive(Debug, Deserialize)]
pub struct TxnOp {
  /// Operation type: "set" or "delete"
  pub op: BatchOpType,
  pub key: String,
  /// Value for the operation (required for "set", ignored for "delete")
  pub value: Option<String>,
}

/// Request for transaction
#[derive(Debug, Deserialize)]
pub struct TxnRequest {
  /// Conditions to check (all must be met for if_then to execute)
  pub conditions: Vec<TxnConditionReq>,
  /// Operations to execute if conditions are met
  pub if_then: Vec<TxnOp>,
  /// Operations to execute if conditions are not met
  #[serde(default)]
  pub else_then: Vec<TxnOp>,
  /// Whether to return previous values
  #[serde(default)]
  pub return_previous: bool,
}

/// Response for transaction
#[derive(Debug, Serialize)]
pub struct TxnResponse {
  pub success: bool,
  /// Which branch was executed: true = if_then, false = else_then
  pub branch: bool,
  /// Previous values (only populated if return_previous was true)
  pub prev_values: Vec<Option<String>>,
}

/// Response for successful operations
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
  pub success: bool,
  pub message: String,
}

/// Response for get operations
#[derive(Debug, Serialize)]
pub struct GetResponse {
  pub key: String,
  pub value: Option<String>,
}

/// Response for errors
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
  pub error: String,
}

/// Application state containing RaftNode
#[derive(Clone)]
pub struct AppState {
  pub raft_node: Arc<rockraft::node::RaftNode>,
}

impl IntoResponse for ErrorResponse {
  fn into_response(self) -> axum::response::Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(self)).into_response()
  }
}

fn init_logging(config: &LogConfig) -> Result<(), Box<dyn Error>> {
  let env_filter = if std::env::var("RUST_LOG").is_ok() {
    EnvFilter::from_default_env()
  } else {
    EnvFilter::new(&config.level)
  };

  match &config.file {
    Some(log_file) => {
      // Ensure parent directory exists
      if let Some(parent) = Path::new(log_file).parent() {
        fs::create_dir_all(parent)?;
      }

      let file_appender = tracing_appender::rolling::never(
        Path::new(log_file).parent().unwrap_or(Path::new(".")),
        Path::new(log_file).file_name().unwrap_or_default(),
      );

      tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_writer(file_appender))
        .init();

      println!("Logging to file: {}", log_file);
    }
    None => {
      tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
    }
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  // Parse command line arguments first to get config path
  let args: Vec<String> = env::args().collect();
  let config_path = if args.len() > 2 && args[1] == "--conf" {
    args[2].clone()
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    std::process::exit(1);
  };

  // Load configuration first (without logging)
  let config = load_config(&config_path)?;

  // Initialize tracing with config
  init_logging(&config.log)?;

  println!("Configuration loaded:");
  println!("  node_id: {}", config.base.node_id);
  println!("  raft_addr: {}", config.base.raft.endpoint);
  println!("  http_addr: {}", config.http_addr);
  println!("  data_path: {}", config.base.rocksdb.data_path);
  println!("  single: {}", config.base.raft.single);
  println!("  join: {:?}", config.base.raft.join);
  println!("  log: {:?}", config.log);

  // Create Raft node
  info!("Creating Raft node...");
  let raft_node = RaftNodeBuilder::from_config(&config.base).await?;
  info!("Raft node created successfully");

  // Create application state
  let state = AppState {
    raft_node: raft_node.clone(),
  };

  // Build HTTP router
  let app = Router::new()
    .route("/get", get(get_handler))
    .route("/set", post(set_handler))
    .route("/delete", post(delete_handler))
    .route("/batch_write", post(batch_write_handler))
    .route("/txn", post(txn_handler))
    .route("/getset", post(getset_handler))
    .route("/prefix", get(prefix_handler))
    .route("/leave", post(leave_handler))
    .route("/join", post(join_handler))
    .route("/members", get(members_handler))
    .route("/health", get(health_handler))
    .route("/metrics", get(metrics_handler))
    .layer(CorsLayer::permissive())
    .layer(TraceLayer::new_for_http())
    .with_state(state);

  // Start HTTP server
  let http_addr = config.http_addr.clone();
  let listener = tokio::net::TcpListener::bind(&http_addr).await?;
  info!("HTTP server listening on http://{}", http_addr);
  info!("Available endpoints:");
  info!(
    "  GET  http://{}/get?key=<key>    - Get a value by key",
    http_addr
  );
  info!(
    "  POST http://{}/set             - Set a key-value pair",
    http_addr
  );
  info!("  POST http://{}/delete          - Delete a key", http_addr);
  info!(
    "  POST http://{}/batch_write     - Batch write multiple keys atomically",
    http_addr
  );
  info!(
    "  POST http://{}/txn             - Execute transaction with conditions",
    http_addr
  );
  info!(
    "  POST http://{}/getset          - Get old value and set new value atomically",
    http_addr
  );
  info!(
    "  POST http://{}/leave           - Remove a node from cluster",
    http_addr
  );
  info!(
    "  POST http://{}/join            - Add a node to cluster",
    http_addr
  );
  info!(
    "  GET  http://{}/members         - Get cluster members",
    http_addr
  );
  info!("  GET  http://{}/health          - Health check", http_addr);
  info!(
    "  GET  http://{}/metrics         - Cluster metrics",
    http_addr
  );
  info!(
    "  GET  http://{}/prefix?prefix=<prefix> - Scan keys by prefix",
    http_addr
  );

  // Spawn HTTP server
  let http_server = tokio::spawn(async move { axum::serve(listener, app).await });

  // Wait for Ctrl+C
  info!("Press Ctrl+C to shutdown...");
  signal::ctrl_c().await?;

  info!("Shutting down Raft node...");
  raft_node.shutdown().await?;
  info!("Raft node shutdown successfully");

  // Abort HTTP server
  http_server.abort();

  info!("Server shutdown complete");
  Ok(())
}

/// Handler for GET /get endpoint
async fn get_handler(
  Query(params): Query<HashMap<String, String>>,
  State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let key = params.get("key").ok_or_else(|| ErrorResponse {
    error: "Missing 'key' parameter".to_string(),
  })?;

  // Use raft_node.read() which handles reading from the state machine
  let req = GetKVReq { key: key.clone() };
  match state.raft_node.read(req).await {
    Ok(value_opt) => {
      let value_str =
        value_opt.map(|v| String::from_utf8(v).unwrap_or_else(|_| "Invalid UTF-8".to_string()));
      Ok(Json(GetResponse {
        key: key.clone(),
        value: value_str,
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to read: {}", e),
    }),
  }
}

/// Handler for POST /set endpoint
async fn set_handler(
  State(state): State<AppState>,
  Json(payload): Json<SetValueRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  // Create a LogEntry with UpsertKV command
  let upsert_kv = Cmd::UpsertKV(UpsertKV::insert(&payload.key, payload.value.as_bytes()));
  let entry = LogEntry::new(upsert_kv);

  // Use raft_node.write() which handles leader forwarding automatically
  match state.raft_node.write(entry).await {
    Ok(_) => {
      info!("Write successful: key='{}'", payload.key);
      Ok(Json(SuccessResponse {
        success: true,
        message: format!("Key '{}' set successfully", payload.key),
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to write: {}", e),
    }),
  }
}

/// Handler for POST /delete endpoint
async fn delete_handler(
  State(state): State<AppState>,
  Json(payload): Json<HashMap<String, String>>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let key = payload.get("key").ok_or_else(|| ErrorResponse {
    error: "Missing 'key' field".to_string(),
  })?;

  // Create an UpsertKV with Delete operation
  let upsert_kv = Cmd::UpsertKV(UpsertKV::delete(key));
  let entry = LogEntry::new(upsert_kv);

  // Use raft_node.write() which handles leader forwarding automatically
  match state.raft_node.write(entry).await {
    Ok(_) => {
      info!("Delete successful: key='{}'", key);
      Ok(Json(SuccessResponse {
        success: true,
        message: format!("Key '{}' deleted successfully", key),
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to delete: {}", e),
    }),
  }
}

/// Handler for POST /batch_write endpoint
///
/// Request body example:
/// ```json
/// {
///   "operations": [
///     { "op": "set", "key": "key1", "value": "value1" },
///     { "op": "set", "key": "key2", "value": "value2" },
///     { "op": "delete", "key": "key3" }
///   ]
/// }
/// ```
///
/// All operations are applied atomically - either all succeed or all fail.
async fn batch_write_handler(
  State(state): State<AppState>,
  Json(payload): Json<BatchWriteRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  if payload.operations.is_empty() {
    return Ok(Json(SuccessResponse {
      success: true,
      message: "No operations to perform".to_string(),
    }));
  }

  // Convert HTTP request entries to UpsertKV entries
  let mut entries = Vec::with_capacity(payload.operations.len());
  for op in &payload.operations {
    let upsert_kv = match op.op {
      BatchOpType::Set => {
        let value = op.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!("Missing 'value' for set operation on key '{}'", op.key),
        })?;
        UpsertKV::insert(&op.key, value.as_bytes())
      }
      BatchOpType::Delete => UpsertKV::delete(&op.key),
    };
    entries.push(upsert_kv);
  }

  // Create batch write request
  let batch_req = BatchWriteReq { entries };

  // Execute batch write
  match state.raft_node.batch_write(batch_req).await {
    Ok(_) => {
      let op_count = payload.operations.len();
      info!("Batch write successful: {} operations", op_count);
      Ok(Json(SuccessResponse {
        success: true,
        message: format!("Batch write successful: {} operations applied", op_count),
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to batch write: {}", e),
    }),
  }
}

/// Handler for GET /health endpoint
async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
  let metrics = state.raft_node.raft().metrics().borrow_watched().clone();

  Json(serde_json::json!({
    "status": "ok",
    "node_id": state.raft_node.raft().node_id(),
    "is_leader": metrics.current_leader == Some(*state.raft_node.raft().node_id()),
    "current_leader": metrics.current_leader,
    "current_term": metrics.current_term,
    "state": metrics.running_state,
  }))
}

/// Handler for GET /metrics endpoint
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
  let metrics = state.raft_node.raft().metrics().borrow_watched().clone();

  Json(serde_json::json!({
    "node_id": state.raft_node.raft().node_id(),
    "current_leader": metrics.current_leader,
    "current_term": metrics.current_term,
    "last_applied": metrics.last_applied,
    "last_log_index": metrics.last_log_index,
    "state": metrics.running_state,
    "membership": metrics.membership_config,
  }))
}

/// Handler for POST /leave endpoint
async fn leave_handler(
  State(state): State<AppState>,
  Json(payload): Json<LeaveRequestHttp>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let req = LeaveRequest {
    node_id: payload.node_id,
  };

  match state.raft_node.remove_node(req).await {
    Ok(()) => Ok(Json(SuccessResponse {
      success: true,
      message: format!("Node {} left the cluster successfully", payload.node_id),
    })),
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to leave cluster: {}", e),
    }),
  }
}

/// Handler for POST /join endpoint
///
/// Add a node to the raft cluster.
/// This request is forwarded to the leader if the current node is not the leader.
///
/// Request body example:
/// ```json
/// {
///   "node_id": 4,
///   "endpoint": "127.0.0.1:7004"
/// }
/// ```
async fn join_handler(
  State(state): State<AppState>,
  Json(payload): Json<JoinRequestHttp>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let endpoint =
    rockraft::raft::types::Endpoint::parse(&payload.endpoint).map_err(|e| ErrorResponse {
      error: format!("Invalid endpoint '{}': {}", payload.endpoint, e),
    })?;

  let req = JoinRequest {
    node_id: payload.node_id,
    endpoint,
  };

  match state.raft_node.add_node(req).await {
    Ok(()) => Ok(Json(SuccessResponse {
      success: true,
      message: format!("Node {} joined the cluster successfully", payload.node_id),
    })),
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to join cluster: {}", e),
    }),
  }
}

/// Handler for GET /members endpoint
async fn members_handler(
  State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let req = rockraft::raft::types::GetMembersReq {};

  match state.raft_node.get_members(req).await {
    Ok(members) => {
      let members_json: BTreeMap<String, serde_json::Value> = members
        .into_iter()
        .map(|(id, node)| {
          let node_json = serde_json::json!({
            "node_id": node.node_id,
            "endpoint": format!("{}", node.endpoint),
          });
          (id.to_string(), node_json)
        })
        .collect();
      Ok(Json(serde_json::json!({
        "success": true,
        "members": members_json,
      })))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to get members: {}", e),
    }),
  }
}

/// Handler for GET /prefix endpoint
///
/// Query parameters:
/// - prefix: The key prefix to scan
///
/// Returns a JSON array of {key, value} objects for all keys matching the prefix.
async fn prefix_handler(
  Query(params): Query<std::collections::HashMap<String, String>>,
  State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let prefix = params.get("prefix").ok_or_else(|| ErrorResponse {
    error: "Missing 'prefix' parameter".to_string(),
  })?;

  let req = ScanPrefixReq {
    prefix: prefix.clone().into_bytes(),
  };
  match state.raft_node.scan_prefix(req).await {
    Ok(results) => {
      let items: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(key, value)| {
          serde_json::json!({
            "key": String::from_utf8_lossy(&key),
            "value": String::from_utf8_lossy(&value),
          })
        })
        .collect();
      Ok(Json(serde_json::json!({
        "success": true,
        "prefix": prefix,
        "count": items.len(),
        "items": items,
      })))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to scan prefix: {}", e),
    }),
  }
}

/// Handler for POST /txn endpoint
///
/// Execute a transaction with conditional operations.
/// All conditions must be met (AND logic) for if_then operations to execute.
/// Otherwise, else_then operations are executed.
///
/// Request body example:
/// ```json
/// {
///   "conditions": [
///     { "key": "counter", "op": "exists" },
///     { "key": "counter", "op": "equal", "value": "10" }
///   ],
///   "if_then": [
///     { "op": "set", "key": "counter", "value": "20" }
///   ],
///   "else_then": [
///     { "op": "set", "key": "counter", "value": "0" }
///   ],
///   "return_previous": true
/// }
/// ```
///
/// Response example:
/// ```json
/// {
///   "success": true,
///   "branch": true,
///   "prev_values": ["10"]
/// }
/// ```
async fn txn_handler(
  State(state): State<AppState>,
  Json(payload): Json<TxnRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  // Build conditions
  let mut conditions = Vec::new();
  for cond in &payload.conditions {
    let txn_cond = match cond.op {
      TxnConditionOp::Exists => TxnCondition::exists(&cond.key),
      TxnConditionOp::NotExists => TxnCondition::not_exists(&cond.key),
      TxnConditionOp::Equal => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!("Condition 'equal' requires a value for key '{}'", cond.key),
        })?;
        TxnCondition::eq(&cond.key, value.as_bytes())
      }
      TxnConditionOp::NotEqual => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!(
            "Condition 'not_equal' requires a value for key '{}'",
            cond.key
          ),
        })?;
        TxnCondition::ne(&cond.key, value.as_bytes())
      }
      TxnConditionOp::Greater => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!(
            "Condition 'greater' requires a value for key '{}'",
            cond.key
          ),
        })?;
        TxnCondition::gt(&cond.key, value.as_bytes())
      }
      TxnConditionOp::Less => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!("Condition 'less' requires a value for key '{}'", cond.key),
        })?;
        TxnCondition::lt(&cond.key, value.as_bytes())
      }
      TxnConditionOp::GreaterEqual => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!(
            "Condition 'greater_equal' requires a value for key '{}'",
            cond.key
          ),
        })?;
        TxnCondition::ge(&cond.key, value.as_bytes())
      }
      TxnConditionOp::LessEqual => {
        let value = cond.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!(
            "Condition 'less_equal' requires a value for key '{}'",
            cond.key
          ),
        })?;
        TxnCondition::le(&cond.key, value.as_bytes())
      }
    };
    conditions.push(txn_cond);
  }

  // Build if_then operations
  let mut if_then = Vec::new();
  for op in &payload.if_then {
    let upsert = match op.op {
      BatchOpType::Set => {
        let value = op.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!("Set operation requires a value for key '{}'", op.key),
        })?;
        UpsertKV::insert(&op.key, value.as_bytes())
      }
      BatchOpType::Delete => UpsertKV::delete(&op.key),
    };
    if_then.push(upsert);
  }

  // Build else_then operations
  let mut else_then = Vec::new();
  for op in &payload.else_then {
    let upsert = match op.op {
      BatchOpType::Set => {
        let value = op.value.as_ref().ok_or_else(|| ErrorResponse {
          error: format!("Set operation requires a value for key '{}'", op.key),
        })?;
        UpsertKV::insert(&op.key, value.as_bytes())
      }
      BatchOpType::Delete => UpsertKV::delete(&op.key),
    };
    else_then.push(upsert);
  }

  // Build transaction request
  let mut txn = TxnReq::new(conditions)
    .if_then_ops(if_then)
    .else_then_ops(else_then);
  if payload.return_previous {
    txn = txn.with_return_previous();
  }

  // Execute transaction
  match state.raft_node.txn(txn).await {
    Ok(rockraft::raft::types::TxnReply::Success {
      branch,
      prev_values,
    }) => {
      let prev_strings: Vec<Option<String>> = prev_values
        .into_iter()
        .map(|v| {
          v.map(|bytes| String::from_utf8(bytes).unwrap_or_else(|_| "Invalid UTF-8".to_string()))
        })
        .collect();

      info!(
        "Transaction successful: branch={}, prev_values={:?}",
        branch, prev_strings
      );

      Ok(Json(TxnResponse {
        success: true,
        branch,
        prev_values: prev_strings,
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to execute transaction: {}", e),
    }),
  }
}

/// Handler for POST /getset endpoint
///
/// Request body example:
/// ```json
/// {
///   "key": "mykey",
///   "value": "new_value"
/// }
/// ```
///
/// Response example:
/// ```json
/// {
///   "success": true,
///   "key": "mykey",
///   "old_value": "previous_value",
///   "new_value": "new_value"
/// }
/// ```
///
/// If the key did not exist, `old_value` will be null.
async fn getset_handler(
  State(state): State<AppState>,
  Json(payload): Json<GetSetRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  // Use the getset API which atomically gets the old value and sets the new value
  match state
    .raft_node
    .getset(&payload.key, payload.value.as_bytes())
    .await
  {
    Ok(old_value_opt) => {
      let old_value_str =
        old_value_opt.map(|v| String::from_utf8(v).unwrap_or_else(|_| "Invalid UTF-8".to_string()));
      info!(
        "Getset successful: key='{}', old_value={:?}, new_value='{}'",
        payload.key, old_value_str, payload.value
      );
      Ok(Json(GetSetResponse {
        success: true,
        key: payload.key,
        old_value: old_value_str,
        new_value: payload.value,
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to getset: {}", e),
    }),
  }
}

/// Load configuration from TOML file
fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
  let config_str = fs::read_to_string(path)
    .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

  let config: Config = toml::from_str(&config_str)
    .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

  Ok(config)
}
