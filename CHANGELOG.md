## [0.1.8] - 2026-09-14

### 🚀 Features

- Add snapshot_log_id() and trigger_snapshot() APIs to RaftNode for snapshot introspection and manual triggering
- Add snapshot trigger/status HTTP endpoints and 7 snapshot integration tests (basic, large dataset, binary data, consistency, batch write, txn, multi-cycle)
- Add 3 snapshot recovery integration tests (cluster restart, follower restart, new node join) with node_id verification via /health endpoint
- Add delete/batch_delete APIs to RaftNode and 6 snapshot edge-case integration tests (concurrent writes, empty DB, post-delete, overwrite, log-vs-snapshot recovery, getset)
- Add 22 snapshot integration tests (normal, recovery, abnormal, log-purge, boundary) with supporting RaftNode APIs and HTTP endpoints for snapshot/delete/purge-log
- Add Kubernetes chaos testing framework with Chaos Mesh integration for rockraft cluster (Dockerfile, StatefulSet manifests, Chaos Mesh CRDs, pytest chaos test suite, and GitHub Actions daily CI workflow)

### 🐛 Bug Fixes

- Add missing libclang-dev dependency in Dockerfile for RocksDB bindgen compilation during chaos test Docker build
- Add missing libprotobuf-dev dependency in Dockerfile for protoc well-known types resolution
- Wait for each StatefulSet pod individually and add cluster formation delay in chaos test workflow
- Remove duplicate port-forward setup from workflow to avoid conflict with test fixture
- Replace flaky kubectl port-forward with kubectl exec for chaos test pod communication
- Treat ordinal 0 as truthy in wait_for by checking `is not None` instead of truthiness
- Use advertise_endpoint for node registration and align example configs to separate listen/advertise addresses
- Use advertise_endpoint for node registration and align example configs to separate listen/advertise addresses
- Add inter-test cluster health wait to prevent quorum loss in chaos tests
- Update chaos test health check to support OpenRaft RunningState dict format
- Extract health state helper and add cluster stabilization after network partition in chaos tests
- Increase K8s probe tolerance and add wait_for_min_healthy in chaos tests to prevent false pod restarts during leader election storms
- Increase K8s probe tolerance, add wait_for_min_healthy helper, and reduce network delay in chaos tests to prevent leader-election storms
- Add raft heartbeat/election-timeout config fields, set relaxed values for chaos tests, and print all config on startup
- Fix compile bug
- Skip init_cluster() on restart when node already in cluster and wait for pod recovery between chaos tests
- Skip init_cluster() on restart when node already in cluster, wait for pod recovery between chaos tests, and retry client requests during leader election
- Skip init_cluster() on restart, retry client requests during leader election, and only look for new leader among surviving nodes in failover test
- Disable chaos test workflow
- Resolve Txn conditions and prev_values against a pending-writes overlay so apply results no longer depend on how the apply stream is chunked into batches
- Make install_snapshot wait for snapshot recovery to complete (spawn_blocking + await) so subsequent log application never races ahead of restored data, and remove the sleep-based workarounds from snapshot tests
- Make the gRPC message size limit configurable with a 256MB default to stop 4MB decode failures from breaking raft replication, and preserve ForwardToLeader redirects across all write paths and forwarded hops via a unified ApiError wire codec

### 🚜 Refactor

- Extract per-command handlers from RaftStateMachine::apply and fold the pending-writes overlay into stage_upsert_kv

### 📚 Documentation

- Add todo.md
- Add todo.md
- Add todo.md
- Update readme, add coredb
## [0.1.7] - 2026-04-12

### 🚜 Refactor

- *(config)* [**breaking**] Remove redundant `single` field from RaftConfig

### ⚙️ Miscellaneous Tasks

- Bump version to v0.1.7
## [0.1.6] - 2026-04-11

### 🐛 Bug Fixes

- *(service)* Remove panic on ForwardResponse serialization failure
- Store snapshot temp files in snapshot_dir instead of system temp dir
- Evict abandoned streaming snapshots to prevent memory leak
- *(node)* Replace std::sync::Mutex with tokio::sync::Mutex in async context

### 🚜 Refactor

- *(error)* Simplify error type hierarchy
- *(error)* Eliminate duplicated error handling code
- *(node)* Eliminate pass-through methods between RaftNode and LeaderHandler
- *(config)* Eliminate ParsedConfig and unify configuration parsing
- *(node)* Deepen RaftNodeBuilder with progressive construction
- *(statemachine)* Inline `mutex_lock_err` function
- Unify naming inconsistencies
- Eliminate assume_leader/forward duplication in RaftNode
- *(node)* Split RaftNode God Object into focused modules
- *(engine)* Hide RocksDBEngine storage detail behind pub(crate) accessor
- *(engine)* Encapsulate RocksDBEngine and eliminate panic on open failure
- Deduplicate RPC serialization, membership checks, and fix integration tests
- *(store)* Extract lock_sys_data() to deduplicate mutex error handling
- *(utils)* Unify timestamp logic into now_millis()
- *(tests)* Extract shared test utilities to eliminate duplication
- Deduplicate RocksDB config and hide LogStore internals
- Deduplicate RocksDB config and hide store internals
- Hide store internals and unify node creation path
- Unify error handling in leader_handler and statemachine
- Extract apply_upsert_kv helper to deduplicate apply() logic
- Split snapshot() into focused helper methods
- Unify Endpoint to single definition in config module
- *(network)* Merge grpc modules into unified network layer
- *(visibility)* Tighten public API by converting pub to pub(crate)

### 📚 Documentation

- *(node)* Improve documentation to explain "why" not "what"
- Add cross-module design decision comments
- Add module-level documentation

### ⚡ Performance

- *(node)* Skip redundant O(n) node sync on cluster join
- Use exponential backoff for forward request retries

### ⚙️ Miscellaneous Tasks

- *(grpc)* Remove dead RpcClientConf with leftover metasrv fields
- Remove dead code across crate
- *(types)* Remove unused forward_to_leader field from ForwardRequest
## [0.1.5] - 2026-04-06

### 🚀 Features

- Add daily test
- Add transaction support with conditional operations
- Add transaction support with conditional operations and previous value return
- Add more snapshot test cases
- Implement RaftNode.join() API for adding nodes to the cluster

### 🐛 Bug Fixes

- Add protoc installation to workflows

### 🧪 Testing

- Add concurrent operation tests for thread safety and consistency

### ⚙️ Miscellaneous Tasks

- Bump version to v0.1.5
- Bump version to v0.1.5
- Update lib.rs
- Update v0.1.5 changelog.md
## [0.1.4] - 2026-03-21

### 🚀 Features

- Add scan_prefix method to RocksStateMachine
- Add scan_prefix API for prefix-based key scanning
- Add scan_prefix API for prefix-based key scanning
- Add batch atomic write support to RaftNode

### 🐛 Bug Fixes

- Update agents.md and fix format

### 🚜 Refactor

- Shrink GrpcConnectionError by replacing AnyError with String
- Change ScanPrefixReq.prefix from String to Vec<u8>
- Move examples/cluster to examples directory
- Comply with Type Import Rules in AGENTS.md

### 📚 Documentation

- Update AGENTS.md

### 🎨 Styling

- Fix clippy warnings and apply code optimizations

### ⚙️ Miscellaneous Tasks

- Add CI/CD workflows and CHANGELOG
