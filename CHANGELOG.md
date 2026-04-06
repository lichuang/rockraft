# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- Update lib.rs

## [0.1.4] - 2025-03-21

### Added
- Batch atomic write support to RaftNode (`batch_write` API)

### Changed
- Move examples/cluster to examples directory
- Comply with Type Import Rules in AGENTS.md

### Fixed
- Update AGENTS.md and fix format

## [0.1.3] - 2025-03-20

### Added
- Scan prefix API for prefix-based key scanning

### Changed
- Change `ScanPrefixReq.prefix` from `String` to `Vec<u8>`
- Shrink `GrpcConnectionError` by replacing `AnyError` with `String`
- Update tonic to v0.14.5

### Fixed
- Fix clippy warnings and apply code optimizations

## [0.1.2] - 2025-03-18

### Added
- `scan_prefix` method to `RocksStateMachine`

### Changed
- Refactor encode/decode, use postcard instead of bincode
- Use openraft 0.10.0-alpha.14

### Fixed
- Fix compile warning

## [0.1.1] - 2025-03-15

### Added
- Restart test cases for cluster example
- Test cases of cluster example

### Fixed
- Fix membership bug
- Fix cluster example start bug
- Fix connection retry logic bug
- Fix add member bug

## [0.1.0] - 2025-03-01

### Added
- Initial release
- Raft consensus implementation based on OpenRaft
- RocksDB storage backend
- gRPC-based inter-node communication
- Cluster management (join/leave nodes)
- Leader election and failover
- Snapshot support for storage recovery
- Connection pooling for gRPC
- HTTP API example with axum
- DNS resolver for node discovery
