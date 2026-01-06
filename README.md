# Rockraft

A strongly consistent distributed key-value store library built on Raft consensus protocol and RocksDB.

[![Rust](https://img.shields.io/badge/rust-2024+-dea584.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Overview

Rockraft is a Rust library that provides distributed consensus for data replication, ensuring high availability and fault tolerance for distributed systems. It combines the power of:

- **OpenRaft** - A production-ready Raft consensus implementation
- **RocksDB** - High-performance embedded database

## Features

- ✅ **Strong Consistency** - All nodes maintain consistent state through Raft consensus
- ✅ **Fault Tolerance** - Automatic leader election and failover
- ✅ **High Performance** - RocksDB storage with efficient serialization
- ✅ **Easy Setup** - Simple configuration and cluster initialization
- ✅ **Snapshot Support** - Efficient storage recovery and compaction
- ✅ **Connection Pooling** - Optimized gRPC connection management
- ✅ **Multi-Node Support** - Scale from single node to large clusters

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [OpenRaft](https://github.com/databendlabs/openraft) - Raft consensus implementation
- [RocksDB](https://github.com/facebook/rocksdb) - High-performance database

## Contact & Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/yourusername/rockraft/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/yourusername/rockraft/discussions)
- **Documentation**: [API Reference](https://docs.rs/rockraft)

## See Also

- [CLAUDE.md](CLAUDE.md) - Detailed technical documentation
- [examples/basic/](examples/basic/) - Complete working examples
