# Rockraft

A strongly consistent distributed key-value store library built on Raft consensus protocol and RocksDB.

[![Rust](https://img.shields.io/badge/rust-2024+-dea584.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

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
- ✅ **Configurable Read Consistency** - Choose per deployment: leader-applied (default), local-applied, lease-read, or quorum-confirmed linearizable reads

## Read Consistency Modes

Reads are NOT linearizable by default — that is the classic trade-off of reading a leader's
applied state without quorum confirmation (a deposed-but-unaware leader can serve stale values,
and a just-committed write can be missed until it is applied). Configure the level per node:

```toml
[raft]
read_consistency = "read_index"   # leader_applied (default) | local_applied | lease_read | read_index
```

| Mode | Staleness guarantee | Cost |
|---|---|---|
| `leader_applied` (default) | Sees only committed data, but may miss just-committed writes and can go stale if the leader was deposed without knowing | One forward hop; cheapest |
| `local_applied` | Same, plus the local view can lag the leader further | Zero network; for staleness-tolerant read-heavy workloads |
| `lease_read` | Linearizable assuming clocks drift less than the election-timeout lease | Near-free confirmation |
| `read_index` | **Linearizable, unconditional** (majority confirmation + state-machine catch-up) | One quorum round-trip per read; concurrent reads can share one confirmation |

Pick `read_index` when reads must reflect every committed write; `lease_read` inside clock-controlled
Lans; keep the default for general KV workloads.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [OpenRaft](https://github.com/databendlabs/openraft) - Raft consensus implementation
- [RocksDB](https://github.com/facebook/rocksdb) - High-performance database

## Contact & Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/yourusername/rockraft/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/yourusername/rockraft/discussions)
- **Documentation**: [API Reference](https://docs.rs/rockraft)

## See Also

- [example/](example/) - Complete cluster working example
- [CoreDB](https://github.com/lichuang/coredb) - A Redis-compatible distributed KV database built on top of Rockraft, demonstrating the library in production: strong consistency, automatic failover, and Redis protocol support (String/Hash/List/Set/ZSet, TTL, and more)
