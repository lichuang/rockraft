# Rockraft / CoreDB 性能优化清单

> 基于 2026-09-20 CoreDB 性能压测（`../coredb/docs/bench.md`）与两侧代码逐行核证整理。
> 压测环境：CoreDB 3 节点（:16379-16381，Raft :17371-17373）vs 原生 Redis 8.2.1（:16999），`redis-benchmark --csv -q`，quick 模式 n=20000。

## 状态说明

- [ ] = 未完成
- [x] = 已完成

---

## 一、压测核心数据（摘录自 bench.md 2.1/2.2）

| 场景 | CoreDB single | CoreDB cluster | Redis standalone |
|---|---|---|---|
| GET d64 | 96,621 | 753 ⚠️(测量假象：key 未写入) | 97,814 |
| GET d1024 | 99,503 | 96,154 | 94,787 |
| SET d64 | 48,662 | **415** 🔴 | 105,263 |
| SET d1024 | 43,668 | 23,474 | 97,561 |
| SET d16384 | 15,397 | 4,811 | 76,336 |
| MSET(10) d16384 | 1,843 | **94.8** 🔴 | 15,699 |
| INCR d64 | 60,606 | 32,000 | 109,890 |
| HSET/LPUSH/SADD/ZADD d64 | 52k-56k | 28k-32k | ~100k |
| GETSET d64 | 48,900 | 26,144 | 104,712 |

**结论性判断**：
- 单节点读已与 Redis 持平（GET ~97k vs ~98k）
- 单节点写约为 Redis 一半（Raft + RocksDB 架构成本，合理）
- **集群转发路径是最大短板**：leader 直连 32k rps，follower 转发仅 300-600 rps（50-100 倍差距），p99=3000ms（= 3s 重试上限特征值）
- 大负载场景（MSET d16384）后集群雪崩：后续 SET d64 从 ~30k 掉到 ~300 rps 无法恢复

---

## 二、优化项清单

### 🔴 1. 转发热路径每次新建 gRPC channel（压测 P0 主因）

- [ ] **未完成**
- **侧**: rockraft
- **位置**: `src/node/forward.rs` 的 `send_forward_request()`
- **证据**: 每次转发调用 `JoinConnectionFactory::create_rpc_channel()`（TCP 握手 + HTTP/2 建连 + hickory DNS 解析）后重建 `RaftServiceClient`，无连接复用。转发是集群写唯一非 leader 入口，直连 leader 32k vs 转发 300-600 rps。
- **修法**: 转发路径接入已有的 `ClientPool::raft_service_client()`（`manager.rs` 的 `connect()` 已带 `max_message_size` 配置，#8 修复后可复用）。`JoinConnectionFactory` 仅保留 join 启动时一次性用途。
- **预期收益**: follower 入口 300-600 → 数千 rps 起步
- **关联**: todo.md #15

### 2. 每条命令 info! 打印完整 payload + `config.log.level` 未生效（压测 -19%）

- [ ] **未完成**
- **侧**: coredb
- **位置**: `coredb/src/server/server.rs:192`（`info!("Received command from {}: {:?}", peer_addr, value)`）；`coredb/src/main.rs`（仅读 `RUST_LOG`，`config.log.level`/`file` 定义了从未接入）
- **证据**: bench.md 2.3 实测 info vs warn 差 **-19%**；大 value 时 `{:?}` hexdump 整个 value，开销放大
- **修法**: 逐命令日志删除或降为 `debug!`；`log.level` 接入 `EnvFilter`；`log.file` 要么实现要么移除
- **预期收益**: **+19% 写吞吐**（一行改动，收益/成本比最高）

### 3. `get_leader` 每次写等 watch + 重试风暴无总预算无 jitter（雪崩放大器）

- [ ] **未完成**
- **侧**: rockraft
- **位置**: `src/node/forward.rs:34-56`（`get_leader` 2s timeout）；`execute_or_forward`（`MAX_RETRIES=20` + `RETRY_INITIAL_INTERVAL=200ms` → `RETRY_MAX_INTERVAL=3s` 指数退避）
- **证据**: 压测 p99=3000ms 正是 3s 重试上限特征值；MSET d16384 后雪崩（32k→300 rps 不恢复）。最坏路径 ≈ 20×(2s get_leader) + 48s 退避 ≈ 88s
- **修法**: (a) leader 未知时不等 2s，读当前快照立即返回进退避；(b) 给整个重试循环加总预算（如 5s）超时返回可重试错误；(c) **退避加 jitter** 避免选举后惊群
- **关联**: todo.md #17；与 #1 合并修效果最佳

### 4. `SET` 无条件预读（走完整 Raft 读链路）

- [ ] **未完成**
- **侧**: coredb
- **位置**: `coredb/src/protocol/string/set.rs:226`
- **证据**: 无条件 `server.get(&params.key)`——即使客户端未传 NX/XX/GET/KEEPTTL。**每个 SET 实际是读一次 + 写一次**（读也走 Raft 往返）
- **修法**: 仅当 `NX/XX/GET/KEEPTTL` 选项存在时才预读
- **预期收益**: SET 吞吐 ~2×

### 5. `StringValue` postcard 序列化 vs 手工字节布局

- [ ] **未完成**
- **侧**: coredb
- **位置**: `coredb/src/protocol/string/set.rs:211-215`（`StringValue::new(...).serialize()`）；`coredb/src/encoding/string.rs`（头部已定义手工布局文档：`flags(1B) | expires_at(8B) | data(NB)`，但实现用 postcard）
- **证据**: `serialize()` = `postcard::to_allocvec`（struct 编码开销），对比手工 3 次 `extend_from_slice` 快约一个数量级
- **修法**: `StringValue` 改手工字节布局，serialize/deserialize 退化为 memcpy
- **注意**: 需兼容已存量数据（flags 版本位已有铺垫）

### 6. 转发 payload / KV value 的重复克隆

- [ ] **未完成**
- **侧**: rockraft
- **位置**:
  - `src/node/forward.rs:100,107,131` — `execute_or_forward` 循环内 `payload.clone()` 每次尝试克隆整个 payload（d16384 = 16KB+，重试 20 次放大）
  - `src/raft/store/statemachine.rs` `stage_upsert_kv()` — `pending.insert(kv.key.clone(), Some(value.clone()))` 每 KV 两次堆分配（MSET(10) d16384 = 10×16KB×2）
- **修法**: payload 包 `Arc<RequestPayload>`；PendingWrites/key 换 `Bytes` 零拷贝切片
- **关联**: 压测 MSET d16384 雪崩场景的成本项

### 7. `ClientPool` 初始化竞态 + `check()` 空实现（伪超时）

- [ ] **未完成**
- **侧**: rockraft
- **位置**: `src/network/pool/client_pool.rs`（`contains_key` + `insert` 非原子）；`src/network/pool/manager.rs` 的 `check()`（直接 `Ok(conn)`，不校验 channel 存活）
- **证据**: 断连的 channel 会被分发 → 请求走完整 10s 超时才失败 → 命中重试（3s 退避），是压测 p99=3000ms 的放大器之一
- **修法**: `DashMap::entry(addr).or_insert_with(...)`；`check()` 做 `channel.health()` 或轻量探测，失败剔除重试一次
- **关联**: todo.md #16（竞态）、#25（check 空实现）

### 8. pipeline 无并发（每连接串行处理）

- [ ] **未完成**
- **侧**: coredb
- **位置**: `coredb/src/server/server.rs`（每连接 spawn 单循环，`process_command` 逐条 await）
- **证据**: bench pipeline 扫描：P=64 GET single 仅 77k（vs P=1 48k），提升远低于预期——每命令有 Raft 往返，串行 pipeline 是瓶颈
- **修法**: pipeline 场景命令流 `buffer_unordered` 化（RESP 响应需保序——`stream::iter(...).then(...)` 保序 unordered）

### 9. INCR/HINCRBY 非原子 read-then-write

- [ ] **未完成**
- **侧**: coredb
- **位置**: `coredb/src/protocol/string/incr.rs:48-91`
- **证据**: read-then-write 两个问题——正确性（并发 INCR 丢更新）与性能（一次 RMW = 2 次 Raft 往返）
- **修法**: 收敛到 rockraft `TxnReq`（条件 + `return_previous` 循环 CAS），或扩展 `Cmd::Txn` op 集支持算术
- **关联**: rockraft 已有 `txn` API 原生支持

### 10. `truncate_after`/`purge` 用 `delete_range_cf`（惰性删除）

- [ ] **未完成**
- **侧**: rockraft
- **位置**: `src/raft/store/log_store.rs`
- **问题**: range delete 是 lazy 的，需 compaction 才释放空间
- **修法**: purge 后配合 `compact_range` 触发回收（权衡 compaction 开销）
- **关联**: todo.md #19

### 11. 逐命令日志级别对照（已完成验证的对照数据）

- [x] **已验证**（压测 2.3）
- **数据**: `RUST_LOG=warn` 49,628 rps vs `RUST_LOG=info` 40,241 rps（short run）——长跑 + 大 value 差距更大（每条命令 Debug 打印完整 value）
- **说明**: 此项与优化项 #2 合并处理

---

## 三、建议执行顺序

| 序 | 项 | 侧 | 预期收益 | 成本 |
|---|---|---|---|---|
| 1 | #2 删逐命令日志 + 接 `log.level` | coredb | +19% | 极低 |
| 2 | #1 转发接入 ClientPool | rockraft | 集群转发 50-100× | 中 |
| 3 | #3 leader 探测重设计 + 总预算 + jitter | rockraft | 消除雪崩、p99 3s→<100ms | 中 |
| 4 | #4 SET 按需预读 | coredb | SET 吞吐 ~2× | 低 |
| 5 | #5 StringValue 手工布局 | coredb | 序列化快 10× | 低 |
| 6 | #6 Bytes 零拷贝 | 两侧 | 大 value 场景显著 | 中 |
| 7 | #7 pool check + entry | rockraft | 消除伪超时 | 低 |
| 8 | #8 pipeline 并发 | coredb | pipeline 吞吐 ×N | 中 |
| 9 | #9 INCR 原子化 | coredb | 正确性 + 减往返 | 中 |
| 10 | #10 purge + compact_range | rockraft | 空间回收 | 低 |

**最短见效路径**：#2（一行改动 +19%）→ #1（转发接入池，集群写跳级）→ #3（探测/预算/jitter，雪崩消失）→ #4。前三项完成后集群写预计从 300-600 rps 跳到万级；MSET d16384 雪崩（bench.md P3）大概率随之消失，需复测确认。

---

## 四、复现方式

```bash
# CoreDB 构建 + quick 压测（结果写入 coredb/bench/results/<timestamp>/）
cd ../coredb && cargo build --release && cd bench && ./run_bench.sh --quick

# rockraft 单元测试
cd ../rockraft && cargo test --all-features
```

原始数据文件：`coredb/bench/results/<timestamp>/raw/`（redis-benchmark CSV + 三节点日志 + 日志级别对照）
详细分析：`coredb/docs/bench.md`（压测记录、问题根因、P0-P5 清单）

---

## 关联文档

- `docs/todo.md` — 正确性/健壮性/死代码清单（性能项 #15/#17/#19/#22 与本清单重叠，以本清单为准）
- `coredb/docs/bench.md` — 压测原始记录与 P0-P5 根因分析