# 老钟楼多路径报时传输（clocktower/multipath）

用 Go 标准库实现的一连接多子流传输协议、链路模拟器、可视化页面和几十组链路矩阵测试。控制盒可以同时挂移动网卡和窄带电台；一条链路断线时，已发未确认数据会在其他子流上重传，恢复后自动带令牌重新加入。

## 快速开始

```bash
# 全部单元/协议/矩阵测试（约 1–2 分钟，矩阵含 43 组链路）
go test ./...

# 只跑非矩阵核心测试，更快
go test ./tests -run 'Test(Smoke|Middlebox|MaxSubflows|SlowPath|Disconnect|HighLoss)' -v

# 43 组链路：逐路径实测单路径基线、MinRTT、轮询、吞吐/时延比例
go run ./cmd/mptest -matrix-mb 2 -large-mb 0

# 生成并逐字节比较真实 300 MiB 随机文件
go run ./cmd/mptest -skip-matrix -large-mb 300

# 打开传输过程页面
go run ./cmd/mpdash -addr 127.0.0.1:8090 -size-mb 32
# 浏览器访问 http://127.0.0.1:8090
```

`make test`、`make matrix`、`make large`、`make dash` 是等价快捷命令。

## 页面内容

页面每 700ms 拉取 `/api/state`，包含：

1. **各条子流状态和拥塞窗口**：路径/子流 ID、active/down/closed、RTT、cwnd、ssthresh、在途字节、SSN NXT/ACK、重传/重注入/断线/重加入计数。
2. **DSN ↔ SSN 映射**：每条 DATA 的连接层统一序号区间、子流序号区间、长度、重传/冗余救援标记。
3. **接收端重排缓冲**：共享 buffer 占用、已收乱序段、缺口 DSN、next DSN。
4. **调度策略对比**：MinRTT 与 RoundRobin 的吞吐、平均/P95 时延、重传和救援事件。
5. 额外展示地址池排队/补齐、LIA α 和最近逐步拥塞公式、事件流。

## 八项需求对应实现

| 需求 | 实现位置 | 说明 |
|---|---|---|
| 1. 主路径建连、交换密钥，其他路径用 token + 截断 MAC 加入 | `pkg/transport/handshake.go`, `address.go`, `pkg/wire/handshake.go` | X25519 ECDH、随机 nonce、HKDF-SHA256；JOIN 携带由 master key 派生的 16 字节 token 和截断 HMAC-SHA256。SYN 重传不会轮换服务端密钥；HACK 丢失时服务端也可由带 keyed mapping MAC 的 DATA 确认。JOIN 有超时重试。 |
| 2. 统一 DSN、子流 SSN、映射带校验 | `pkg/wire/data.go`, `pkg/transport/sender.go`, `receiver.go` | 连接层字节序号 DSN 全局唯一；每个子流维护独立 SSN。DATA 中携带 `(path, DSN start/len, SSN start/len)` 映射，映射同时有 CRC32 和 12 字节截断 HMAC。 |
| 3. 连接层 ACK 与子流 ACK 独立；连接层确认才释放缓存；共享接收窗口 | `receiver.go`, `sender.go`, `types.go` | `SUB-ACK` 维护 SSN 累积/SACK，只驱动该子流 cwnd；`CONN-ACK` 维护 DSN 累积/SACK 和 rwnd，发送端只有收到 CONN-ACK 才释放段和 SendBuffer。接收端所有子流共用一个 reorder buffer/rwnd。 |
| 4. MinRTT / RoundRobin；慢路径占满窗口时快路径重发并减半慢窗口 | `sender.go` | 两种调度器均检查各子流 cwnd 余量和共享 DSN 窗口。共享窗口被高 RTT 路径阻塞且 RTT 差至少 1.5 倍时，最快路径用新 SSN 冗余重注入慢路径未确认 DSN；同一事件轮次内补齐，并调用 `cc.Group.Halve(slow)`。测试 `TestSlowPathWindowRescue` 断言 rescue/reinjection 发生且慢窗口减半。 |
| 5. 多子流窗口联动，总激进程度不超过普通单连接 | `pkg/cc/cc.go` | 拥塞避免使用 MPTCP LIA：`α = W_total · max(w_i/rtt_i²) / (Σ w_i/rtt_i)²`，每 ACK 增量 `min(α/W_total, 1/w_r)`。慢启动每 ACK +1 MSS；RTO 采用 Reno 式 `ssthresh=W/2, cwnd=1`；救援/显式减半使用 Halve。`TestLinkedFormulaStepByStep` 逐步重算 α、增量和窗口。 |
| 6. 断线重定向、恢复自动重新加入 | `pkg/simnet/link.go`, `address.go` | 每条模拟链路可配置周期性 `FlapEvery/FlapDown`。down 事件立即把该子流在途记账标记丢失，发送调度优先把 DSN 改到其他活动子流；up 后用同一 token/MAC 重新 JOIN。测试 `TestDisconnectRedirectAndAutoRejoin` 覆盖。 |
| 7. 中间设备改选项/拆包 → 映射校验失败 → 回退普通单路径 | `pkg/simnet/link.go`, `wire/data.go`, `receiver.go` | 模拟器支持 `mutate-option`（翻转签名映射中的 DSN 字节）和 `split-mapping`（在映射选项中间切开）。接收端 CRC/HMAC/截断错误归类为 `ErrMappingCheck`，认证后发送 FALLBACK；两端关闭非主路径，调度器强制 single。`TestMiddleboxFallback` 对两种模式均验证字节一致且状态变为 `single-path-fallback`。 |
| 8. 最大子流数可配；超额地址排队；关闭后按优先级补齐 | `address.go`, `types.go` | `Config.MaxSubflows` 控制上限；未建连地址保留在地址池并在页面显示 queued。`CloseSubflow` 发送 `SUB-CLOSE`，服务端释放槽位后，客户端按 Priority 建立下一条。`TestMaxSubflowsAndPriorityRefill` 连续重复运行通过。 |

## 模拟链路

`pkg/simnet` 中每个 `Profile` 可配置：

- `RateBytesPerSec`：带宽整形/串行化时延；
- `OneWayDelay`、`Jitter`：传播时延和抖动；
- `LossRate` + 固定 `Seed`：可复现随机丢包；
- `FlapEvery` / `FlapDown`：周期性断线和恢复；
- `Middlebox`：修改映射选项或拆包；
- `QueuePackets`：qdisc/inbox 容量和尾部丢弃。

## 测试矩阵

`internal/scenarios` 生成 **43 组**确定性组合，包括：

- 6ms 对 6/20/60ms（约十倍 RTT 差）；
- 100/300/900 Mbps 快路径加低速 companion；
- 0、0.5%、2%、6%、12%、22% 丢包；
- 频繁断线 + 不同丢包；
- 三/四路径异构组合；
- 24 组 delay/loss/bandwidth 交叉组合。

`reports/mptest-report.md` 和 `.json` 是一次完整 2 MiB × 三策略（每条候选路径还会单独实测基线）的报告：43/43 PASS，MinRTT/最佳单路径比例约为 **0.61–2.28**。极端频繁断线场景的内置门槛为 0.55，普通组合为 0.65–0.80。

`reports/large-file-300m.*` 是 300 MiB 真实随机文件验证：发送文件和接收文件分块逐字节比较通过，本次机器用时约 19.5s、约 16.1 MB/s。

## 代码结构

```text
cmd/mptest/             矩阵、基线、300MiB 文件校验 CLI
cmd/mpdash/             Web 页面服务
internal/dashboard/     /api/state 与 embed 静态页面
internal/scenarios/     43 组链路组合
internal/testkit/       内存模拟网络双端测试夹具
pkg/wire/               二进制帧、密钥、JOIN、DSN/SSN mapping、ACK 编解码
pkg/cc/                 LIA 联动拥塞控制和 Reno 参考窗口
pkg/simnet/             带宽/时延/丢包/断线/中间盒模拟
pkg/transport/          收发、调度、重排、重传、地址管理、快照
tests/                  端到端需求测试和 43 组 Go 矩阵
reports/                最近一次矩阵和大文件报告
```

## 说明

这是一个面向演示和测试的用户态协议实现，不直接占用真实 UDP/网卡；`transport.Link` 是窄接口，后续可以增加 UDP 链路实现而复用连接层、调度器和页面。加密使用标准库 X25519/HKDF/HMAC；没有引入第三方 Go 依赖。
