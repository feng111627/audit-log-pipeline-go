# audit-log-pipeline-go

本地审计日志采集与转发管道：用本地文件（JSON Lines 尾随）和本地 HTTP 接入模拟事件来源，
用本地目录模拟转发目标，事件的过滤、投递与留存结果全部可复现。

## 构建与运行

```bash
go build ./cmd/auditpipe
./auditpipe \
  -file ./events.jsonl \          # 可多次指定，多个文件来源并发采集
  -http 127.0.0.1:8080 \          # 本地 HTTP 接入（可选）
  -sink-dir ./audit-out \         # 转发目标目录
  -checkpoint ./audit-out/checkpoint \
  -drop-prefix debug. \           # 过滤规则：丢弃 action 前缀
  -default-actor unknown \        # 字段补全：缺失 actor 的默认值
  -queue 256 \                    # 有界队列容量（背压）
  -max-age 720h -max-bytes 104857600   # 保留规则（可选）
```

## 接入协议

### 事件格式（JSON）

| 字段 | 必填 | 说明 |
|------|------|------|
| `id` | 是 | 事件唯一 ID，用于端到端去重 |
| `action` | 是 | 动作，如 `user.login` |
| `timestamp` | 是 | RFC3339 时间；超过当前时间 24h 判为非法 |
| `actor` | 否 | 缺失时按 `-default-actor` 补全 |
| `labels` / `payload` | 否 | 附加维度与负载 |

### 文件来源

`-file` 指定 JSON Lines 文件，逐行读取并持续尾随追加内容。

### HTTP 来源

`POST http://<addr>/events`，请求体为单条事件 JSON。

- 成功：`202 Accepted`
- 失败：`422 UnprocessableEntity`，响应头 `X-Reject-Reason` 给出拒绝分类

### 拒绝原因（一眼可区分）

| 原因 | 含义 |
|------|------|
| `MALFORMED_JSON` | 输入不是合法 JSON |
| `MISSING_FIELD` | 必填字段缺失（附带字段名） |
| `INVALID_FIELD` | 字段值非法（如时间戳超未来 24h） |

被拒绝的输入不会被悄悄丢掉：HTTP 同步返回 422，文件来源的拒绝记录会
计入统计并在进程退出时逐条打印（内存中保留上限 1024 条）。

## 投递语义

- **至少一次**：目标写入失败按固定间隔重试，直到成功或进程停止。
- **不重复落库**：`DirSink` 按单调 `Seq` 幂等确认；管道在分配序号前按事件
  `id` 去重，重启时从目标扫描已知 ID 预热去重集合，重放不产生重复。
- **不跟丢**：序号由单一调度 goroutine 单调分配；`Seq` 跳号时目标直接报错
  （`seq gap`），不会从中间接着写。
- **位点**：每条事件落库后原子推进 checkpoint 文件。启动时比对本地位点与
  目标位点，**位点倒退直接报错**（`checkpoint regression`）而不是续写。
- **背压**：来源→调度→投递之间全是有界队列（`-queue`），目标变慢时采集端
  自然阻塞，管道内积压不超过队列容量 + 2 条，内存不被无限撑大。

## 防篡改串联

落库前为每条事件计算 SHA-256 哈希并串联：`prev_hash` 指向上一条事件的
`hash`（链首为 `GENESIS`），`hash` 覆盖事件全部内容与 `prev_hash`。
篡改任何已落盘字段或抽掉中间事件都会导致校验失败。

## 保留规则

`-max-age`（时限）与 `-max-bytes`（容量）可单独或组合使用，每分钟执行一次：

- 清理以**整分片**为单位，永不清理当前活跃分片，保证追加写不中断；
- 被清理分片在 `manifest.json` 中标记 `deleted` 并保留链锚点
  （`first_prev`/`last_hash`），链校验从锚点继续，**不会因清理产生误报**。

## 验证方法

### 单元测试

```bash
go test ./... -v        # 日志按 “业务=... 用例=... 结果=...” 输出覆盖明细
go test -race ./...     # 并发竞争检测
```

覆盖的业务与用例：

| 业务 | 用例 |
|------|------|
| 事件解析与拒绝分类 | 非 JSON / 缺 id / 缺 action / 缺 timestamp / 非法 timestamp / 合法解析 |
| 并发采集 | 多文件+HTTP 并发不重复不跟丢、序号连续无洞、坏事件带可区分原因且不影响好事件 |
| 过滤与补全 | 按规则丢弃、缺 actor 补全后投递 |
| 重启恢复 | 重启后不丢不重、链完整、位点倒退直接报错 |
| 目标故障 | 临时不可用重试后至少一次投递、重试不重复落库、目标变慢时背压有界、恢复后全量投递 |
| 投递目标 | 重复投递幂等、Seq 跳号报错、重启位点恢复、落盘篡改可检出 |
| 防篡改串联 | 完整链通过、篡改内容检出、断链检出、清理后锚点校验不误报 |
| 保留清理 | 超容量/超时限清理最旧分片、清理后链校验不误报、活跃分片保留位点连续 |

### 链完整性校验

代码内调用 `sink.VerifyDir(dir)`：返回 `-1` 表示通过，否则返回第一个
断链/被篡改事件的 `Seq`。已清理分片通过清单锚点自动跳过。

### 手动复现

```bash
# 1. 准备事件文件并启动管道（见上文命令）
# 2. HTTP 投递一条：curl -X POST 127.0.0.1:8080/events -d '{"id":"e1","action":"user.login","timestamp":"2026-10-07T01:00:00Z"}'
# 3. 查看 ./audit-out/segment-000001.jsonl 中的哈希链与 ./audit-out/checkpoint 位点
# 4. 重启进程：重放文件不会重复落库，新事件续接序号与哈希链
```

## 目录结构

```
cmd/auditpipe        入口（flag 装配）
internal/event       事件模型、解析、拒绝分类
internal/source      文件尾随 / HTTP 接入
internal/filter      可配置过滤与字段补全
internal/pipeline    并发编排、去重、背压、重试、位点提交
internal/sink        可插拔目标接口 + 本地目录实现（分片、幂等、链密封）
internal/checkpoint  位点持久化与倒退检测
internal/chain       哈希串联与校验
internal/retention   按时间/容量的整分片清理
```
