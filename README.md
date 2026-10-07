# audit-log-pipeline-go

本地审计日志采集与转发管道：用本地文件 / 本地 HTTP 模拟事件来源，用本地目录模拟转发目标，
事件的过滤、投递与留存结果均可复现。

## 架构

```
FileSource ─┐
            ├─> 有界队列(背压) ─> 解析 ─> 补全 ─> 校验 ─> 过滤 ─> 哈希串联 ─> 批量投递 ─> DirSink(本地目录)
HTTPSource ─┘                     └────────── 拒绝(带原因) ──────────┘                    ├─ seg-NNNNNN.log
                                                                                          ├─ OFFSET (位点)
                                                                                          └─ ANCHOR (清理锚点)
```

- 所有来源并发采集，汇入**单个有界队列**；队列满时来源阻塞，形成背压，内存不被无限撑大。
- **单个串行处理循环**完成解析/补全/校验/过滤/串联/批量投递，保证并发采集下事件不重复、不跟丢，哈希链一致。
- 相同事件 ID 会被去重（来源重试不会产生重复记录）。

## 接入协议

### 事件格式（单行 JSON）

```json
{"id":"e1","timestamp":"2026-10-07T01:00:00Z","action":"login","actor":"alice","source":"app","fields":{"k":"v"}}
```

必填字段：`id`、`timestamp`、`action`、`actor`。`source` 等可由 `-enrich` 默认值补全。

### 来源

| 来源 | 接入方式 |
|------|----------|
| 文件 | `-file PATH`（可重复），按行读取，读完跟随追加（tail） |
| HTTP | `-http ADDR`，`POST /ingest`，body 为单行事件 JSON，成功返回 202 |

### 过滤规则

- `-drop-action-prefix P`：丢弃 action 以 P 开头的事件
- `-drop-actor A`：丢弃 actor 为 A 的事件

### 拒绝原因（一眼可区分，不会被悄悄丢掉）

| 原因 | 含义 |
|------|------|
| `BAD_FORMAT` | 不是合法 JSON 或结构不符 |
| `MISSING_FIELD` | 必填字段缺失（详情列出缺哪些字段） |
| `FILTERED` | 命中过滤规则（详情列出命中的规则） |

拒绝记录会写入运行日志并可通过 `Pipeline.Rejections()` 获取。

## 投递语义

- **至少一次**：批次写入段文件并 fsync、原子更新 `OFFSET`（tmp+rename+目录 fsync）后才算投递成功；
  目标临时不可用时按 `-retry` 间隔重试，进程退出前尽力冲刷在途批次。
- **不重复落库**：`DirSink` 以 `seq` 为位点，`seq <= committed` 的记录视为重复投递并幂等跳过——
  进程重启或目标故障恢复后的重投不会产生重复记录。
- **位点保护**：新批次的第一条未提交记录必须恰好是 `committed+1`；
  出现空洞/倒退时返回 `ErrOffsetRegression` 直接报错，绝不从中间接着写。
- **重启恢复**：`OFFSET` 持久化已提交位点与链尾哈希，重启后从位点续写、续链。

## 防篡改串联

每条记录携带 `seq`、`prev_hash`、`hash`（`hash = SHA256(seq | prev_hash | event)`），
逐行写入段文件 `seg-NNNNNN.log`。篡改记录内容、抽掉记录（断链）、改序号都能被校验检出。

## 保留规则

`run` 时通过 `-max-age DUR` 与 `-max-bytes N` 配置，每分钟执行一次：

- 以**整段**为单位清理（绝不在段中间截断），活跃段（最后一个段）永不删除；
- 删除前校验该段链完整，**校验失败的段拒绝删除**，留给审计；
- 删除后把被删前缀的链尾哈希写入 `ANCHOR`，后续校验以锚点为起点——**清理本身不会造成链校验误报**。

## 验证方法

```bash
# 全量单测（日志含 业务/用例/结果）
go test ./... -v

# 端到端
auditpipe run -sink out -file events.log -http 127.0.0.1:8080 \
    -drop-action-prefix debug. -max-age 24h -max-bytes 1073741824
auditpipe verify -sink out   # 校验目录内所有段的链完整性，篡改即 FAIL
```

## 测试覆盖

| 业务 | 用例 | 位置 |
|------|------|------|
| 并发采集 | 多文件+HTTP 并发，不重复不跟丢 | `internal/pipeline` |
| 拒绝可区分 | BAD_FORMAT / MISSING_FIELD / FILTERED | `internal/pipeline` |
| 目标故障 | 临时不可用恢复后不丢不重、重复投递幂等 | `internal/pipeline`、`internal/sink` |
| 背压 | 慢目标下有界队列不丢事件 | `internal/pipeline` |
| 重启恢复 | 位点与链尾恢复、管道重启续链 | `internal/sink`、`internal/pipeline` |
| 位点保护 | 位点倒退/空洞直接报错 | `internal/sink` |
| 篡改检测 | 篡改内容 / 断链 / 序号异常 | `internal/chain` |
| 保留清理 | 按容量/按时间清理且校验无误报、篡改段拒删 | `internal/retention` |
