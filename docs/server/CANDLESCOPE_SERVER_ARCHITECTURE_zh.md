# CandleScope Server 架构合同 v1

状态：FROZEN_FOR_PHASE_1
架构版本：candlescope.server-architecture.v1
前置合同：CANDLESCOPE_SERVER_PRODUCT_CONTRACT_zh.md

## 1. 当前基线

当前 CandleScope 是成熟的单进程组合式后端，而不是可直接横向扩展的集群：

- backend/app/main.py 通过 app.state 挂载 DataEngine、回放、指标和插件运行时；
- backend/app/data_engine/data_manager/event_bus.py 是单事件循环内的进程内总线；
- backend/app/data_engine/storage/klines_repo.py 面向 SQLite K 线批量持久化；
- backend/app/replay/actor.py 已建立正确的单会话单写者 Actor 语义；
- backend/app/data_engine/data_manager/manager.py 已提供存储依赖注入入口。

服务器演进必须保留领域语义和外部 API，替换组合、传输、所有权和持久化边界。

## 2. 目标形态

用户访问一个逻辑 CandleScope 后端。生产部署内部由以下平面组成：

1. 接入平面：统一入口、身份、HTTP API、WebSocket 网关。
2. 数据平面：采集器、持久事件日志、标准化、落库、归档和实时 fan-out。
3. 控制平面：组织权限、配置、目录、任务、租约、审计和元数据。
4. 回放平面：调度器、单写者会话 Actor、checkpoint 和查询适配器。
5. 研究平面：任务调度、CPU/GPU Worker、不可变数据快照和结果目录。
6. 可观测平面：指标、日志、trace、数据质量、积压与恢复状态。

逻辑数据流：

    Exchange
      -> Collector shards
      -> Kafka-compatible durable event log
         -> ClickHouse writer -> ClickHouse
         -> Parquet archiver -> object storage
         -> live fan-out -> WebSocket gateways -> frontends
         -> feature and alert consumers

    Frontends
      -> API gateway
         -> PostgreSQL control plane
         -> query service -> ClickHouse or object archive
         -> replay scheduler -> Replay Worker pool
         -> research scheduler -> CPU/GPU Worker pool

## 3. 服务边界

Phase 0 冻结边界，不强制一开始就部署大量微服务。Phase 1 可以由少数进程承载多个角色，但角色之间只能通过稳定端口通信：

| 角色 | 权威状态 | 可水平扩展 | 不允许依赖 |
| --- | --- | --- | --- |
| Collector | 连接状态、producer epoch | 按交易所和市场流 | 前端会话、回放账户 |
| Event log | 已确认事件与分区 offset | 按分区 | SQLite、API 进程内队列 |
| ClickHouse writer | 消费 offset、批次状态 | 消费组 | 用户请求状态 |
| Archive writer | 对象 commit 与 hash | 消费组 | 可变本地文件作为权威源 |
| API gateway | 请求上下文 | 无状态扩展 | 采集器内存状态 |
| WS gateway | 短期连接和订阅映射 | 多实例 | 作为行情权威源 |
| Replay Worker | 被租约保护的会话 Actor | 按 session_id | 实时总线、其他 Worker 内存 |
| Research Worker | 单个任务沙箱 | 按 job_id | API 和采集进程资源池 |

进程合并只是部署优化，不能改变这些所有权规则。

## 4. 事件合同与分区

跨进程行情统一使用 docs/server/contracts/market-event-envelope-v1.schema.json。Python 参考实现位于 backend/app/server_contracts/market_event.py。

核心规则：

- partition_key 等于规范化 MarketStreamKey.topic；
- 同一逻辑流的事件只进入一个分区，从而保留分区内顺序；
- append 和 ordered_delta 事件必须带 source_event_id 或 sequence；
- ordered_delta 必须带完整 sequence_start 和 sequence_end；
- producer_epoch 用于识别旧生产者复活和写入冲突；
- payload_canonicalization 固定为 rfc8785；payload_sha256 对 RFC 8785/JCS 生成的 UTF-8 bytes 计算；
- payload 必须属于 I-JSON；超出 IEEE-754 互操作范围的整数、精确小数和大数使用字符串承载；
- event_id 是一次规范化事件的全局身份，重试不得重新生成；
- 交付为至少一次，ClickHouse、归档和下游消费者必须幂等；
- 同 identity 不同 hash 为完整性冲突，进入隔离流并告警。

不承诺跨市场流全局有序。需要跨品种研究时，以 event_time、received_at、published_at 和数据 epoch 明确对齐，而不是依赖消息到达顺序。

## 5. 存储职责

### PostgreSQL

保存组织、用户、角色、工作区、数据目录、任务、回放会话目录、租约、插件配置、审计和事务状态。不保存高频原始事件主表。

### ClickHouse

保存高频成交、盘口、清算、市场指标、K 线、派生特征和交互分析结果。表排序键必须从查询模式和流身份推导；写入必须批量化。数据修正采用版本和重算，不对原始事实做不可审计覆盖。

### 对象存储与 Parquet

保存原始或规范化不可变分区、manifest、hash、schema、源覆盖和数据 epoch。每次可查询快照由 MarketDataSnapshotRef 唯一标识：data_epoch + 单调 snapshot_version + manifest_uri + manifest_sha256。旧 manifest 永久保持不可变和可寻址；新归档只能发布新版本 manifest，不能覆盖旧版本。它是 ClickHouse 重建、离线研究和长期保留的权威来源。默认不得由本地 GC 策略隐式删除。

ArchiveCommit 必须返回该快照引用、对象 URI/hash，以及按 MarketStreamKey.partition_key 汇总的事件时间、sequence 和数量覆盖范围。MarketEventQuery 必须接收完整快照引用，返回页也回显同一引用及本页覆盖范围；分页 cursor 必须绑定 manifest_sha256，跨快照复用时 fail closed。禁止以“当前最新”隐式替代调用方已经固定的快照。

交互查询以不可变 Parquet 快照为正确性权威。只有 ClickHouse writer 消费组的 committed next offset 已覆盖请求的 snapshot_version，且同一请求的 ClickHouse 页与 Parquet 页逐项相等时，查询服务才可返回热结果；游标落后时 `auto` 必须走冷端，强制 `hot` 必须明确拒绝，任何热冷差异必须 fail closed。Phase 1F 的逐请求双读是正确性证明机制，不是最终容量方案。

查询服务只能由已认证的内部入口调用，并为认证拒绝、请求校验和每次查询生成不含凭据与行情 payload 的结构化审计事件。成功热查询登记为不可变后台 parity probe；任一前台或后台热冷页不一致会锁存热端 quarantine。Phase 1H 把 quarantine 代际状态和 RFC 8785 审计哈希链放入 PostgreSQL；所有查询实例读取同一状态，进程重启不能解除隔离。解除必须使用独立控制凭据、匹配当前 generation，并把状态转换和成功审计放进同一数据库事务；不允许按一次成功采样自动解除。

Phase 1I 把 query-control DDL 从运行进程移到固定 SHA-256 的外部版本化迁移，并把登录身份分成只写必要状态的 runtime role 与只读 auditor role。运行时会逐项验证迁移版本和有效权限，缺表、版本漂移、越权或缺权均拒绝启动。审计 verifier 使用只读 repeatable-read 快照；经校验的数据库 head 可用独立保管的 HMAC-SHA256 key 签名并条件写入对象存储，再以明确的 anchor URI 校验备份恢复后的事件、head、迁移版本和状态。该锚点能让只有数据库管理权的一方无法静默重算历史，但不是公钥签名、不可抵赖账本、对象锁或生产灾备方案；HMAC/S3 管理权未隔离、锚点 URI 未可靠保存、对象被删除或旧锚被故意选取时，仍需要外部控制补足。

Phase 1J 增加 PostgreSQL 物理 base backup、连续 WAL archive 适配器和真实 target-time PITR 门禁。每套备份以不可变对象保存 PostgreSQL `backup_manifest`、`base.tar.gz`、`pg_wal.tar.gz`，并在 RFC 8785/HMAC 清单中绑定 system identifier、timeline、LSN 范围、精确 `recovery_target_time`、WAL 前缀和 Phase 1I 审计锚；恢复前必须同时验证清单、全部对象 hash 和锚点引用。WAL 文件名、大小、metadata 与内容 hash 均 fail closed，同名重放只接受完全相同的 bytes。物理恢复后 identity sequence 只承诺继续单调前进，不承诺与已提交审计记录连续：PostgreSQL sequence 的非事务语义和恢复重放会留下合法间隙，审计正确性仍由 record count、递增 sequence、previous hash 和 head 全链共同验证。

Phase 1K 用一个固定 PostgreSQL advisory lock 建立 query-control 写入备份窗口。每个审计、quarantine latch 和 clear 事务必须先取得 shared transaction lock；备份协调器以 auditor 身份取得 exclusive session lock，只有既有 shared writer 全部结束后才返回成功，此后新写事务立即 fail closed。协调器持续 heartbeat 并把 fence UUID/取得时间写入物理备份 manifest v2；publisher 还会从 `pg_stat_activity/pg_locks` 反查指定 operator 的排他锁仍存在。这个机制只 drain PostgreSQL query-control 写事务，不声称停止已经在 ClickHouse/Parquet 执行的应用请求；这些请求在窗口内无法提交审计，因此对外仍 fail closed。

Phase 1L 把物理备份清单升级为 manifest v3，并补上可独立复验的连续 WAL 覆盖证明。publisher 以 auditor 在单个 SQL statement 内捕获 recovery target time、target LSN、PostgreSQL 计算的 WAL filename 与 segment size；再从 base backup `end_lsn` 到 target LSN 按同一 timeline 枚举每个 segment。只有所有不可变 WAL 对象的 URI、metadata、hash 和精确大小均验证成功，且 Phase 1K fence 与 Phase 1I anchor 仍有效，才发布签名清单。独立 verifier 会重新读取整段 WAL；缺段、跨 timeline、区间倒退、边界算法不符或超过有界段数均 fail closed。该证明覆盖单个已签名恢复目标，不等于长期归档健康、生产 RPO/RTO 或自动调度。

Phase 1M 增加一个由外部调度器调用的一次性物理备份 job。它先用专用 PostgreSQL `LOGIN REPLICATION` 非 superuser 角色执行有界 `pg_basebackup`，严格接受 tar/gzip/streamed-WAL 的三个预期 artifact，再在 Phase 1K exclusive fence 内运行 Phase 1L bundle，最后只输出绑定 manifest、anchor、恢复目标和 fence 的结构化 receipt。每台主机使用 kernel file lock 拒绝本机重叠任务，多主机仍由 PostgreSQL exclusive fence 串行化一致窗口。`pg_basebackup` 子进程只接收 allowlist libpq 环境，后续窗口不继承 replication passfile；staging/lock/passfile 必须归服务账号所有且不可被组或其他用户访问。仓库提供 hardened systemd oneshot/timer 和 journal failure-signal 模板，但模板没有安装，journal 信号也不是已经接通的告警路由。

Phase 1N 为恢复候选选择增加 fail-closed 门禁。操作员必须明确提供最多 32 个 Phase 1M canonical 成功回执；选择器不会使用 S3 list 或隐式“最新”查询。它只读取每个签名 manifest 的 metadata，逐项把 unsigned job receipt 与 HMAC 保护的 backup ID、manifest/anchor hash、恢复目标、WAL 覆盖和 fence 对账，并要求 cluster ID、PostgreSQL system identifier、timeline 与操作员预期完全一致。唯一最新目标必须落在默认 30 小时新鲜度和 5 分钟未来时钟偏差内；并列、漂移、过期或跨身份均拒绝。只有选中项随后下载全部 artifacts、连续 WAL 和 anchor 做完整 verify；失败不会静默降级到更旧备份。输出明确声明只是 supplied set 内最新而不是全局 latest，也不会自动执行恢复或提升数据库。

冷端 pruning 只能建立在不改变首事实 identity 语义的证明上；仅凭 segment 时间范围不能安全跳过同一逻辑流的历史 segment。PostgreSQL 控制面故障时，实例不得继续提供未审计的查询或操作热端；冷 Parquet 仍是数据正确性权威，但该 HTTP 服务本身应因审计/控制依赖不可用而 fail closed。审计表当前仍按完整单链和全局唯一 sequence/hash 验证；在定义分区键、跨分区唯一性、链 checkpoint、备份和法定保留要求前，不启用自动分区或删除。

### SQLite

继续服务 personal Profile、测试和便携离线模式。服务器模式不得把 SQLite 放在共享写入或跨节点所有权路径上。

## 6. 实时前端

- HTTP 查询服务和 WebSocket 网关保持无状态或仅持有可重建连接状态；
- 用户、组织、工作区和数据权限由控制平面授权；
- 网关不能各自重复建立交易所连接；共享上游订阅由数据平面管理；
- 慢客户端使用有界队列、合并策略和明确的 stale 状态，不允许反压采集器；
- 热点流允许 fan-out 缓存，但缓存不成为权威历史来源；
- 外部 API 保持版本化，现有前端优先通过兼容适配继续使用 /api/v1。

## 7. 回放扩展

现有单写者 Actor 是必须保留的确定性边界。服务器版通过增加 Actor 和 Worker 数量扩展，而不是并发修改一个账户。

PostgreSQL 保存 session_id 到 worker_id 的租约、fencing epoch、状态摘要和 checkpoint 目录。Replay Worker 从 ClickHouse 或 Parquet 读取冻结行情，在内存推进热状态并周期性提交 checkpoint。网关按 session_id 路由命令；Worker 丢失租约后必须停止写入。

故障接管验收必须比较 cursor、source event chain、state hash、component hash、订单、成交和账本，不能只比较最终权益。

## 8. 研究与任务隔离

研究调度器只管理任务元数据和配额；实际 Python、Rust 或 GPU 工作负载运行在独立 Worker。任务输入必须引用不可变 data_epoch，输出必须记录代码版本、环境镜像、参数、种子、输入 manifest 和结果 hash。

资源优先级固定为：

1. 采集连续性和持久化；
2. 实时分发；
3. 交互查询；
4. 回放交互；
5. 批量研究和训练。

每类工作负载使用独立队列、并发限额和资源池。ClickHouse 也需要独立用户、查询限制和超时，防止研究查询拖垮写入与前端查询。

## 9. 恢复与一致性

- 事件日志确认前，生产者可重试；确认后必须可由消费者重新读取；
- 每个消费者持久记录自身 offset，不共享进程内游标；
- ClickHouse 批次提交、归档对象 commit 和 offset 推进必须可对账；
- 对象写入先生成临时对象，再以带 data_epoch、snapshot_version 和 hash 的 manifest commit 宣告可见；
- 进程重启、节点重启和网络分区均需故障注入测试；
- 恢复时宁可停止发布或标记 stale，也不发布无法证明连续的数据。
- PostgreSQL PITR 只有在备份清单、审计锚、目标时间和目标所需 WAL 都经验证后才能提升；缺少任一对象或校验不匹配时不得启动查询运行时。

## 10. 可观测与数据质量

每个市场流至少暴露：最后 event_time、最后 received_at、消息速率、字节速率、sequence gap、重复数、hash 冲突、生产者 epoch、事件日志积压、ClickHouse 落后量和归档落后量。

平台层至少暴露：API/WS 延迟、连接数、慢客户端、查询资源、Replay Actor 数、任务队列、Worker 心跳、租约冲突、checkpoint 年龄和恢复结果。

健康接口不能只报告进程存活。采集健康必须能证明数据正在推进并成功持久化。

## 11. 语言边界

- Python/FastAPI 继续承担控制平面、API、研究编排和现有生态；
- Rust 只在真实剖析确认后承担高频采集、协议解析、盘口重建或回放热点；
- TypeScript/React 前端继续复用；
- Phase 0 不引入 Go，后续也不同时维护功能重叠的 Go 与 Rust 数据平面。

## 12. 已决与待决事项

已决：PostgreSQL 控制平面、ClickHouse 分析存储、Kafka-compatible 事件日志、Parquet 对象归档、RFC 8785 payload hash、不可变版本化 manifest、单会话单写者回放、Profile fail closed。

待 Phase 1 实测后决定：Kafka 与 Redpanda 的具体实现、对象存储实现、ClickHouse 单机或集群拓扑、Rust 迁移点、生产硬件数量和数据保留成本。
