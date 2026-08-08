# CandleScope Server Phase 0 执行与验收

状态：COMPLETE
阶段目标：冻结服务器产品、数据和部署边界，不宣称服务器运行时可用。

## 1. 本阶段交付物

| 交付物 | 路径 | 作用 |
| --- | --- | --- |
| 产品合同 | docs/server/CANDLESCOPE_SERVER_PRODUCT_CONTRACT_zh.md | 定义用户、工作负载、共享与隔离边界 |
| 架构合同 | docs/server/CANDLESCOPE_SERVER_ARCHITECTURE_zh.md | 定义平面、所有权、数据流与恢复规则 |
| 容量 Envelope | docs/server/contracts/phase0-capacity-envelope-v1.json | 保存暂定压测目标，不作为容量声明 |
| JSON Schema | docs/server/contracts/market-event-envelope-v1.schema.json | 跨语言事件 wire contract |
| RFC 8785 golden vectors | docs/server/contracts/rfc8785-payload-golden-vectors-v1.json | 锁定数字、负零、Unicode 排序和 SHA-256 的跨语言结果 |
| Node.js RFC 8785 verifier | backend/scripts/verify_server_rfc8785_vectors.mjs | 用独立 ECMAScript 运行时验证共享 golden vectors |
| 部署 Profile | backend/app/deployment/profile.py | 严格解析 personal/server 并 fail closed |
| Python 事件模型 | backend/app/server_contracts/market_event.py | 规范身份、顺序、时间和 payload hash |
| 服务端口 | backend/app/server_contracts/ports.py | 隔离事件日志、查询和归档具体客户端 |
| 合同测试 | backend/tests/test_server_phase0_contracts.py | 正向、往返、负面和配置测试 |
| 架构门禁 | backend/tests/test_server_phase0_architecture.py | 防止合同包依赖具体传输或存储框架 |

## 2. Phase 0 不改变的行为

- 默认启动路径仍是 personal；
- 当前 FastAPI、SQLite、进程内 EventBus、前端和回放行为不改变；
- 不安装 PostgreSQL、ClickHouse、Kafka/Redpanda 或 MinIO；
- 不把现有 MarketEvent 自动发布到任何外部系统；
- 不开启 server Profile，也不发布性能或可靠性声明。

## 3. 门禁

Phase 0 只有在以下结果全部通过后才能标记 COMPLETE：

1. 新增合同测试全绿；
2. 相关现有市场流模型测试全绿；
3. Python compileall 通过；
4. JSON 合同文件无重复键、可按 Draft 2020-12 元数据严格解析；
5. git diff --check 通过；
6. personal 默认和角色绑定保持稳定；
7. server Profile 的 require_runtime_support 明确拒绝；
8. 事件 wire 往返不改变 event_id、partition_key、payload、canonicalization 或 hash；
9. ordered_delta 缺失 sequence、payload hash 冲突和未知 wire 字段均 fail closed；
10. Python 和 Node.js 分别验证同一组 RFC 8785 golden vectors，覆盖 ECMAScript 数字、负零和 UTF-16 属性排序；
11. ArchiveCommit 和 MarketEventQuery 显式绑定 data_epoch、snapshot_version、不可变 manifest hash 和覆盖范围；
12. server_contracts 不导入 FastAPI、SQLite、Kafka、ClickHouse、PostgreSQL 或对象存储 SDK。

## 4. Phase 1 唯一首条纵向链路

Phase 1 从单一真实高频通道开始：

    Binance BTCUSDT aggTrade
      -> 现有 Python ingestion adapter
      -> MarketEventEnvelope v1 adapter
      -> Kafka-compatible event log
      -> batch ClickHouse writer
      -> immutable Parquet archive
      -> versioned manifest commit
      -> snapshot-pinned query adapter
      -> 一个现有 API 或回放读取入口

选择 aggTrade 是为了覆盖真实持续吞吐、幂等、sequence、重连和回放读取，同时避免第一步就承担完整 L2 盘口恢复复杂度。

## 5. Phase 1 硬验收

- 同一通道连续运行 24 小时；
- 生产者、事件日志、消费者和存储分别重启后可恢复；
- 已确认事件零丢失，重复写入可幂等；
- Event log、ClickHouse 与 Parquet 的 data_epoch、snapshot_version、数量、范围、sequence、manifest 和 payload hash 可对账；
- 故意制造 sequence gap 或同 identity 不同 hash 时 fail closed；
- personal Profile 的完整既有门禁保持通过；
- 不以单个进程 PID 存活代替数据正在推进且已持久化的证据。

## 6. Phase 1 前仍需实测的输入

容量 Envelope 的数字是初始公司级目标。开始硬件和分区规划前，Phase 1 必须记录实际交易所/市场/通道目录、平均与峰值事件大小、事件率、日增量、热点 symbol 分布，以及前端订阅和回放使用模型。没有这些数据时不得承诺节点数量或保留成本。

## 7. Phase 0 验证结果

机器可读证据位于 evidence/phase0-verification.json。Phase 0.1 合同与市场流测试 58 项通过；Python 与 Node.js 对同一组 RFC 8785 golden vectors 得到相同结果；Draft 2020-12 Schema、严格无重复键解析、Ruff、格式、compileall、三个 JSON 合同、依赖一致性和 git diff 检查通过。

全量 Replay 相关测试为 782 通过、1 失败。唯一失败在未修改的 Windows 原仓库同一 HEAD 上可稳定复现，属于既有错误码期望漂移，不由本阶段引入。WSL 无法执行绑定 Windows AMD64 官方插件 bundle 的 release gate；该平台边界单独记录，不作为 Linux 服务器能力已交付的证据。
