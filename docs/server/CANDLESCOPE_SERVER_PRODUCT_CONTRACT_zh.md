# CandleScope Server 产品合同 v1

状态：FROZEN_FOR_PHASE_1
合同版本：candlescope.server-product.v1
适用范围：公司内部部署的 CandleScope 逻辑集群

## 1. 产品定义

CandleScope Server 是“一家公司部署一套”的内部量化数据与研究平台。用户只感知一个访问入口、一套身份权限、一套数据目录和一套 API；实现允许由多个进程、节点和专用资源池共同组成。

它不是由项目方托管全世界用户的公网 SaaS，也不是把个人版 FastAPI 进程放到一台配置更高的机器上。

## 2. 必须支持的工作负载

1. 交易所行情 24 小时持续采集、校验、补洞和不可变归档。
2. 多个团队和大量独立前端同时查看实时与历史数据。
3. 交互式指标查询、告警、研究数据读取和结果共享。
4. 多个相互隔离的确定性回放会话。
5. 可排队、可取消、可限额、可复现的批量量化分析与模型任务。
6. 采集、实时分发、交互查询、回放和批量分析之间的资源隔离。

Phase 0 的暂定验证负载见 contracts/phase0-capacity-envelope-v1.json。其中数字是架构与压测目标，不是当前版本容量声明，也不是硬件采购结论。

## 3. 双部署 Profile

同一仓库保留两个显式 Profile：

| Profile | 定位 | 控制数据 | 行情分析 | 事件传输 | 归档 |
| --- | --- | --- | --- | --- | --- |
| personal | 个人、离线、开发 | SQLite | SQLite | 进程内 | 本地文件 |
| server | 公司内部逻辑集群 | PostgreSQL | ClickHouse | Kafka-compatible | 对象存储中的 Parquet |

环境变量为 CANDLESCOPE_PROFILE=personal 或 server。默认值必须始终是 personal。在服务器依赖和恢复门禁完成前，server 必须拒绝启动，不能静默退回 SQLite 或进程内事件总线。

## 4. 共享与隔离边界

应共享：

- 前端组件、HTTP/WS 外部 DTO 和 /api/v1 兼容面；
- 交易所适配器、标准化规则、市场流身份和数据质量规则；
- 指标定义、插件协议和回放确定性领域模型；
- 时间语义、费用规则、精度规则和 fail-closed 能力状态。

必须隔离：

- 数据库、事件日志、归档和跨进程分发的具体实现；
- 在线 API、采集、回放、研究任务和插件执行的资源池；
- 组织、团队、工作区、数据集和任务的权限范围；
- 单会话回放 Actor 的所有权与故障接管。

禁止复制一份前端或长期维护两套领域模型。Profile 差异通过端口和适配器实现，不允许把 if server_mode 散落在业务逻辑中。

## 5. 不可妥协的数据语义

1. 同一个逻辑市场流使用规范化身份 exchange + market_type + symbol + channel + params。
2. 每个跨进程事件使用 MarketEventEnvelope v1，同时记录事件时间、接收时间和发布时间；payload 以 RFC 8785/JCS 规范化并绑定 SHA-256。
3. 同一个逻辑市场流固定使用同一分区键；有序增量不得随机分区。
4. 交付按至少一次设计。生产者重试必须复用同一个 event_id；消费者必须幂等。
5. 同一事件身份出现不同 payload hash 时必须隔离并告警，不能后写覆盖。
6. 顺序缺口、队列溢出、生产者 epoch 回退、盘口交叉或归档对账失败必须显式降级或 fail closed。
7. 已确认写入事件不允许丢失。是否“已确认”以事件日志返回的持久化回执为准。
8. Parquet 归档是不可变重建来源；任何查询、回放或重算都必须绑定 data_epoch、snapshot_version、不可变 manifest hash、输入范围和代码版本。

## 6. 回放合同

- 每个回放会话保持单写者 Actor，不允许通过并发修改账户来换吞吐。
- 不同会话可按 session_id 分配到不同 Replay Worker。
- 全局会话目录、所有权租约和 fencing epoch 属于控制平面。
- 行情读取来自版本化 ClickHouse 数据或不可变归档；热状态在 Worker 内存，持久恢复依赖 checkpoint。
- Worker 接管后必须证明 cursor、事件链、组件 hash 和账户状态一致。
- 回放和实时行情运行时保持隔离；任何历史能力缺失都返回明确 capability，而不是回退实时数据。

## 7. 量化任务合同

- 任意用户代码和重型分析不得运行在 API 或采集进程内。
- 调度器必须提供组织配额、优先级、取消、超时、checkpoint 和审计。
- CPU、GPU 和内存密集型任务使用独立 Worker 池。
- 每次任务绑定不可变数据快照、运行环境、代码版本、参数和随机种子。
- 批量任务不得挤占采集落盘、实时推送或回放交互预算。

## 8. 安全与租户边界

服务器版至少提供组织、团队、用户、工作区和服务账号；权限最小集合为管理员、研究员、交易员、只读用户。所有 API token、配置变更、数据导出、任务提交和回放控制均需审计。

插件和用户策略默认不可信。执行环境不得继承宿主密钥，网络、文件、CPU、内存和运行时间均须受限。Phase 0 只冻结边界，不声明隔离运行时已经交付。

## 9. Phase 0 明确不交付

- PostgreSQL、ClickHouse、Kafka/Redpanda、MinIO 的运行实例；
- 当前 MarketEvent 到持久事件日志的生产适配器；
- 多节点 API/WS 网关、Replay Worker 或分析 Worker；
- 性能、可用性、容灾或 24 小时运行声明；
- Rust 采集器或对现有 Python 热路径的替换。

## 10. Phase 0 完成条件

- 本合同和架构合同冻结；
- 容量目标以机器可读文件记录，且明确不是当前容量声明；
- personal Profile 默认兼容，非法 Profile 严格拒绝；
- server Profile 在运行实现缺失时 fail closed；
- MarketEventEnvelope v1 有 Python 模型、严格 JSON Schema、RFC 8785 跨语言 golden vectors、往返和负面测试；
- 归档提交和查询页显式绑定 data_epoch、snapshot_version、不可变 manifest 与覆盖范围；
- 服务器合同包不依赖 FastAPI、SQLite、Kafka 或 ClickHouse 客户端；
- Phase 1 入口、24 小时纵向链路和验收门禁明确。
