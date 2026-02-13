# Data Pipeline 最佳实践优化架构与 TodoList

适用范围：本项目当前的四路数据源（Polymarket / Binance / Chainlink / 0x tracker）以及 `eda/01_etl` 与 `eda/02_visual` 的下游流程。

## 1) 目标与设计原则

- 解耦：采集（Ingest）与分析（ETL/可视化）分层，避免互相阻塞。
- 最终一致性：控制面（Discovery）描述目标状态，执行面（Recorder）持续收敛。
- 可恢复：保留原始层（WAL/JSONL），任何阶段失败可重放。
- 可扩展：统一 schema、分区策略、容器化部署，支持增量处理。
- 可观测：每层有健康指标、延迟指标、积压指标和告警策略。

---

## 2) 推荐目标架构（生产级）

### A. Ingest 层（实时采集）

- `discovery_loop`：持续刷新 `targets.json`（原子写入 + 失败不覆盖）。
- `recorder(polymarket)`：按 `targets.json` 动态增删市场任务，写原始事件。
- `binance/chainlink/tracker`：各自写原始事件流。
- 输出到 `raw` 区（按 source + 日期 + 小时组织）：
  - `raw/polymarket/dt=YYYY-MM-DD/hour=HH/*.jsonl`
  - `raw/binance/dt=YYYY-MM-DD/hour=HH/*.jsonl`
  - `raw/chainlink/dt=YYYY-MM-DD/hour=HH/*.jsonl`
  - `raw/tracker_0x/dt=YYYY-MM-DD/hour=HH/*.jsonl`

### B. Bronze/Silver 层（增量转存）

- 独立 `compact` 作业将 raw 增量转 Parquet，不影响采集。
- `bronze`：1:1 保留原始字段（类型规范化）。
- `silver`：统一字段命名、补充标准时间列、清洗异常值。
- 输出到 `lake` 区：
  - `lake/bronze/<source>/dt=.../hour=.../*.parquet`
  - `lake/silver/<source>/dt=.../hour=.../*.parquet`
- 使用 checkpoint/manifest 记录已处理文件，避免重复全量扫描。

### C. Feature/ETL 层（研究与建模输入）

- 主骨架：Binance book_ticker（时间轴）。
- 挂载：
  - Binance agg_trades 窗内特征（qty/vol/vwap/count）。
  - Polymarket best bid/ask（按 market_id）。
  - Chainlink feed（按币种）。
  - 0x 交易点（事件流或 merge_asof）。
- 输出宽表到 `mart`：
  - `lake/mart/aligned_master/symbol=btcusdt/dt=.../*.parquet`
  - 多币种分别输出，避免跨币种误对齐。

### D. Visualization 层（窗口可视化）

- 输入 `mart` 的宽表 + top2/markets 元信息。
- 每市场独立窗口图，避免一图混多市场语义。
- 输出规范化命名（symbol + market_id + window_start）。

### E. Orchestration & Ops

- 全容器化（compose 或 k8s）：
  - ingest-*（4~5 个）
  - compact-*（1~2 个）
  - etl-*（批处理）
  - visual-*（批处理）
- 每容器定义 healthcheck + restart 策略 + 资源限制。
- 统一日志结构化（json log）与指标上报。

---

## 3) 统一数据契约（建议）

### 公共字段（所有 source）

- `source`: 数据源标识
- `event_time_ms`: 源事件时间（若有）
- `ingest_time_ms`: 本机接收时间（local receipt）
- `unixtime`: 秒级时间戳
- `event_type`: 事件类型
- `instrument_id`: 标的主键（symbol / market_id / feed / conditionId）

### Source 扩展字段

- Binance：`symbol`, `b`, `a`, `p`, `q`, `m`, ...
- Polymarket：`market_id`, `token_id`, `best_bid`, `best_ask`, `is_snapshot`, ...
- Chainlink：`feed`, `answer`, `round_id`, ...
- 0x：`conditionId`, `side`, `price`, `size`, `tx_hash`, ...

### 文件与分区约束

- 分区优先：`dt`, `hour`。
- 文件大小目标：128MB~512MB（避免小文件灾难）。
- 压缩：优先 `zstd`，兼顾压缩率与读取速度。

---

## 4) 当前痛点到优化动作映射

- 痛点：JSONL 体积大，查询要全量遍历。
  - 动作：raw->parquet 增量 compaction + 分区裁剪。
- 痛点：`select_top2_markets.py` 运行慢。
  - 动作：`tracker` 按时间分区；conditionId->market_id 建持久化索引表。
- 痛点：`run_etl.py` 处理链路长、内存压力大。
  - 动作：读取时过滤（symbol/time）；窗口特征改前缀和或 DuckDB window。
- 痛点：多币种反复扫描同一批文件。
  - 动作：一次读取后按 symbol 分流并并行输出多个 `master_<symbol>.parquet`。

---

## 5) 分阶段 TodoList（按优先级）

## P0（本周必须完成）

- [x] Discovery 写 `targets.json` 改为原子写（tmp + fsync + replace）。
- [x] Discovery API 失败时不覆盖旧 `targets.json`（保留 last good）。
- [x] Recorder 改为周期 reconcile（diff: add/remove），不再仅启动时加载一次。
- [x] 统一 raw 目录结构为 `source/dt/hour`，新数据按小时滚动。
- [x] 新增 `compact` 作业：把 raw JSONL 增量转 bronze parquet。
- [x] 给 `select_top2_markets.py` 增加只读最近分区能力，避免扫全历史。
- [x] 生成并维护 `conditionId -> market_id` 索引文件（parquet/json）。
- [x] 为 ingest/compact/etl 容器补 healthcheck 与 restart 策略。

## P1（1~2 周）

- [x] 建立 silver 层统一 schema 与类型（严格字段字典）。
- [ ] ETL 读取改 predicate pushdown（按 symbol + 时间过滤）。
- [ ] Agg 特征计算改向量化前缀和或 DuckDB window，去除 Python 大循环。
- [ ] 多币种 ETL 一次扫描多输出（`master_btcusdt/ethusdt/...`）。
- [x] 可视化按市场自动批量出图，支持并发与失败重试。
- [x] 指标监控：targets_age、running_tasks、compact_lag、etl_latency。
- [x] 告警策略：Discovery 停更、Recorder 任务突降、Compact 积压。

## P2（2~4 周）

- [x] 引入数据质量校验（schema drift、空值率、时序倒退、重复率）。
- [x] 引入小文件合并策略（daily compaction）。
- [x] 建立实验数据集版本管理（按窗口和参数固化）。
- [x] 形成自动日报（统计 + 样例图 + 异常事件）。
- [x] 增加回放工具（指定时间窗重建 mart 数据）。

---

## 6) 建议目录蓝图

```text
poly/
  ingest/
  eda/
  docs/
  data/
    raw/
      polymarket/dt=2026-02-12/hour=07/*.jsonl
      binance/dt=2026-02-12/hour=07/*.jsonl
      chainlink/dt=2026-02-12/hour=07/*.jsonl
      tracker_0x/dt=2026-02-12/hour=07/*.jsonl
    lake/
      bronze/<source>/dt=.../hour=.../*.parquet
      silver/<source>/dt=.../hour=.../*.parquet
      mart/aligned_master/symbol=btcusdt/dt=.../*.parquet
    checkpoint/
      compact_<source>.json
      etl_<symbol>.json
```

---

## 7) 验收标准（Definition of Done）

- [ ] 任意 24h 窗口查询不再全量扫历史 JSONL。
- [ ] `select_top2_markets.py` 在同等数据量下耗时下降到可接受范围（目标 3x~10x）。
- [ ] `run_etl.py` 可按 symbol/时间快速出 `master_<symbol>.parquet`。
- [ ] 可视化支持多市场自动出图且不混币种。
- [ ] 整体链路具备告警与健康检查，异常可定位、可恢复。

