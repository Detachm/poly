# poly 项目整体架构 (Plan)

## 目标

把「多源录制」得到的原始 JSONL 变成可分析的宽表与图表，支撑 0x8dxd 策略复盘：对齐时间轴、验证延迟与套利窗口、观察 0x8dxd 的动手时机。

---

## 数据流概览（当前实现）

```
┌─────────────────────────────────────────────────────────────────────────┐
│  ingest/ 数据采集                                                        │
│  Discovery → targets.json                                                │
│  Polymarket WS │ Binance (agg_trades + book_ticker) │ Chainlink │ Tracker│
│  输出: polymarket/ binance/ chainlink/ tracker_0x8dxd/ (JSONL + local_ts) │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  ingest/compact_raw_to_bronze.py：raw -> bronze                           │
│  • checkpoint: compact_<source>.json（inode/offset/carry/part_idx）      │
│  • 仅增量读取 JSONL 新增内容                                               │
│  • append 写 bronze part parquet                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  ingest/build_silver.py：bronze -> silver                                 │
│  • checkpoint: silver_<source>.json                                        │
│  • 仅处理新增 bronze parquet                                                │
│  • append 写 silver part parquet                                            │
└─────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  eda/01_etl 第一阶段：清洗与对齐                                          │
│  • 选市场: select_top2_markets.py (24h PnL, crypto_only → top2.json)    │
│  • 主骨架: BookTicker local_receipt_ts_ms                                │
│  • 挂载: AggTrades 窗内特征、Polymarket 盘口、Chainlink、可选时间/市场过滤 │
│  输出: eda/output/master.parquet                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  eda/02_visual 第二阶段：可视化验证（已实现）                              │
│  • 选一段波动窗口（如 5 分钟）                                             │
│  • X: 时间(秒)  Y左: Binance 价、Chainlink 价(阶梯)  Y右: Binance−Chainlink│
│  • 标记: 0x8dxd 买入(红三角)/卖出(绿三角)                                 │
│  输出: 图表 PNG/HTML → eda/02_visual/output/                             │
└─────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────┐
│  eda/03_analysis / 04_reports 预留                                        │
│  描述性统计、延迟分布、套利窗口统计、汇总报告                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 目录结构约定

| 路径 | 用途 |
|------|------|
| **ingest/** | 数据下载与录制脚本；入口在项目根，如 `python ingest/recorder.py` |
| **eda/01_etl/** | ETL 脚本与配置；output 与 02 共用 `eda/output/` |
| **eda/02_visual/** | 可视化脚本与输出（output/） |
| **eda/03_analysis/** | 分析脚本与输出 |
| **eda/04_reports/** | 报告输出 |
| **targets.json** | 项目根目录；Polymarket 目标市场，由 ingest/discovery 生成 |
| **POLY_DATA_DIR** | 数据根目录，默认 `/vault/core/data/poly` |

---

## 关键设计决策（已落地）

1. **主时间轴**：统一用 **local_receipt_ts_ms**（本机收到时间），不用交易所时间，以反映「交易员真实看到数据」的时刻。
2. **ETL 主骨架**：Binance **BookTicker**（最高频盘口）；AggTrades 不逐笔 merge，而是做**窗内衍生指标**（如 100ms 内主动买/卖量、VWAP、笔数）挂到主骨架上。
3. **Polymarket 市场标识**：从 `targets.json` 的 **question** 解析 label（方案 A，如 btc_feb、eth_feb）；同一 Book 时刻可对应多个市场的 best_bid/best_ask 列。
4. **Chainlink**：多 feed 时按 feed 分列（如 chainlink_BTC_USD）；对齐用 backward，不设或设较大 tolerance。
5. **0x8dxd 选市场**：按过去 24h 成交算每市场 PnL，取**最大盈利**与**最大亏损**各 1 个市场，且默认只统计 **Crypto** 市场（title/slug 含 BTC/ETH/SOL/XRP 等），再仅对这两市场做 ETL 时间窗口内的清洗对齐。
6. **增量落湖策略**：raw->bronze 使用按文件 offset 增量读取；bronze->silver 使用 checkpoint 增量消费；两层均为 append-only parquet 分片。
7. **容器网络策略**：核心采集与 discovery_loop 使用 host 网络并默认走 `127.0.0.1:7897` 代理，规避 bridge 到本机代理不可达问题。

---

## 配置文件与入口

- **ETL 配置**：`eda/01_etl/config/etl_config.json`（data_dir、window_ms、symbol_filter、output_filename、max_* 等）
- **数据目录**：未在 config 指定时用环境变量 `POLY_DATA_DIR`
- **一键筛选+ETL**：`select_top2_markets.py --hours 24 --run-etl`（需先设 POLY_DATA_DIR 或 --data-dir）
