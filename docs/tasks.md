# 待办与状态 (Tasks)

## 优化总览文档

- 端到端最佳实践优化架构与执行清单见：`docs/etl_best_practice_todolist.md`

## 已实现（核心链路）

- [x] ingest/ 归类（discovery、recorder、binance、chainlink、tracker、validate、test_pipeline）
- [x] eda 目录结构（01_etl、02_visual、03_analysis、04_reports）
- [x] ETL：BookTicker 主骨架 + AggTrades 窗内特征 + Polymarket（question 解析 label）+ Chainlink 按 feed
- [x] ETL 参数：--market-ids、--start-ts-ms、--end-ts-ms、--last-hours；config 中 max_* 限制行数
- [x] select_top2_markets：24h PnL 按 conditionId、crypto_only 筛选、conditionId→market_id 映射、--run-etl 一键
- [x] 时间窗口优先用 API timestamp（成交时间）；price/size 多字段解析与 0–1 校验
- [x] top2 输出诊断：trades_in_window、markets_with_trades、total_pnl_usd、crypto_only
- [x] ETL 进度显示：[1/5]～[5/5]、读文件与条数、窗内特征百分比
- [x] 采集容器网络与代理稳定化（host 网络 + `127.0.0.1:7897`）
- [x] `compact` 增量改造：按 `inode+offset+carry` 读取 raw 新增行
- [x] bronze 输出改为 append-only `part-*.parquet`
- [x] `silver` 增量改造：按 `silver_<source>.json` checkpoint 仅处理新增 bronze
- [x] silver 输出改为 append-only `part-*.parquet`
- [x] 采集与管道文档同步（`README.md`、`PIPELINE.md`）

---

## 待做（保留项）

### 第三阶段：分析 (eda/03_analysis)

- [ ] 描述性统计：各源价格/价差的分布、分位数。
- [ ] 延迟分析：Binance 与 Chainlink 的时间差分布（如 Binance 跳变到 Chainlink 更新的间隔）。
- [ ] 套利窗口统计：价差超过某阈值的持续时间、出现频率等。
- [ ] 输出到 `eda/03_analysis/output/`（表格、摘要 JSON/CSV）。

### 第四阶段：报告 (eda/04_reports)

- [ ] 汇总多窗口/多市场的图表与统计，生成可读报告（Markdown 或 HTML）。
- [ ] 输出到 `eda/04_reports/output/`。

### 数据与脚本（通用）

- [ ] `chainlink` 健康检查稳定性（偶发 unhealthy）排查与修复。
- [ ] `visual` 容器 unhealthy 原因定位与修复（输出文件 freshness/依赖失败）。
- [ ] 多 symbol ETL 设计（单次扫描多币种，减少重复读 raw）。
- [ ] 增加 bronze/silver 小文件合并任务（定时 compact 以降低查询开销）。

### 0x8dxd 监控系统缺失组件

#### P0 — 策略逆向必需

- [ ] **实时 Activity 采集**：当前 tracker 仅拉 `/trades`。需新增 `/activity` 轮询，采集 REDEEM（结算）、REWARD、MAKER_REBATE 等事件。这些是判断盈亏闭环的核心：只看 trades 无法知道最终结算结果。
- [ ] **实时 Positions 快照**：定期拉取 `/positions`，记录持仓变化（size/avgPrice/curPrice/cashPnl）。当前只有一次性下载的快照，无法看持仓随时间的演变。
- [ ] **Orderbook 快照采集**：在他每笔成交前后，记录该 Polymarket 市场的 orderbook 深度。用于判断他是 taker 还是 maker、是否在特定 spread 条件下触发下单。当前完全缺失。
- [ ] **Binance 价格对齐**：虽然已采集 Binance aggTrades/bookTicker，但未与 0x8dxd 的交易时间戳做实时对齐。需要在 tracker 或 ETL 层加入时间戳匹配逻辑，将他每笔 Polymarket 交易映射到最近的 BTC/ETH 现货价格。

#### P0.5 — 策略验证

- [ ] **Binance 现货价格对齐验证**：将 0x8dxd 每笔 Polymarket 交易的时间戳对齐 Binance BTC/ETH 现货价格（aggTrades），验证他是否在现货已出现短期动量后才入场。具体方法：对每笔交易取前 1/5/15 分钟的现货涨跌幅，检验其与下注方向（Up/Down）的相关性。若相关性显著，则证实"看现货动量下注"假说。

#### P1 — 提升分析质量

- [ ] **市场元数据采集**：每个 conditionId 对应的市场创建时间、到期时间、结算规则（price threshold / up-down 定义）。当前只有 title/slug，缺少结构化的规则字段。需要调 Polymarket CLOB API (`/markets`) 或 Gamma API 获取。
- [ ] **同市场其他参与者的成交流**：拉取同一 conditionId 下所有人的 trades（不只是 0x8dxd 的），用于判断他在市场中的占比、是否是主力、是否有对手方固定模式。
- [ ] **历史数据补全**：Activity API 有 ~3500 条上限。需要分时间段批量拉取（按 `before`/`after` 参数分页），或从链上 Polygon 直接解析 CTFExchange 合约事件日志，补全完整交易历史。
- [ ] **Tracker 轮询频率提升**：当前 30s 一次，对于 1-5 分钟窗口的市场偏慢。考虑改为 5-10s，或改用 WebSocket（如果 Data API 支持）。

#### P2 — 系统健壮性

- [ ] **Tracker 多端点合并**：将 trades / activity / positions 三个端点的轮询合并到同一个 tracker 服务中，共享 checkpoint 和 session，减少容器数量。
- [ ] **成交→结算→PnL 自动关联**：在 silver/mart 层建立 trade → redeem 的关联表（按 conditionId + outcome 匹配），自动计算每个市场的完整 PnL 闭环。
- [ ] **告警增强**：当检测到 0x8dxd 开始交易新市场时推送通知（Telegram/webhook），支持实时跟单场景。
- [ ] **双边下注检测器**：自动检测同一 conditionId 下同时持有 Yes 和 No 仓位的情况，计算对冲比例和净敞口，作为 mart 层的衍生指标。

---

## 对新对话有用的信息

1. **top2.json 长什么样**：含 `window_start_ts_ms`、`window_end_ts_ms`、`max_profit`/`max_loss`（各含 conditionId、market_id、pnl、cost、revenue、slug、title）、`market_ids_for_etl`（字符串数组）。用 top2 跑 ETL 时：`--market-ids id1,id2 --start-ts-ms <start> --end-ts-ms <end>`。
2. **ETL 卡顿**：通常卡在「读大 JSONL」或「窗内特征逐行算」。已加进度；若仍慢可先设 config 里 `max_book_rows`/`max_agg_rows` 做小范围试跑。
3. **conditionId 与 market_id**：Polymarket 盘口 `raw_data.market` 即 conditionId；ETL 通过扫描 `market_*.jsonl` 建立 conditionId→market_id。
4. **验证 top2 数字**：可用 jq 或小脚本从 trades.jsonl 按 conditionId 筛出两条市场，对 cost/revenue/pnl 加总与 top2 对照；链上用 transactionHash 上 Polygonscan 核对单笔。
5. **环境变量**：`POLY_DATA_DIR` 未设置时，`--data-dir $POLY_DATA_DIR` 会变成 `--data-dir` 无参数，argparse 报错；可不传 `--data-dir` 用默认，或先 `export POLY_DATA_DIR=...`。
