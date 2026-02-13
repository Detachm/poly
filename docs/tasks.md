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

### 数据与脚本

- [ ] `chainlink` 健康检查稳定性（偶发 unhealthy）排查与修复。
- [ ] `visual` 容器 unhealthy 原因定位与修复（输出文件 freshness/依赖失败）。
- [ ] 多 symbol ETL 设计（单次扫描多币种，减少重复读 raw）。
- [ ] 增加 bronze/silver 小文件合并任务（定时 compact 以降低查询开销）。

---

## 对新对话有用的信息

1. **top2.json 长什么样**：含 `window_start_ts_ms`、`window_end_ts_ms`、`max_profit`/`max_loss`（各含 conditionId、market_id、pnl、cost、revenue、slug、title）、`market_ids_for_etl`（字符串数组）。用 top2 跑 ETL 时：`--market-ids id1,id2 --start-ts-ms <start> --end-ts-ms <end>`。
2. **ETL 卡顿**：通常卡在「读大 JSONL」或「窗内特征逐行算」。已加进度；若仍慢可先设 config 里 `max_book_rows`/`max_agg_rows` 做小范围试跑。
3. **conditionId 与 market_id**：Polymarket 盘口 `raw_data.market` 即 conditionId；ETL 通过扫描 `market_*.jsonl` 建立 conditionId→market_id。
4. **验证 top2 数字**：可用 jq 或小脚本从 trades.jsonl 按 conditionId 筛出两条市场，对 cost/revenue/pnl 加总与 top2 对照；链上用 transactionHash 上 Polygonscan 核对单笔。
5. **环境变量**：`POLY_DATA_DIR` 未设置时，`--data-dir $POLY_DATA_DIR` 会变成 `--data-dir` 无参数，argparse 报错；可不传 `--data-dir` 用默认，或先 `export POLY_DATA_DIR=...`。
