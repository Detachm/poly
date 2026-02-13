# 项目规则与约定 (Rules)

## 代码与路径

- **数据根目录**：所有录制与 ETL 默认使用 `POLY_DATA_DIR`（未设置则为 `/vault/core/data/poly`）。脚本若接受 `--data-dir`，相对路径按**项目根**解析。
- **ingest 脚本调用**：在**项目根目录**执行，例如 `python ingest/recorder.py`、`python ingest/validate_timestamps.py`，不要 `cd ingest` 再跑（targets.json 在根目录）。
- **ETL 脚本**：可从根目录运行 `python eda/01_etl/scripts/run_etl.py`，或 `python eda/01_etl/scripts/select_top2_markets.py`；config 与 output 路径均相对脚本所在目录或项目根处理。
- **增量 checkpoint**：
  - raw->bronze：`checkpoint/compact_<source>.json`
  - bronze->silver：`checkpoint/silver_<source>.json`

## 时间戳

- **统一字段**：录制与 ETL 统一使用 **local_receipt_ts_ms**（毫秒）作为「本机收到」时间；API 返回的 **timestamp** 为成交时间（秒），用于 24h 窗口筛选时优先用 timestamp。
- **Polymarket**：merge_asof 时 tolerance 建议 1s；**Chainlink** 可不设或 60s，backward 取最近一条。

## 市场与标识

- **Polymarket**：  
  - 盘口文件为 `market_{market_id}.jsonl` 或 `market_{market_id}_{YYYYMMDDHH}.jsonl`；  
  - conditionId（API/链上）与 market_id（数字）通过 `raw_data.market`（盘口消息里的 conditionId）在 ETL 中建立映射。  
- **Crypto 市场**：与 `ingest/discovery.py` 一致，关键词为 Bitcoin、Ethereum、Solana、Crypto、BTC、ETH、SOL、XRP、Ripple；短词用词边界匹配。

## 数据格式

- **Binance**：agg_trades 含 `p`(价)、`q`(量)、`m`(maker 是否买方)；book_ticker 含 `b`/`a`(最优买卖价)、`s`(symbol)。stream 格式为 `btcusdt@aggTrade` / `btcusdt@bookTicker`，symbol 取 `@` 前部分。
- **Polymarket 盘口**：raw_data 为 book 消息，含 `bids`/`asks` 数组，每项 `price`/`size`；best_bid = max(bids.price)，best_ask = min(asks.price)。
- **Chainlink**：`updates.jsonl` 每行含 `feed`、`answer`（价格）；多 feed 时按 feed 分列合并。
- **Bronze/Silver 输出**：均为 append-only 分片 parquet（`part-*.parquet`），不对既有 parquet 做原地追加。

## 容器网络与代理

- `polymarket` / `discovery_loop` / `binance` / `chainlink` / `tracker_0x8dxd` 使用 `host` 网络模式。
- 默认代理地址使用 `http://127.0.0.1:7897`（`HTTP_PROXY` / `HTTPS_PROXY`）。
- 若出现握手超时，先检查本机代理端口监听与容器环境变量。

## 验证与调试

- **top2 PnL 交叉核对**：用 `trades.jsonl` 按 conditionId 筛出两条市场的成交，手算 cost/revenue/pnl，与 top2.json 中 max_profit/max_loss 对照。
- **链上验证**：每条 trade 有 `transactionHash`，可在 Polygonscan 查该 tx 的 Token Transfers 与记录是否一致。
- **ETL 进度**：run_etl 已加 [1/5]～[5/5] 及读文件/窗内特征百分比进度，便于定位卡顿阶段。

## 文档与规则存放

- 整体架构、未实现细节、对新对话有帮助的上下文：放在 **`docs/`** 下的 `plan.md`、`rules.md`、`tasks.md`。
- 各阶段用法与参数：见 `README.md`、`PIPELINE.md`、`eda/01_etl/README.md`。
