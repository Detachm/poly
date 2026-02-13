# 第一阶段：数据清洗与对齐 (ETL & Alignment)

- **主骨架**：Binance BookTicker 的 `local_receipt_ts_ms`
- **AggTrades**：过去 N ms 的主动买/卖量、成交量、VWAP、笔数（窗内衍生指标）
- **Polymarket**：从 `targets.json` 的 question 解析 label（如 btc_feb），按 market 合并 best_bid / best_ask
- **Chainlink**：按 feed 合并 answer（如 chainlink_BTC_USD）

## 运行

```bash
# 使用默认数据目录（环境变量 POLY_DATA_DIR 或 /vault/core/data/poly）
python eda/01_etl/scripts/run_etl.py

# 指定数据目录（相对项目根或绝对路径）
python eda/01_etl/scripts/run_etl.py --data-dir eda/01_etl/test_data
```

## 配置

`config/etl_config.json`：

- `data_dir`：留空则用 `POLY_DATA_DIR`
- `window_ms`：AggTrades 窗长（默认 100）
- `polymarket_tolerance_ms`：Polymarket merge_asof 容差（默认 1000）
- `symbol_filter`：Binance 交易对，如 btcusdt
- `output_filename`：输出文件名（默认 master.parquet）

输出写入 `eda/output/`（与 02_visual 共用）。

## 按 0x8dxd 盈亏选市场再清洗（筛选 + 对齐）

先根据 0x8dxd 过去 N 小时成交按市场算 PnL，**只保留买入次数 ≥ --min-buys（默认 10）的市场**，再从中取最大盈利/最大亏损代表；输出所有符合条件市场的列表（`markets`），ETL 与画图会按该列表处理（有多少市场画多少张图）。

```bash
# 1. 仅筛选：输出 JSON（含 markets、market_ids_for_etl、时间窗口、symbol_for_etl）
python eda/01_etl/scripts/select_top2_markets.py --data-dir $POLY_DATA_DIR --hours 24 --output eda/01_etl/output/top2.json

# 只保留买入次数 >= 5 的市场（默认 10，可降低以包含更多市场）
python eda/01_etl/scripts/select_top2_markets.py --data-dir $POLY_DATA_DIR --hours 24 --min-buys 5 --output eda/01_etl/output/top2.json

# 2. 按 top2.json 手动跑 ETL（须指定与市场币种一致的 Binance 交易对，或用 --top2 自动读）
python eda/01_etl/scripts/run_etl.py --data-dir $POLY_DATA_DIR \
  --symbol ethusdt \
  --market-ids 1303355,1303353 \
  --start-ts-ms 1699913610000 --end-ts-ms 1700000010000

# 或传入 top2.json，自动使用其中的 symbol_for_etl（与选出的市场币种一致）
python eda/01_etl/scripts/run_etl.py --data-dir $POLY_DATA_DIR --top2 eda/01_etl/output/top2.json \
  --market-ids 1303366,1334201 --start-ts-ms 1770786085288 --end-ts-ms 1770872485288

# 3. 一键：筛选 + 按币种分别跑 ETL（每个币种输出 master_<symbol>.parquet，保证 BN + Chainlink 与市场一致）
python eda/01_etl/scripts/select_top2_markets.py --data-dir $POLY_DATA_DIR --hours 24 --run-etl
```

- **select_top2_markets.py**：读 `tracker_0x8dxd/trades.jsonl`，过去 N 小时按 conditionId 算 PnL；**只保留 n_buys ≥ --min-buys（默认 10）的市场**，输出 **markets**（列表，每项含 **coin**）、max_profit/max_loss、market_ids_for_etl、window_*、symbol_for_etl。`--run-etl` 时**按币种分组**，每个币种单独跑 ETL（`--symbol btcusdt` + `--output-filename master_btcusdt.parquet` 等），避免 BTC 市场用 ETH/SOL 的 BN 或 Chainlink。
- **run_etl.py** 参数：`--symbol`、`--output-filename`（如 master_btcusdt.parquet，用于按币种输出）、`--top2`、`--market-ids`、`--start-ts-ms`/`--end-ts-ms`、`--last-hours`。
