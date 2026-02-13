# 第二阶段：可视化验证 (02_visual)

读取 ETL 输出的 `master.parquet`，**按 top2 的每个市场单独出图**。每张图包含：

- **对应币种的 Binance 盘口价格**（b/a 中价）
- **逐笔聚合**：100ms 窗内 VWAP（叠加在价格图）、成交量与买卖量（下方子图）
- **Chainlink 价格**（阶梯线，按市场币种自动选 feed，如 ETH→chainlink_ETH_USD）
- **Binance − Chainlink 价差**（右轴）
- **0x8dxd 买卖点**（红三角/绿三角，仅该市场的成交）

**时间窗口**：默认为该**市场在数据中的存续期间**（该市场 Polymarket best_bid/best_ask 有值的区间），即从 parquet 里该市场列首次/末次有值的时间段。

## 运行

在**项目根目录**执行：

```bash
# 按 top2 两个市场各出一张图，时间为各市场存续期
python eda/02_visual/scripts/plot_window.py

# 只出一张图：用 top2 总窗口或全量
python eda/02_visual/scripts/plot_window.py --single

# 单图 + 最近 5 分钟
python eda/02_visual/scripts/plot_window.py --single --last-minutes 5

# 指定起止时间（毫秒时间戳）
python eda/02_visual/scripts/plot_window.py --single --start-ts-ms 1770786085288 --end-ts-ms 1770786385288

# 指定 parquet / top2 / 数据目录
python eda/02_visual/scripts/plot_window.py --parquet eda/output/master.parquet --top2 eda/01_etl/output/top2.json --data-dir $POLY_DATA_DIR

# 不画 0x8dxd 买卖点
python eda/02_visual/scripts/plot_window.py --no-trades
```

## 配置

`config/visual_config.json`：

- `master_parquet`：ETL 输出路径（默认 `eda/output/master.parquet`）
- `top2_json`：top2 筛选结果，用于时间窗口与 conditionId 过滤（默认 `eda/01_etl/output/top2.json`）
- `window_minutes`：未指定起止时的参考窗口长度（分钟）
- `chainlink_feed`：指定 Chainlink 列名（如 `chainlink_ETH_USD`），留空则用 parquet 中第一个 `chainlink_*` 列
- `output_dir`：图表输出目录（默认 `eda/02_visual/output/`）
- `figure_dpi`：输出 PNG 分辨率（默认 150）

## 输入与窗口

- **输入**：有 top2 且存在按币种 ETL 输出时，按市场币种读取 `eda/output/master_<symbol>.parquet`（如 BTC 市场用 `master_btcusdt.parquet`），保证 BN 盘口与 Chainlink 均为该币种。否则回退到单文件 `eda/output/master.parquet`（或 `--parquet`）。可选 `eda/01_etl/output/top2.json`（`--top2`）提供 markets 列表（含 coin）、时间窗口与 conditionId。
- **选窗口**：  
  - 若提供 `top2.json` 且未传 `--start-ts-ms` / `--end-ts-ms` / `--last-minutes`，则用 top2 的窗口；  
  - `--last-minutes N`：取 parquet 末尾最近 N 分钟；  
  - 否则用 `--start-ts-ms` / `--end-ts-ms`，或整段数据。
- **0x8dxd 买卖点**：从 `$POLY_DATA_DIR/tracker_0x8dxd/trades.jsonl`（或 `--data-dir` 下）按时间窗口过滤，若有 top2 则只保留 max_profit / max_loss 对应 `conditionId` 的成交；用 `local_receipt_ts_ms`（或 `--use-receipt-time`）对齐到主时间轴。

## 输出

图表保存到 `eda/02_visual/output/`，默认文件名形如 `window_YYYYMMDD_HHMM_5m.png`；也可通过 `--output` 指定路径。

## 依赖

需安装 `matplotlib`（见项目根目录 `requirements.txt`）。
