# 多源录制 Pipeline（0x8dxd 策略数据基础设施）

**首次运行**：`pip install -r requirements.txt`

## 架构概览

```
Discovery (Crypto 定向) → targets.json
         ↓
┌────────┴────────┬─────────────────┬─────────────────┐
│ Polymarket WS   │ Binance         │ Chainlink       │ (可选) 0x8dxd
│ recorder.py     │ binance_recorder │ chainlink_recorder │ Tracker
└────────┬────────┴────────┬────────┴────────┬────────┘
         ↓                 ↓                 ↓
   data/polymarket/   data/binance/    data/chainlink/
   (JSONL + local_ts) (JSONL + local_ts) (JSONL + local_ts)
```

所有录制流均带 **local_receipt_ts_ms** 与 **unixtime**，回测时用其对齐多源。Polymarket 另写 `heartbeat.jsonl` 便于可观测。

## 1. Discovery（定向 Crypto）

```bash
# 仅 Crypto 相关市场（默认），成交量门槛放低
python discovery.py

# 全部市场
python discovery.py --all-markets
```

输出 `targets.json`，供 Polymarket 录制使用。

## 2. 多源并行录制（The Quartet）

### 方式 A：本机分别开终端

```bash
# 1. Polymarket WebSocket（优先用 recorder.py，低延迟）
python recorder.py --targets targets.json --output-dir data/polymarket

# 2. Binance 现货 AggTrades + BookTicker
python binance_recorder.py --output-dir data/binance

# 3. Chainlink Polygon 价格更新（默认 0.2s 轮询）
export POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY  # 可选
python chainlink_recorder.py --output-dir data/chainlink --poll-interval 0.2
```

### 方式 B：Supervisor

```bash
mkdir -p logs   # 首次运行前创建日志目录
supervisord -c supervisord.conf
# 查看: supervisorctl status
# 停止: supervisorctl shutdown
```

### 方式 C：Docker Compose

```bash
docker compose up -d
# 含 discovery_loop、polymarket、binance、chainlink、tracker_0x8dxd；需先有 targets.json（discovery_loop 会写）
# 代理: HTTP_PROXY/HTTPS_PROXY；Tracker: TRACKER_USER_ADDRESS
```

## 3. 输出目录与格式

| 源         | 目录              | 格式   | 说明 |
|------------|-------------------|--------|------|
| Polymarket | data/polymarket/  | JSONL  | market_{id}_{YYYYMMDDHH}.jsonl 按小时轮转，含 event_type、is_snapshot、unixtime；heartbeat.jsonl 每 60s |
| Binance    | data/binance/     | JSONL  | agg_trades / book_ticker，单文件 >100MB 轮转，含 unixtime |
| Chainlink  | data/chainlink/   | JSONL  | updates.jsonl，含 unixtime |
| Tracker    | data/tracker_0x8dxd/ | JSONL  | trades.jsonl，含 unixtime |

## 4. 时间戳校验与测试

```bash
python validate_timestamps.py --data-dir data --lines 20   # 各源最新 local_receipt_ts_ms 与时间差
python test_pipeline.py                                      # 最小化单元测试
```

## 5. 环境变量

- `HTTP_PROXY` / `HTTPS_PROXY`：Polymarket 录制（国内常需）。
- `POLYGON_RPC_URL`：Chainlink 监控用的 Polygon RPC（建议 Alchemy/Infura）。

## 6. 0x8dxd Tracker

```bash
export TRACKER_USER_ADDRESS=0x...   # 实际监控地址
python tracker_0x8dxd.py --output-dir data/tracker_0x8dxd --interval 30
```

轮询 Data API 获取该地址成交，写入 `data/tracker_0x8dxd/trades.jsonl`，每条带 `local_receipt_ts_ms`。

## 7. Discovery 每 10 分钟合并新市场

```bash
python discovery_loop.py   # 每 10 分钟 discovery 并合并新 Crypto 市场到 targets.json
python discovery_loop.py --once   # 只跑一次
```

## 8. 一键启动（本地测试）

```bash
./run_all.sh              # 先 discovery 一次（需可访问 Gamma API），再后台启动 5 个进程
./run_all.sh --skip-discovery   # 跳过 discovery，直接用现有 targets.json 启动录制
# 生产环境建议: mkdir -p logs && supervisord -c supervisord.conf
```
