# 数据采集（多源录制）

本目录包含**数据下载 / 录制**相关脚本，与下游 **`eda/`** 分析流水线分离。

## 脚本说明

| 脚本 | 说明 |
|------|------|
| `discovery.py` | 扫描 Polymarket（Crypto 定向），生成/更新 `targets.json` |
| `discovery_loop.py` | 定时合并新市场到 targets.json（与 recorder 等并行） |
| `recorder.py` | Polymarket CLOB WebSocket 盘口录制 |
| `market_recorder.py` | Polymarket 备用录制实现 |
| `binance_recorder.py` | Binance AggTrades + BookTicker |
| `chainlink_recorder.py` | Chainlink Polygon 价格轮询 |
| `tracker_0x8dxd.py` | 0x8dxd 地址成交监控 |
| `validate_timestamps.py` | 多源时间戳与完整性校验 |
| `test_pipeline.py` | 最小化单元测试 |

## 运行方式

**请在项目根目录**执行（`targets.json` 与数据路径以根目录为基准）：

```bash
# Discovery（生成 targets.json）
python ingest/discovery.py

# 单源录制示例
python ingest/recorder.py --targets targets.json --output-dir $POLY_DATA_DIR/polymarket
python ingest/binance_recorder.py --output-dir $POLY_DATA_DIR/binance
python ingest/chainlink_recorder.py --output-dir $POLY_DATA_DIR/chainlink
python ingest/tracker_0x8dxd.py --output-dir $POLY_DATA_DIR/tracker_0x8dxd

# 一键启动（根目录）
./run_all.sh

# 校验与测试
python ingest/validate_timestamps.py --data-dir $POLY_DATA_DIR
python ingest/test_pipeline.py
```

详见根目录 **`PIPELINE.md`**。
