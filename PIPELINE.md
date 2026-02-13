# 多源录制 Pipeline（0x8dxd 策略数据基础设施）

首次运行：

```bash
pip install -r requirements.txt
```

## 项目架构

当前采集链路按「发现目标 -> 多源采集 -> 分层加工」运行：

```text
discovery / discovery_loop
        -> <data>/targets.json（Compose 下为 /app/data/targets.json，避免单文件挂载 Errno 16）
        -> polymarket recorder (WS)
        -> binance recorder (WS)
        -> chainlink recorder (RPC poll)
        -> tracker_0x8dxd (HTTP poll)
        -> compact (raw -> bronze)
        -> silver (bronze -> silver)
        -> metrics / alert
        -> etl (manual profile)
        -> visual
```

### Compose 服务职责

- `discovery_loop`：周期更新 `<data>/targets.json`（默认 60s 一轮）；写入失败时自动重试，建议使用 data 目录路径。
- `polymarket`：按 `<data>/targets.json` 订阅市场事件并写 raw。
- `binance`：录制 `aggTrade` + `bookTicker`。
- `chainlink`：轮询 Polygon 上预置 Feed。
- `tracker_0x8dxd`：轮询用户地址成交数据。
- `compact`：将 raw JSONL 压实到 bronze parquet。
- `silver`：构建各源 silver 层。
- `metrics`：生成 `metrics.json` 并执行告警检查。
- `etl`：手动 profile 触发，生成 top2 并跑 ETL。
- `visual`：批量绘图任务。

### 网络与代理说明（当前架构）

- `polymarket` / `discovery_loop` / `binance` / `chainlink` / `tracker_0x8dxd` 使用 `host` 网络模式。
- 上述服务默认代理为 `http://127.0.0.1:7897`（`HTTP_PROXY` / `HTTPS_PROXY`）。
- 这样可以避免 bridge 网络下容器到本机代理端口不可达的问题。

## 输出架构

数据根目录默认：`/vault/core/data/poly`，可由 `POLY_DATA_DIR` 覆盖。

```text
${POLY_DATA_DIR}/
  targets.json        # discovery_loop 写入；首次可无，由 discovery 拉取后生成
  raw/
    polymarket/
    binance/
    chainlink/
    tracker_0x8dxd/
  lake/
    bronze/
    silver/
    mart/
  checkpoint/          # 增量进度与缓存（见下方说明）
    compact_*.json
    silver_*.json
    condition_market_index.json
    metrics.json
```

**checkpoint 目录用途**：存放增量处理的进度与缓存，避免重复全量扫描。

- `compact_<source>.json`：raw→bronze 每个文件的 inode/offset/carry，按 offset 只读新增行。
- `silver_<source>.json`：已处理的 bronze parquet 列表，silver 只处理新增文件。
- `condition_market_index.json`：conditionId→market_id 映射缓存（select_top2 / ETL 用）。
- `metrics.json`：当前指标快照，供告警检查用。

若数据目录不可写，可单独把 checkpoint 挂到可写目录，见「启动方式」中的 `POLY_CHECKPOINT_DIR`。

### 各源输出格式

| 源 | 目录 | 格式 | 说明 |
|---|---|---|---|
| Polymarket | `raw/polymarket/` | JSONL | `market_{id}_{YYYYMMDDHH}.jsonl` 按小时轮转；另有 `heartbeat.jsonl` |
| Binance | `raw/binance/` | JSONL | `agg_trades.jsonl` / `book_ticker.jsonl`，单文件超过阈值自动轮转 |
| Chainlink | `raw/chainlink/` | JSONL | `updates.jsonl` |
| Tracker | `raw/tracker_0x8dxd/` | JSONL | `trades.jsonl` |

所有源均写入 `local_receipt_ts_ms` 与 `unixtime`，用于多源对齐。

### Bronze / Silver 增量机制（当前实现）

- `compact`：按 JSONL 文件 `inode + offset + carry` 增量读取，仅处理新增行。
- `compact`：bronze 采用 append-only，输出为 `.../<file_stem>/part-*.parquet`。
- `silver`：按 `checkpoint/silver_<source>.json` 仅处理新增 bronze parquet。
- `silver`：同样 append-only，输出为 `.../<bronze_part_stem>/part-*.parquet`。

### ETL 数据源（当前实现）

- **run_etl.py** 与 **select_top2_markets.py** 优先从 **lake/silver** 读取：Binance（book_ticker / agg_trades）、Polymarket、Chainlink、tracker_0x8dxd（trades）与 conditionId→market_id 映射。
- 当某源 silver 下无 parquet 时，自动回退到 **raw** 对应 JSONL。

## 启动方式

### 方式 A：Docker Compose（推荐）

```bash
# 首次或配置变更后
docker compose up -d --build

# 仅重建关键采集服务（例如代理或网络变更）
docker compose up -d --force-recreate polymarket discovery_loop binance chainlink tracker_0x8dxd

# 重建处理层（当 compact/silver 逻辑更新后）
docker compose up -d --build --force-recreate compact silver
```

### 方式 B：Supervisor（非容器）

```bash
mkdir -p logs
supervisord -c supervisord.conf
supervisorctl status
```

### 方式 C：本机脚本（开发调试）

```bash
./run_all.sh
# 或跳过 discovery
./run_all.sh --skip-discovery
```

## 检查方式

### 1) 容器健康检查

```bash
docker compose ps
```

关键采集容器状态应为 `healthy`：`polymarket`、`binance`、`chainlink`、`tracker_0x8dxd`。

### 2) 关键日志检查

```bash
docker logs --tail 80 poly-binance-1
docker logs --tail 80 poly-chainlink-1
docker logs --tail 80 poly-tracker_0x8dxd-1
docker logs --tail 80 poly-polymarket-1
```

判断标准：

- Binance：出现“已收到首条 aggTrade/bookTicker”。
- Chainlink：出现“Chainlink 更新 ...”。
- Tracker：持续输出“成交 ...”。
- Polymarket：持续输出订阅/写盘与 heartbeat。

### 3) 数据落盘检查

```bash
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/raw/polymarket
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/raw/binance
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/raw/chainlink
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/raw/tracker_0x8dxd
```

### 4) 处理层检查

```bash
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/lake/bronze
ls -lah ${POLY_DATA_DIR:-/vault/core/data/poly}/lake/silver
cat ${POLY_DATA_DIR:-/vault/core/data/poly}/checkpoint/compact_polymarket.json
cat ${POLY_DATA_DIR:-/vault/core/data/poly}/checkpoint/silver_polymarket.json
cat ${POLY_DATA_DIR:-/vault/core/data/poly}/checkpoint/metrics.json
```

## 手动跑通 ETL 到出图

在项目根目录执行，数据与 checkpoint 目录需可写（或设置 `POLY_CHECKPOINT_DIR` 到可写目录）。

```bash
# 1) 环境（按需修改）
export POLY_DATA_DIR=/vault/core/data/poly
export POLY_CHECKPOINT_DIR=$HOME/poly_checkpoint   # 若数据目录下 checkpoint 不可写
mkdir -p "$POLY_CHECKPOINT_DIR"
mkdir -p eda/01_etl/output

# 2) 选 top2 市场并执行 ETL（写入 top2.json + lake/mart 下 master_*.parquet）
python eda/01_etl/scripts/select_top2_markets.py \
  --data-dir "$POLY_DATA_DIR" \
  --hours 24 \
  --min-buys 10 \
  --output eda/01_etl/output/top2.json \
  --run-etl

# 3) 按 top2 出图（PNG 写到 eda/02_visual/output/）
python eda/02_visual/scripts/batch_plot_retry.py \
  --top2 eda/01_etl/output/top2.json \
  --data-dir "$POLY_DATA_DIR"
```

说明：

- 步骤 2 会为每个币种调用 `run_etl.py`，输出在 `<POLY_DATA_DIR>/lake/mart/aligned_master/symbol=<coin>usdt/...`。
- 步骤 3 会读该 mart 下的 parquet 与 top2.json，按市场出图；未传 `--data-dir` 时会用环境变量 `POLY_DATA_DIR`，并会从 `data_dir/lake/mart/aligned_master` 找 parquet。

## 用 Docker 手动跑一次 ETL（推荐，避免本机写数据目录权限问题）

`etl` 服务在 `manual` profile 下，需显式触发；容器内以 root 写 `/app/data`，可正常写入 `lake/mart`。

```bash
# 在项目根目录

# 1) 只跑 ETL（选 top2 + 写 master_*.parquet 到 data/lake/mart）
docker compose --profile manual run --rm etl

# 2) 再跑一次出图（读上一步生成的 top2.json 与 mart parquet，写 PNG 到 eda/02_visual/output）
docker compose run --rm visual python -u eda/02_visual/scripts/batch_plot_retry.py \
  --top2 /app/eda/01_etl/output/top2.json \
  --data-dir /app/data
```

- 步骤 1 会写 `eda/01_etl/output/top2.json`（在项目目录）和 `<POLY_DATA_DIR>/lake/mart/...`（在挂载的数据目录）。
- 步骤 2 用同一数据目录下的 mart 与 top2 出图，PNG 落在项目里的 `eda/02_visual/output/`。
- 若需改时间窗口或最小买单数，可覆盖默认参数，例如：  
  `docker compose --profile manual run --rm etl python -u eda/01_etl/scripts/select_top2_markets.py --data-dir /app/data --hours 48 --min-buys 5 --output /app/eda/01_etl/output/top2.json --run-etl`

## 常用环境变量

- `POLY_DATA_DIR`：数据根目录，默认 `/vault/core/data/poly`。
- `POLY_CHECKPOINT_DIR`：checkpoint 目录（增量进度与缓存）。未设置时用 `<POLY_DATA_DIR>/checkpoint`。**若数据目录不可写**，可设为可写路径，例如：
  - 本机跑脚本：`export POLY_CHECKPOINT_DIR=$HOME/poly_checkpoint`
  - Docker Compose：在宿主机设置 `POLY_CHECKPOINT_DIR` 为可写目录（如 `$HOME/poly_checkpoint`），compose 会将其挂载到容器内 `/app/data/checkpoint`，compact / silver / metrics / etl 的 checkpoint 写入均落在此目录。
- `HTTP_PROXY` / `HTTPS_PROXY`：外网代理。
- `POLYGON_RPC_URL`：Chainlink 使用的 Polygon RPC 地址。
- `TRACKER_USER_ADDRESS`：Tracker 监控地址（默认 `0x8dxd`）。
- `ETL_MIN_BUYS`：ETL 最小买单过滤阈值。
