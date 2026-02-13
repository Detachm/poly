# poly

多源数据采集与处理流水线（Polymarket / Binance / Chainlink / 0x8dxd Tracker）。

## 项目架构

```text
discovery -> targets.json
         -> raw ingest (polymarket / binance / chainlink / tracker)
         -> compact (offset 增量读取 raw -> bronze append parquet)
         -> silver (checkpoint 增量读取 bronze -> silver append parquet)
         -> metrics / alert
         -> etl (manual)
         -> visual
```

## 目录结构

- `ingest/`：数据发现、采集、健康检查、加工与指标脚本。
- `eda/`：ETL、可视化与分析脚本。
- `docs/`：任务记录与工程文档。
- `docker-compose.yml`：完整容器化流水线编排。
- `targets.json`：Polymarket 目标市场列表（由 discovery 生成/更新）。

## 快速启动

推荐 Docker Compose：

```bash
docker compose up -d --build
docker compose ps
```

如需仅重建关键采集服务（代理/网络变更后常用）：

```bash
docker compose up -d --force-recreate polymarket discovery_loop binance chainlink tracker_0x8dxd
```

如需重建处理层（compact/silver 逻辑升级后）：

```bash
docker compose up -d --build --force-recreate compact silver
```

## 快速检查

```bash
docker compose ps
docker logs --tail 80 poly-binance-1
docker logs --tail 80 poly-chainlink-1
docker logs --tail 80 poly-tracker_0x8dxd-1
docker logs --tail 80 poly-polymarket-1
```

数据根目录默认是 `/vault/core/data/poly`，可通过 `POLY_DATA_DIR` 覆盖。

## 详细文档

- `PIPELINE.md`：采集架构、输出架构、启动方式、检查方式（主文档）。
- `eda/01_etl/README.md`：ETL 细节。
