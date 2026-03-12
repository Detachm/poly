# 数据保留（Retention）

按「Polymarket 录制 + 0x8dxd 有动作」的市场保留数据，其余删除；Binance/Chainlink 仅保留与保留市场 (币种, 时间段) 并集重叠的小时分区。

## 步骤

### 1. 审计（只读）

从**项目根**运行：

```bash
python eda/retention/audit.py --data-dir /vault/core/data/poly -o eda/retention/audit_result.json
```

- 扫描 Polymarket raw/silver 得到「有录制的市场」及时间范围
- 扫描 0x8dxd trades 得到有成交的 conditionId，并映射到 market_id
- **保留市场** = 有录制 且 0x8dxd 有动作（且能映射到 market_id）
- 从保留市场中 **crypto** 市场得到 (币种, 时间范围) 并集，再离散化为应保留的 (dt, hour) 列表
- 输出 `audit_result.json`：`kept_market_ids`、`kept_condition_ids`、`union_symbol_time_ranges`、`keep_hours_binance_chainlink`

### 2. 执行保留（删除/重写）

先**干跑**确认：

```bash
python eda/retention/apply_retention.py --audit eda/retention/audit_result.json --dry-run
```

确认无误后执行（二选一）：

- **只删 silver、保留 raw/bronze**（原数据不删，下游 ETL 只读 silver 故等效保留集；不改 checkpoint，后续 build_silver 不会重算已删的 silver）：
  ```bash
  python eda/retention/apply_retention.py --audit eda/retention/audit_result.json --silver-only
  ```
- **全量删除**（raw/bronze/silver 都删并更新 checkpoint）：
  ```bash
  python eda/retention/apply_retention.py --audit eda/retention/audit_result.json
  ```

- `--phase all`（默认）：polymarket → tracker → binance → chainlink
- `--phase polymarket`：只删非保留市场的 plmkt raw/bronze/silver 并更新 checkpoint
- `--phase tracker`：重写 raw trades.jsonl（只保留 kept_condition_ids），清空 tracker bronze/silver 与 checkpoint，需**手动重跑** compact + build_silver 仅 tracker
- `--phase binance` / `--phase chainlink`：删除不在 `keep_hours_binance_chainlink` 内的 (dt, hour) 分区并更新 checkpoint

### 无 sudo 时：在容器内跑 retention（推荐）

若数据是 Docker 容器写的，宿主机用户没有权限删 silver，可**在容器内**执行保留脚本（与 compact/silver 同权限）：

```bash
# 先确保已生成 audit_result.json（宿主机或容器内跑过 audit）
docker compose --profile manual run --rm retention
```

默认执行 `--silver-only`。若要全量删除或指定 phase，可覆盖 command：

```bash
docker compose --profile manual run --rm retention python eda/retention/apply_retention.py --audit eda/retention/audit_result.json --data-dir /app/data
```

### 3. Tracker 重跑（phase tracker 后必做）

```bash
# 仅 compact tracker
python ingest/compact_raw_to_bronze.py --data-dir /vault/core/data/poly --sources tracker_0x8dxd

# 仅 build_silver tracker
python ingest/build_silver.py --data-dir /vault/core/data/poly --source tracker_0x8dxd
```

## 约定

- 时间用 **A**：每个保留市场用「该市场我们录制的完整时间范围」参与并集
- 非 crypto 保留市场：只保留 plmkt + 0x8d，不向 Binance/Chainlink 的 (币种, 时间段) 并集贡献
- Polymarket：只保留「保留市场」的数据，其余市场 raw/bronze/silver 删除
- 有 plmkt+0x8d 无 binance/chainlink 的时段：不改动
- 无 plmkt+0x8d 却有 binance/chainlink 的 (dt, hour)：删除
