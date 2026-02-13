#!/usr/bin/env python3
"""
采集链路关键指标：
- targets_age
- running_tasks (近 10 分钟内有写入的 polymarket 市场数)
- compact_lag
- etl_latency (mart 最近文件新鲜度)
"""

import argparse
import json
import time
from pathlib import Path


def _latest_mtime(files):
    if not files:
        return None
    return max(p.stat().st_mtime for p in files)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect pipeline metrics")
    parser.add_argument("--data-dir", default="/vault/core/data/poly")
    parser.add_argument("--targets", default="targets.json")
    parser.add_argument("--output", default="data/checkpoint/metrics.json")
    args = parser.parse_args()

    now = time.time()
    data_dir = Path(args.data_dir)
    targets = Path(args.targets)

    targets_age = None
    if targets.exists():
        targets_age = int(now - targets.stat().st_mtime)

    pm_files = sorted((data_dir / "raw" / "polymarket").rglob("market_*.jsonl"))
    running_tasks = 0
    for p in pm_files:
        if now - p.stat().st_mtime <= 600:
            running_tasks += 1

    bronze_files = sorted((data_dir / "lake" / "bronze").rglob("*.parquet"))
    compact_lag = int(now - _latest_mtime(bronze_files)) if bronze_files else None

    mart_files = sorted((data_dir / "lake" / "mart").rglob("*.parquet"))
    etl_latency = int(now - _latest_mtime(mart_files)) if mart_files else None

    metrics = {
        "collected_at": int(now),
        "targets_age_sec": targets_age,
        "running_tasks": running_tasks,
        "compact_lag_sec": compact_lag,
        "etl_latency_sec": etl_latency,
    }

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
