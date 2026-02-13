#!/usr/bin/env python3
"""
告警规则检查：
- Discovery 停更
- Recorder 任务突降
- Compact 积压
"""

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Check pipeline alerts")
    parser.add_argument("--metrics", default="data/checkpoint/metrics.json")
    parser.add_argument("--max-targets-age", type=int, default=1800)
    parser.add_argument("--min-running-tasks", type=int, default=1)
    parser.add_argument("--max-compact-lag", type=int, default=1800)
    args = parser.parse_args()

    path = Path(args.metrics)
    if not path.exists():
        print("ALERT: metrics file missing")
        return 1
    m = json.loads(path.read_text(encoding="utf-8"))

    alerts = []
    if m.get("targets_age_sec") is not None and m["targets_age_sec"] > args.max_targets_age:
        alerts.append(f"Discovery stale: {m['targets_age_sec']}s")
    if m.get("running_tasks") is not None and m["running_tasks"] < args.min_running_tasks:
        alerts.append(f"Recorder tasks low: {m['running_tasks']}")
    if m.get("compact_lag_sec") is not None and m["compact_lag_sec"] > args.max_compact_lag:
        alerts.append(f"Compact lag high: {m['compact_lag_sec']}s")

    if alerts:
        for a in alerts:
            print("ALERT:", a)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
