#!/usr/bin/env python3
"""
小文件合并：按天合并 parquet（daily compaction）。
"""

import argparse
from pathlib import Path

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge small parquet files by day partition")
    parser.add_argument("--input-root", required=True, help="如 /data/lake/bronze/binance")
    parser.add_argument("--min-files", type=int, default=5)
    parser.add_argument("--output-name", default="merged.parquet")
    args = parser.parse_args()

    root = Path(args.input_root)
    if not root.exists():
        return 1

    daily_dirs = sorted([p for p in root.rglob("dt=*") if p.is_dir()])
    merged_count = 0
    for d in daily_dirs:
        files = sorted(d.rglob("*.parquet"))
        if len(files) < args.min_files:
            continue
        try:
            df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
            out = d / args.output_name
            df.to_parquet(out, index=False)
            merged_count += 1
        except Exception as e:
            print(f"skip {d}: {e}")
    print(f"merged daily partitions: {merged_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
