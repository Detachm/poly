#!/usr/bin/env python3
"""
可视化批量出图（带失败重试）。
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch plot with retry")
    parser.add_argument("--top2", default="eda/01_etl/output/top2.json")
    parser.add_argument("--data-dir", default="", help="数据根目录，与 ETL 一致，用于读 parquet 与 trades")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-wait-sec", type=int, default=5)
    args = parser.parse_args()

    script = Path(__file__).resolve().parent / "plot_window.py"
    cmd = [sys.executable, str(script), "--top2", args.top2]
    if args.data_dir:
        cmd.extend(["--data-dir", args.data_dir])
    for i in range(1, args.retries + 1):
        ret = subprocess.call(cmd)
        if ret == 0:
            print("plot success")
            return 0
        print(f"plot failed (attempt {i}/{args.retries}), retrying...")
        time.sleep(args.retry_wait_sec)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
