#!/usr/bin/env python3
"""
回放工具：指定时间窗重建 mart 数据（按 symbol）。
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay ETL for a given window")
    parser.add_argument("--symbol", required=True, help="如 btcusdt")
    parser.add_argument("--start-ts-ms", type=int, required=True)
    parser.add_argument("--end-ts-ms", type=int, required=True)
    parser.add_argument("--data-dir", default="")
    parser.add_argument("--market-ids", default="")
    parser.add_argument("--output-filename", default="")
    args = parser.parse_args()

    run_etl = Path(__file__).resolve().parent / "run_etl.py"
    out = args.output_filename or f"master_{args.symbol}.parquet"
    cmd = [
        sys.executable,
        str(run_etl),
        "--symbol",
        args.symbol,
        "--start-ts-ms",
        str(args.start_ts_ms),
        "--end-ts-ms",
        str(args.end_ts_ms),
        "--output-filename",
        out,
    ]
    if args.data_dir:
        cmd += ["--data-dir", args.data_dir]
    if args.market_ids:
        cmd += ["--market-ids", args.market_ids]
    print(" ".join(cmd))
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
