#!/usr/bin/env python3
"""
通用健康检查：
- 目录中最近更新文件是否在阈值内
- 可选要求文件最小字节数
"""

import argparse
import sys
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple file freshness healthcheck")
    parser.add_argument("--dir", required=True, help="待检查目录")
    parser.add_argument("--glob", default="*.jsonl", help="匹配文件模式")
    parser.add_argument("--max-age-sec", type=int, default=300, help="最近更新时间阈值")
    parser.add_argument("--min-size-bytes", type=int, default=1, help="最小文件大小")
    args = parser.parse_args()

    target_dir = Path(args.dir)
    if not target_dir.exists() or not target_dir.is_dir():
        print(f"unhealthy: invalid dir {target_dir}")
        return 1

    files = [p for p in target_dir.glob(args.glob) if p.is_file()]
    if not files:
        print("unhealthy: no matching files")
        return 1

    newest = max(files, key=lambda p: p.stat().st_mtime)
    stat = newest.stat()
    age = int(time.time() - stat.st_mtime)
    if age > args.max_age_sec:
        print(f"unhealthy: stale file {newest.name}, age={age}s")
        return 1
    if stat.st_size < args.min_size_bytes:
        print(f"unhealthy: file too small {newest.name}, size={stat.st_size}")
        return 1

    print(f"healthy: {newest.name} age={age}s size={stat.st_size}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
