#!/usr/bin/env python3
"""
实验数据集版本固化：按窗口和参数复制 mart 输出到版本目录。
"""

import argparse
import json
import shutil
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze mart dataset snapshot")
    parser.add_argument("--input-parquet", required=True)
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--meta-json", default="{}")
    parser.add_argument("--output-root", default="data/experiments")
    args = parser.parse_args()

    src = Path(args.input_parquet)
    if not src.exists():
        return 1
    dst_root = Path(args.output_root) / args.version_id
    dst_root.mkdir(parents=True, exist_ok=True)
    dst_parquet = dst_root / src.name
    shutil.copy2(src, dst_parquet)
    meta = json.loads(args.meta_json)
    (dst_root / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"frozen dataset: {dst_parquet}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
