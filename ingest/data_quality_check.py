#!/usr/bin/env python3
"""
数据质量校验：
- schema drift
- 空值率
- 时间倒退
- 重复率
"""

import argparse
import json
from pathlib import Path

import pandas as pd


def _load_schema(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_schema(path: Path, schema: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Data quality check for parquet files")
    parser.add_argument("--input-glob", required=True, help="例如 /data/lake/bronze/binance/**/*.parquet")
    parser.add_argument("--schema-file", required=True, help="schema 基线文件")
    parser.add_argument("--max-null-rate", type=float, default=0.5)
    args = parser.parse_args()

    files = sorted(Path("/").glob(args.input_glob.lstrip("/"))) if args.input_glob.startswith("/") else sorted(Path(".").glob(args.input_glob))
    if not files:
        print("no files")
        return 1

    df = pd.concat([pd.read_parquet(p) for p in files[-20:]], ignore_index=True)
    if df.empty:
        print("empty dataframe")
        return 1

    schema_now = {c: str(df[c].dtype) for c in df.columns}
    schema_file = Path(args.schema_file)
    schema_old = _load_schema(schema_file)
    if schema_old is None:
        _save_schema(schema_file, schema_now)
        print("schema baseline created")
        return 0

    added = sorted(set(schema_now) - set(schema_old))
    removed = sorted(set(schema_old) - set(schema_now))
    changed = sorted([c for c in schema_now if c in schema_old and schema_now[c] != schema_old[c]])
    null_rate = df.isna().mean().sort_values(ascending=False)
    high_null = [c for c, v in null_rate.items() if v > args.max_null_rate]

    ts_col = "ingest_time_ms" if "ingest_time_ms" in df.columns else ("local_receipt_ts_ms" if "local_receipt_ts_ms" in df.columns else None)
    rollback = 0
    dup_rate = 0.0
    if ts_col:
        ts = pd.to_numeric(df[ts_col], errors="coerce").dropna()
        if not ts.empty:
            rollback = int((ts.diff() < 0).sum())
            dup_rate = 1.0 - (ts.nunique() / len(ts))

    report = {
        "rows": int(len(df)),
        "schema_added": added,
        "schema_removed": removed,
        "schema_changed": changed,
        "high_null_columns": high_null,
        "timestamp_rollback_count": rollback,
        "duplicate_rate": dup_rate,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))

    failed = bool(added or removed or changed or high_null or rollback > 0 or dup_rate > 0.2)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
