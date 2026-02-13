#!/usr/bin/env python3
"""
bronze -> silver：统一 schema 与类型（最小可用版本）。
"""

import argparse
import json
import os
from pathlib import Path

import pandas as pd

POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")


def normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    out = df.copy()
    if "source" not in out.columns:
        out["source"] = source
    if "unixtime" not in out.columns and "ingest_time_ms" in out.columns:
        out["unixtime"] = (pd.to_numeric(out["ingest_time_ms"], errors="coerce") // 1000).astype("Int64")
    for col in ("event_time_ms", "ingest_time_ms"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    if "event_type" not in out.columns:
        out["event_type"] = None
    if source == "binance" and "s" in out.columns:
        out["instrument_id"] = out["s"].astype(str).str.lower()
    elif source == "polymarket" and "market_id" in out.columns:
        out["instrument_id"] = out["market_id"].astype(str)
    elif source == "chainlink" and "feed" in out.columns:
        out["instrument_id"] = out["feed"].astype(str)
    elif source == "tracker_0x8dxd" and "conditionId" in out.columns:
        out["instrument_id"] = out["conditionId"].astype(str)
    return out


def _file_state(path: Path) -> dict:
    st = path.stat()
    return {"mtime_ns": st.st_mtime_ns, "size": st.st_size}


def _load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build silver parquet from bronze parquet")
    parser.add_argument("--data-dir", default=POLY_DATA_DIR)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    bronze_root = data_dir / "lake" / "bronze" / args.source
    silver_root = data_dir / "lake" / "silver" / args.source
    checkpoint_root = (Path(os.environ.get("POLY_CHECKPOINT_DIR", "")).resolve() if os.environ.get("POLY_CHECKPOINT_DIR") else data_dir / "checkpoint")
    checkpoint_path = checkpoint_root / f"silver_{args.source}.json"
    checkpoint = _load_checkpoint(checkpoint_path)
    processed = 0
    if not bronze_root.exists():
        return 1
    for p in sorted(bronze_root.rglob("*.parquet")):
        if p.name.startswith("."):
            continue
        rel = p.relative_to(bronze_root)
        key = str(rel)
        state = _file_state(p)
        if checkpoint.get(key) == state:
            continue

        # silver 追加写：沿用 bronze 分层，并新增独立 silver part
        out_dir = silver_root / rel.parent / rel.stem
        out_name = f"part-{int(pd.Timestamp.now('UTC').timestamp() * 1000)}.parquet"
        out = out_dir / out_name
        out.parent.mkdir(parents=True, exist_ok=True)
        df = pd.read_parquet(p)
        normalize(df, args.source).to_parquet(out, index=False)
        checkpoint[key] = state
        processed += 1
    _save_checkpoint(checkpoint_path, checkpoint)
    print(f"silver {args.source}: {processed} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
