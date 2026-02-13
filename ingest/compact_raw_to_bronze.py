#!/usr/bin/env python3
"""
raw JSONL -> bronze Parquet 增量 compact。
- 按 source 递归扫描 raw/<source>/dt=.../hour=.../*.jsonl
- checkpoint 记录每个文件的 inode/offset/carry，按 offset 增量读取
- bronze 采用 append-only：每次写入新的 parquet part 文件
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")


def _file_state(path: Path) -> Dict[str, int]:
    st = path.stat()
    return {"mtime_ns": st.st_mtime_ns, "size": st.st_size, "inode": st.st_ino}


def _load_checkpoint(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_checkpoint(path: Path, checkpoint: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _event_time_ms(source: str, rec: dict):
    if source == "binance":
        for key in ("E", "T"):
            if rec.get(key) is not None:
                try:
                    return int(rec[key])
                except Exception:
                    pass
    if source == "chainlink":
        if rec.get("updatedAt") is not None:
            try:
                return int(rec["updatedAt"]) * 1000
            except Exception:
                pass
    if source == "tracker_0x8dxd":
        if rec.get("timestamp") is not None:
            try:
                return int(rec["timestamp"]) * 1000
            except Exception:
                pass
    return None


def _read_new_records(path: Path, entry: dict) -> Tuple[List[dict], dict]:
    state = _file_state(path)
    old_inode = int(entry.get("inode", 0) or 0)
    old_offset = int(entry.get("offset", 0) or 0)
    carry = str(entry.get("carry", "") or "")
    part_idx = int(entry.get("part_idx", 0) or 0)

    # 文件轮转或截断时重置增量游标
    if old_inode != state["inode"] or old_offset > state["size"]:
        old_offset = 0
        carry = ""

    if old_offset == state["size"] and not carry:
        entry.update(state)
        entry["offset"] = state["size"]
        return [], entry

    with open(path, "rb") as f:
        f.seek(old_offset)
        payload = f.read()
        new_offset = f.tell()

    text = carry + payload.decode("utf-8", errors="ignore")
    rows: List[dict] = []
    tail = ""
    if text:
        lines = text.splitlines(keepends=True)
        for i, raw in enumerate(lines):
            is_last = i == len(lines) - 1
            if is_last and not raw.endswith("\n"):
                tail = raw
                continue
            line = raw.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    entry.update(state)
    entry["offset"] = new_offset
    entry["carry"] = tail
    entry["part_idx"] = part_idx
    return rows, entry


def _checkpoint_root(data_dir: Path) -> Path:
    v = os.environ.get("POLY_CHECKPOINT_DIR", "").strip()
    return Path(v).resolve() if v else data_dir / "checkpoint"


def compact_source(data_dir: Path, source: str) -> int:
    raw_root = data_dir / "raw" / source
    bronze_root = data_dir / "lake" / "bronze" / source
    checkpoint_path = _checkpoint_root(data_dir) / f"compact_{source}.json"
    checkpoint = _load_checkpoint(checkpoint_path)
    processed = 0

    if not raw_root.exists():
        return 0

    for jsonl in sorted(raw_root.rglob("*.jsonl")):
        rel = jsonl.relative_to(raw_root)
        key = str(rel)
        entry = checkpoint.get(key, {})
        if not isinstance(entry, dict):
            entry = {}

        incoming, entry = _read_new_records(jsonl, entry)

        rows: List[dict] = []
        for rec in incoming:
            ingest_ms = rec.get("local_receipt_ts_ms")
            try:
                ingest_ms = int(ingest_ms) if ingest_ms is not None else None
            except Exception:
                ingest_ms = None
            event_ms = _event_time_ms(source, rec)
            out = dict(rec)
            if source == "chainlink":
                # uint80 可能超过 pyarrow int64，落 bronze 时转字符串避免溢出
                for k in ("roundId", "answeredInRound"):
                    if out.get(k) is not None:
                        out[k] = str(out[k])
            if isinstance(out.get("raw_data"), (dict, list)):
                out["raw_data"] = json.dumps(out["raw_data"], ensure_ascii=False)
            out["source"] = source
            out["ingest_time_ms"] = ingest_ms
            out["event_time_ms"] = event_ms
            rows.append(out)

        if rows:
            df = pd.DataFrame(rows)
            part_idx = int(entry.get("part_idx", 0) or 0) + 1
            part_dir = bronze_root / rel.parent / rel.stem
            out_name = f"part-{int(time.time() * 1000)}-{part_idx:06d}.parquet"
            out_path = part_dir / out_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, index=False)
            entry["part_idx"] = part_idx
            processed += 1

        checkpoint[key] = entry

    _save_checkpoint(checkpoint_path, checkpoint)
    return processed


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental compact raw JSONL to bronze parquet")
    parser.add_argument("--data-dir", default=POLY_DATA_DIR)
    parser.add_argument("--sources", default="polymarket,binance,chainlink,tracker_0x8dxd")
    parser.add_argument("--interval-sec", type=int, default=0, help="0=run once; >0 loop mode")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    if args.interval_sec <= 0:
        total = 0
        for s in sources:
            n = compact_source(data_dir, s)
            print(f"compact {s}: {n} files")
            total += n
        return 0

    print(f"compact loop start, interval={args.interval_sec}s")
    while True:
        for s in sources:
            n = compact_source(data_dir, s)
            print(f"[{int(time.time())}] compact {s}: {n} files")
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
