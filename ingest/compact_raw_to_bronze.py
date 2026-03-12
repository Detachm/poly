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
from datetime import datetime, timedelta, timezone
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


def _load_retention_filter(path: Path) -> dict:
    """加载 retention_filter.json，返回过滤配置。文件不存在则返回空 dict（不过滤）。"""
    if not path.exists():
        return {}
    try:
        data = json.load(open(path, "r", encoding="utf-8"))
        return {
            "kept_market_ids": set(data.get("kept_market_ids", [])),
            "keep_hours": set(tuple(x) for x in data.get("keep_hours_binance_chainlink", [])),
        }
    except Exception:
        return {}


def _should_process_file(source: str, rel: Path, filt: dict) -> bool:
    """按源判断是否应处理此文件。filt 为空时全量处理（向后兼容）。"""
    if not filt:
        return True

    if source == "polymarket":
        stem = rel.name.replace(".jsonl", "")
        if stem.startswith("market_"):
            mid = stem.replace("market_", "").split("_")[0]
            if mid.isdigit():
                return mid in filt.get("kept_market_ids", set())
        return True  # heartbeat.jsonl 等非 market 文件始终处理

    if source in ("binance", "chainlink"):
        dt, hour = None, None
        for part in rel.parts:
            if part.startswith("dt="):
                dt = part[3:]
            if part.startswith("hour="):
                hour = part[5:]
        if dt and hour is not None:
            # 最近 48 小时内的数据始终处理，filter 只过滤更老的历史数据
            try:
                partition_time = datetime(int(dt[:4]), int(dt[5:7]), int(dt[8:10]),
                                         int(hour), tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - partition_time).total_seconds() < 48 * 3600:
                    return True
            except Exception:
                pass
            return (dt, hour) in filt.get("keep_hours", set())
        return True  # 非分区文件始终处理

    # tracker_0x8dxd, chain_0x8dxd: 不过滤，始终处理
    return True


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
    if source == "chain_0x8dxd":
        if rec.get("unixtime") is not None:
            try:
                return int(rec["unixtime"]) * 1000
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


def _recent_partitions(raw_root: Path, lookback_hours: int) -> List[Path]:
    """只返回最近 lookback_hours 小时内的分区目录。lookback_hours<=0 时返回 raw_root 本身（全量扫描）。"""
    if lookback_hours <= 0:
        return [raw_root]
    now = datetime.now(timezone.utc)
    dirs = []
    for h in range(lookback_hours + 1):
        t = now - timedelta(hours=h)
        p = raw_root / f"dt={t.strftime('%Y-%m-%d')}" / f"hour={t.hour:02d}"
        if p.is_dir():
            dirs.append(p)
    # 也包含 raw_root 下不在 dt= 分区内的文件（如 heartbeat.jsonl）
    for f in raw_root.iterdir():
        if f.is_file() and f.suffix == ".jsonl":
            dirs.append(f)
    return dirs


def compact_source(data_dir: Path, source: str, retention_filter: dict = None, lookback_hours: int = 0) -> int:
    raw_root = data_dir / "raw" / source
    bronze_root = data_dir / "lake" / "bronze" / source
    checkpoint_path = _checkpoint_root(data_dir) / f"compact_{source}.json"
    checkpoint = _load_checkpoint(checkpoint_path)
    processed = 0

    if not raw_root.exists():
        return 0

    # 收集待扫描的 jsonl 文件
    scan_roots = _recent_partitions(raw_root, lookback_hours)
    jsonl_files = []
    for root in scan_roots:
        if root.is_file():
            jsonl_files.append(root)
        else:
            jsonl_files.extend(root.rglob("*.jsonl"))
    jsonl_files.sort()

    for jsonl in jsonl_files:
        rel = jsonl.relative_to(raw_root)
        key = str(rel)

        # 过滤：跳过不在 retention filter 中的文件（不更新 checkpoint）
        if not _should_process_file(source, rel, retention_filter):
            continue

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
    parser.add_argument("--sources", default="polymarket,binance,chainlink,tracker_0x8dxd,chain_0x8dxd")
    parser.add_argument("--interval-sec", type=int, default=0, help="0=run once; >0 loop mode")
    parser.add_argument("--lookback-hours", type=int, default=6, help="only scan recent N hours of partitions; 0=full scan")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    filter_path = _checkpoint_root(data_dir) / "retention_filter.json"

    if args.interval_sec <= 0:
        retention_filter = _load_retention_filter(filter_path)
        if retention_filter:
            print(f"retention filter: {len(retention_filter.get('kept_market_ids', set()))} markets, {len(retention_filter.get('keep_hours', set()))} hours")
        total = 0
        for s in sources:
            n = compact_source(data_dir, s, retention_filter, args.lookback_hours)
            print(f"compact {s}: {n} files")
            total += n
        return 0

    print(f"compact loop start, interval={args.interval_sec}s, lookback={args.lookback_hours}h")
    while True:
        retention_filter = _load_retention_filter(filter_path)
        if retention_filter:
            print(f"[{int(time.time())}] retention filter: {len(retention_filter.get('kept_market_ids', set()))} markets, {len(retention_filter.get('keep_hours', set()))} hours")
        for s in sources:
            n = compact_source(data_dir, s, retention_filter, args.lookback_hours)
            print(f"[{int(time.time())}] compact {s}: {n} files")
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
