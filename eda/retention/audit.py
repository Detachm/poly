#!/usr/bin/env python3
"""
数据保留审计（只读）：遍历 Polymarket + 0x8dxd，得到「保留市场」与 (币种, 时间段) 并集。
输出 audit_result.json，供 apply_retention.py 使用。
从项目根运行: python eda/retention/audit.py --data-dir /path
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

# 同目录 lib，支持直接运行脚本
_DIR = Path(__file__).resolve().parent
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))
import lib


def _polymarket_markets_and_ranges(data_dir: Path) -> dict[str, tuple[int, int]]:
    """扫描 raw 或 silver polymarket，得到 market_id -> (start_ms, end_ms)。用路径 dt/hour 推断时间。"""
    out: dict[str, tuple[int, int]] = {}
    # 先扫 raw（支持 raw/polymarket 或 legacy data_dir/polymarket）
    raw_root = data_dir / "raw" / "polymarket"
    if not raw_root.exists():
        raw_root = data_dir / "polymarket"
    if raw_root.exists():
        for path in sorted(raw_root.rglob("market_*.jsonl")):
            mid = None
            if path.stem.startswith("market_"):
                parts = path.stem.replace("market_", "").split("_")
                mid = parts[0] if parts else None
            if not mid or not mid.isdigit():
                continue
            part = lib.partition_from_path(path)
            if part:
                dt, hour = part
                h_start = lib.partition_hour_to_start_ms(dt, hour)
                if h_start is not None:
                    h_end = h_start + 3600 * 1000
                    if mid not in out:
                        out[mid] = (h_start, h_end)
                    else:
                        s, e = out[mid]
                        out[mid] = (min(s, h_start), max(e, h_end))
            else:
                # 根下 market_123.jsonl，用 mtime 近似
                mtime = path.stat().st_mtime
                ms = int(mtime * 1000)
                if mid not in out:
                    out[mid] = (ms, ms)
                else:
                    s, e = out[mid]
                    out[mid] = (min(s, ms), max(e, ms))
    # 再扫 silver 补全（可能 raw 已删）
    silver_root = data_dir / "lake" / "silver" / "polymarket"
    if silver_root.exists():
        for p in silver_root.rglob("*.parquet"):
            # .../dt=.../hour=.../market_123/part-*.parquet
            rel = p.relative_to(silver_root)
            parts = rel.parts
            mid = None
            dt, hour = None, None
            for x in parts:
                if x.startswith("market_"):
                    mid = x.replace("market_", "").strip()
                if x.startswith("dt="):
                    dt = x[3:]
                if x.startswith("hour="):
                    hour = x[5:]
            if not mid or not mid.isdigit() or not dt or hour is None:
                continue
            h_start = lib.partition_hour_to_start_ms(dt, hour)
            if h_start is None:
                continue
            h_end = h_start + 3600 * 1000
            if mid not in out:
                out[mid] = (h_start, h_end)
            else:
                s, e = out[mid]
                out[mid] = (min(s, h_start), max(e, h_end))
    return out


def _tracker_condition_ids_with_trades(data_dir: Path) -> set[str]:
    """所有在 trades 里出现过的 conditionId。"""
    cids = set()
    raw_root = data_dir / "raw" / "tracker_0x8dxd"
    if not raw_root.exists():
        raw_root = data_dir / "tracker_0x8dxd"
    for path in sorted(raw_root.rglob("trades.jsonl")) if raw_root.exists() else []:
        for rec in lib._iter_jsonl(path):
            cid = rec.get("conditionId") or rec.get("condition_id")
            if cid:
                cids.add(str(cid).strip())
    # silver
    silver_root = data_dir / "lake" / "silver" / "tracker_0x8dxd"
    if silver_root.exists():
        try:
            import pandas as pd
            for p in silver_root.rglob("*.parquet"):
                df = pd.read_parquet(p)
                if "conditionId" in df.columns:
                    for v in df["conditionId"].dropna().astype(str):
                        cids.add(v.strip())
                if "condition_id" in df.columns:
                    for v in df["condition_id"].dropna().astype(str):
                        cids.add(v.strip())
        except ImportError:
            pass
    return cids


def _tracker_trade_entries_by_cid(data_dir: Path) -> dict[str, dict]:
    """每个 conditionId 取一条 trade 记录（含 title/slug）用于 coin_from_market。"""
    out: dict[str, dict] = {}
    raw_root = data_dir / "raw" / "tracker_0x8dxd"
    if not raw_root.exists():
        raw_root = data_dir / "tracker_0x8dxd"
    for path in sorted(raw_root.rglob("trades.jsonl")) if raw_root.exists() else []:
        for rec in lib._iter_jsonl(path):
            cid = rec.get("conditionId") or rec.get("condition_id")
            if not cid or cid in out:
                continue
            cid = str(cid).strip()
            out[cid] = {"title": rec.get("title"), "slug": rec.get("slug")}
    return out


def _union_symbol_time_ranges(
    kept_market_ids: set[str],
    pm_ranges: dict[str, tuple[int, int]],
    cid_to_mid: dict[str, str],
    trade_entry_by_cid: dict[str, dict],
) -> list[dict]:
    """从 crypto 保留市场得到 (coin, start_ms, end_ms) 列表。"""
    ranges: list[dict] = []
    for mid in kept_market_ids:
        if mid not in pm_ranges:
            continue
        start_ms, end_ms = pm_ranges[mid]
        # 找该 market 对应的任一 cid 的 entry 以取 title/slug
        entry = {}
        for cid, m in cid_to_mid.items():
            if m == mid and cid in trade_entry_by_cid:
                entry = trade_entry_by_cid[cid]
                break
        coin = lib.coin_from_market(entry)
        if not coin:
            continue
        ranges.append({"coin": coin, "start_ms": start_ms, "end_ms": end_ms})
    return ranges


def _hours_overlapping_ranges(ranges: list[dict]) -> set[tuple[str, str]]:
    """给定 (coin, start_ms, end_ms) 列表，返回应保留的 (dt, hour) 集合（UTC）。"""
    keep: set[tuple[str, str]] = set()
    for r in ranges:
        start_ms = r["start_ms"]
        end_ms = r["end_ms"]
        start_sec = start_ms // 1000
        end_sec = end_ms // 1000
        # 覆盖该区间内每一小时的 (dt, hour)
        t = start_sec
        while t <= end_sec + 3600:
            dt = datetime.fromtimestamp(t, tz=timezone.utc)
            dt_str = dt.strftime("%Y-%m-%d")
            hour_str = dt.strftime("%H")
            keep.add((dt_str, hour_str))
            t += 3600
    return keep


def run_audit(data_dir: Path, output_path: Path | None) -> dict:
    data_dir = data_dir.resolve()
    pm_ranges = _polymarket_markets_and_ranges(data_dir)
    tracker_cids = _tracker_condition_ids_with_trades(data_dir)
    trade_entry_by_cid = _tracker_trade_entries_by_cid(data_dir)

    # conditionId -> market_id
    silver_pm = data_dir / "lake" / "silver" / "polymarket"
    if silver_pm.exists() and list(silver_pm.rglob("*.parquet")):
        cid_to_mid = lib.build_condition_id_to_market_id_from_silver(data_dir)
    else:
        pm_raw = data_dir / "raw" / "polymarket"
        pm_legacy = data_dir / "polymarket"
        raw_dir = pm_raw if pm_raw.exists() else pm_legacy
        cid_to_mid = lib.build_condition_id_to_market_id_from_raw(raw_dir)

    # 保留市场 = 有 plmkt 录制 且 0x8dxd 有动作（且能映射到 market_id）
    kept_market_ids = set()
    for cid in tracker_cids:
        mid = cid_to_mid.get(cid)
        if mid and mid in pm_ranges:
            kept_market_ids.add(mid)
    kept_condition_ids = [cid for cid, mid in cid_to_mid.items() if mid in kept_market_ids]

    union_ranges = _union_symbol_time_ranges(
        kept_market_ids, pm_ranges, cid_to_mid, trade_entry_by_cid
    )
    keep_hours_binance_chainlink = _hours_overlapping_ranges(union_ranges)

    result = {
        "data_dir": str(data_dir),
        "kept_market_ids": sorted(kept_market_ids),
        "kept_condition_ids": sorted(set(kept_condition_ids)),
        "union_symbol_time_ranges": union_ranges,
        "keep_hours_binance_chainlink": sorted(keep_hours_binance_chainlink),
        "pm_market_count": len(pm_ranges),
        "tracker_cid_count": len(tracker_cids),
        "cid_to_mid_count": len(cid_to_mid),
    }
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description="数据保留审计（只读），输出 audit_result.json")
    parser.add_argument("--data-dir", default=lib.POLY_DATA_DIR, help="数据根目录")
    parser.add_argument("--output", "-o", default="", help="输出 JSON 路径，默认 eda/retention/audit_result.json")
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"数据目录不存在: {data_dir}", file=sys.stderr)
        return 1
    out = Path(args.output) if args.output else Path(__file__).resolve().parent / "audit_result.json"
    result = run_audit(data_dir, out)
    print(f"kept_market_ids: {len(result['kept_market_ids'])}")
    print(f"kept_condition_ids: {len(result['kept_condition_ids'])}")
    print(f"union ranges: {len(result['union_symbol_time_ranges'])}")
    print(f"keep_hours (binance/chainlink): {len(result['keep_hours_binance_chainlink'])}")
    print(f"已写入: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
