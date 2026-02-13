#!/usr/bin/env python3
"""
从 0x8dxd 过去 24 小时成交中，按市场（conditionId）计算 PnL，
先筛出买入次数 >= N（默认 10）的市场，再从中取最大盈利/最大亏损代表；
输出所有符合条件市场的列表（markets），供 ETL 与画图使用（有多少市场画多少张图）。
输出 JSON：max_profit / max_loss、market_ids_for_etl、markets（列表）、symbol_for_etl、时间窗口等。
"""

import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")

# 与 discovery 一致：仅保留 Crypto 相关市场时的关键词
CRYPTO_KEYWORDS = ["Bitcoin", "Ethereum", "Solana", "Crypto", "BTC", "ETH", "SOL", "XRP", "Ripple"]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
HOUR_MS = 3600 * 1000


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                pass


def _partition_hour_from_path(path: Path) -> int | None:
    parts = path.parts
    dt = None
    hour = None
    for p in parts:
        if p.startswith("dt="):
            dt = p[3:]
        if p.startswith("hour="):
            hour = p[5:]
    if not dt or hour is None:
        return None
    try:
        import datetime as _dt
        d = _dt.datetime.strptime(f"{dt} {hour}", "%Y-%m-%d %H")
        return int(d.timestamp() * 1000)
    except Exception:
        return None


def _recent_partition_files(root: Path, glob_pat: str, recent_hours: int) -> list[Path]:
    files = sorted(root.rglob(glob_pat)) if root.exists() else []
    if recent_hours <= 0:
        return files
    cutoff_ms = int(__import__("time").time() * 1000) - recent_hours * HOUR_MS
    out = []
    for p in files:
        h = _partition_hour_from_path(p)
        if h is None or h >= cutoff_ms:
            out.append(p)
    return out


def _collect_silver_parquet_files(data_dir: Path, source: str, path_substring: str = "") -> list[Path]:
    silver_root = data_dir / "lake" / "silver" / source
    if not silver_root.exists():
        return []
    files = sorted(silver_root.rglob("*.parquet"), key=lambda p: str(p))
    if path_substring:
        files = [p for p in files if path_substring in str(p)]
    return files


def load_trades_from_silver(
    data_dir: Path, end_ts_ms: int, window_hours: float, recent_hours_partitions: int, use_api_timestamp: bool = True
) -> list[dict]:
    """从 silver tracker_0x8dxd parquet 读取并过滤时间窗口内的成交。"""
    files = _collect_silver_parquet_files(data_dir, "tracker_0x8dxd", "trades")
    if not files:
        return []
    if recent_hours_partitions > 0:
        cutoff_ms = int(__import__("time").time() * 1000) - recent_hours_partitions * HOUR_MS
        files = [p for p in files if _partition_hour_from_path(p) is None or _partition_hour_from_path(p) >= cutoff_ms]
    dfs = []
    for p in files:
        try:
            dfs.append(pd.read_parquet(p))
        except Exception:
            continue
    if not dfs:
        return []
    df = pd.concat(dfs, ignore_index=True)
    start_ts_ms = end_ts_ms - int(window_hours * HOUR_MS)
    start_ts_sec = start_ts_ms // 1000
    end_ts_sec = end_ts_ms // 1000
    if use_api_timestamp and "timestamp" in df.columns:
        t = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df[(t >= start_ts_sec) & (t <= end_ts_sec)]
    else:
        ts_col = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df.columns else "ingest_time_ms"
        if ts_col not in df.columns:
            return []
        t = pd.to_numeric(df[ts_col], errors="coerce")
        df = df[(t >= start_ts_ms) & (t <= end_ts_ms)]
    return df.to_dict("records")


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_index_mapping(index_path: Path) -> dict[str, str]:
    if not index_path.exists():
        return {}
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            mapping = data.get("mapping", data)
            if isinstance(mapping, dict):
                return {str(k): str(v) for k, v in mapping.items() if k and v}
    except Exception:
        pass
    return {}


def build_condition_id_to_market_id_from_silver(
    data_dir: Path, index_path: Path | None = None, recent_hours: int = 0
) -> dict[str, str]:
    """从 lake/silver/polymarket 的 parquet 建立 conditionId -> market_id；每个 market_XXX 只读一个 parquet 取首条 raw_data。"""
    cid_to_mid = {}
    silver_root = data_dir / "lake" / "silver" / "polymarket"
    if not silver_root.exists():
        return cid_to_mid
    cutoff_ms = int(__import__("time").time() * 1000) - recent_hours * HOUR_MS if recent_hours > 0 else 0
    market_to_one_file: dict[str, Path] = {}
    for d in silver_root.iterdir():
        if not d.is_dir():
            continue
        for hour_dir in d.iterdir() if d.name.startswith("dt=") else []:
            if not hour_dir.is_dir() or not hour_dir.name.startswith("hour="):
                continue
            if recent_hours > 0:
                h = _partition_hour_from_path(hour_dir)
                if h is not None and h < cutoff_ms:
                    continue
            for mid_dir in hour_dir.iterdir():
                if not mid_dir.is_dir() or not mid_dir.name.startswith("market_"):
                    continue
                mid = mid_dir.name.replace("market_", "").strip()
                if not mid.isdigit() or mid in market_to_one_file:
                    continue
                one = next(mid_dir.rglob("*.parquet"), None)
                if one is not None:
                    market_to_one_file[mid] = one
    for market_id, path in market_to_one_file.items():
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if "raw_data" not in df.columns or df.empty:
            continue
        row = df.iloc[0]
        raw = row.get("raw_data")
        if pd.isna(raw):
            continue
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
        if not isinstance(raw, dict):
            continue
        cid = raw.get("market")
        if cid:
            cid_to_mid[str(cid).strip()] = market_id
    if index_path is not None:
        _atomic_write_json(index_path, {"updated_at_ms": int(__import__("time").time() * 1000), "mapping": cid_to_mid})
    return cid_to_mid


def build_condition_id_to_market_id(polymarket_dir: Path, index_path: Path | None = None, recent_hours: int = 0) -> dict[str, str]:
    """
    从 polymarket 目录下 market_*.jsonl 的首条含 raw_data.market 的记录建立 conditionId -> market_id。
    只有「本机曾录制过盘口」的市场才有对应文件；trades 里可能包含未录制市场的成交，这类 conditionId 无法解析出 market_id（为 null），无法参与 ETL/画图。
    """
    cid_to_mid = {}
    if not polymarket_dir.exists():
        return cid_to_mid
    files = _recent_partition_files(polymarket_dir, "market_*.jsonl", recent_hours)
    for path in files:
        name = path.stem  # market_123 或 market_123_2025010112
        parts = name.replace("market_", "").split("_")
        market_id = parts[0] if parts else None
        if not market_id or not market_id.isdigit():
            continue
        for rec in _iter_jsonl(path):
            raw = rec.get("raw_data")
            if not isinstance(raw, dict):
                continue
            cid = raw.get("market")
            if cid:
                cid_to_mid[str(cid).strip()] = market_id
                break
    if index_path is not None:
        _atomic_write_json(index_path, {"updated_at_ms": int(__import__("time").time() * 1000), "mapping": cid_to_mid})
    return cid_to_mid


def is_crypto_trade(rec: dict) -> bool:
    """根据 title / slug 判断是否为 Crypto 相关市场（与 discovery 口径一致）。"""
    text = " ".join(
        str(x).lower() for x in (rec.get("title") or "", rec.get("slug") or "") if x
    )
    if not text:
        return False
    for kw in CRYPTO_KEYWORDS:
        k = kw.lower()
        if k in ("eth", "sol", "btc", "xrp"):
            if k == "eth" and ("ethereum" in text or re.search(r"\beth\b", text)):
                return True
            if k == "sol" and ("solana" in text or re.search(r"\bsol\b", text)):
                return True
            if k == "btc" and ("bitcoin" in text or re.search(r"\bbtc\b", text)):
                return True
            if k == "xrp" and ("ripple" in text or re.search(r"\bxrp\b", text)):
                return True
        elif k in text:
            return True
    return False


def coin_from_market(entry: dict) -> str:
    """从市场的 title/slug 推断币种符号，用于 Binance symbol（如 ETH -> ethusdt）。"""
    text = " ".join(
        str(x).lower() for x in (entry.get("title") or "", entry.get("slug") or "") if x
    )
    if not text:
        return ""
    if "ethereum" in text or re.search(r"\beth\b", text):
        return "ETH"
    if "bitcoin" in text or re.search(r"\bbtc\b", text):
        return "BTC"
    if "solana" in text or re.search(r"\bsol\b", text):
        return "SOL"
    if "ripple" in text or re.search(r"\bxrp\b", text):
        return "XRP"
    return ""


def load_trades_in_window(
    trade_files: list[Path], end_ts_ms: int, window_hours: float, use_api_timestamp: bool = True
) -> list[dict]:
    """
    读取 trades.jsonl，只保留时间窗口内的记录。
    use_api_timestamp=True 时用 API 的 timestamp（成交时间，单位秒）过滤，否则用 local_receipt_ts_ms（本机收到时间，毫秒）。
    """
    start_ts_ms = end_ts_ms - int(window_hours * HOUR_MS)
    start_ts_sec = start_ts_ms // 1000
    end_ts_sec = end_ts_ms // 1000
    rows = []
    for trades_path in trade_files:
        for rec in _iter_jsonl(trades_path):
            if use_api_timestamp and rec.get("timestamp") is not None:
                try:
                    ts = int(rec["timestamp"])
                except (TypeError, ValueError):
                    ts = None
                if ts is not None and start_ts_sec <= ts <= end_ts_sec:
                    rows.append(rec)
                continue
            ts = rec.get("local_receipt_ts_ms")
            if ts is None:
                continue
            try:
                ts = int(ts)
            except (TypeError, ValueError):
                continue
            if start_ts_ms <= ts <= end_ts_ms:
                rows.append(rec)
    return rows


def _parse_number(rec: dict, *keys: str) -> float | None:
    """从 rec 中按 keys 顺序取第一个存在的值并转为 float，失败返回 None。"""
    for k in keys:
        v = rec.get(k)
        if v is None:
            continue
        try:
            return float(str(v).strip())
        except (TypeError, ValueError):
            continue
    return None


def pnl_per_market(trades: list[dict]) -> dict[str, dict]:
    """
    按 conditionId 分组，计算每个市场的简单交易 PnL（美元）：
    PnL = sum(SELL 的 price*size) - sum(BUY 的 price*size)。
    API 的 price 为 0–1（每份美元价），size 为份数，price*size = 美元。
    兼容字段名：price/limitPrice/priceDecimal，size/amount。
    """
    by_cid = {}
    for rec in trades:
        cid = rec.get("conditionId") or rec.get("condition_id")
        if not cid:
            continue
        cid = str(cid).strip()
        if not cid:
            continue
        price = _parse_number(rec, "price", "limitPrice", "priceDecimal")
        size = _parse_number(rec, "size", "amount")
        if price is None or size is None or size <= 0:
            continue
        if price < 0 or price > 1.1:
            continue
        side = (rec.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            continue
        if cid not in by_cid:
            by_cid[cid] = {"cost": 0.0, "revenue": 0.0, "n_buys": 0, "n_sells": 0, "slug": rec.get("slug"), "title": rec.get("title")}
        notional = price * size
        if side == "BUY":
            by_cid[cid]["cost"] += notional
            by_cid[cid]["n_buys"] += 1
        else:
            by_cid[cid]["revenue"] += notional
            by_cid[cid]["n_sells"] += 1
    for cid, v in by_cid.items():
        v["pnl"] = v["revenue"] - v["cost"]
    return by_cid


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="选出 0x8dxd 过去 24h 最大盈利与最大亏损的 2 个市场，输出供 ETL 使用")
    parser.add_argument("--data-dir", default="", help="数据根目录，默认 POLY_DATA_DIR")
    parser.add_argument("--trades", default="", help="trades.jsonl 路径，默认 <data-dir>/tracker_0x8dxd/trades.jsonl")
    parser.add_argument("--hours", type=float, default=24.0, help="统计时间窗口（小时）")
    parser.add_argument("--end-ts-ms", type=int, default=0, help="窗口结束时间戳(ms)，0=当前时间")
    parser.add_argument("--output", default="", help="输出 JSON 路径，默认打印到 stdout")
    parser.add_argument("--min-buys", type=int, default=10, help="只保留买入次数 >= 该值的市场（默认 10）")
    parser.add_argument("--run-etl", action="store_true", help="选完后自动调用 run_etl（同目录）")
    parser.add_argument("--use-receipt-time", action="store_true", help="用 local_receipt_ts_ms 过滤窗口，默认用 API 的 timestamp（成交时间）")
    parser.add_argument("--no-crypto-filter", action="store_true", help="关闭 Crypto 市场筛选，默认只统计 title/slug 含 BTC/ETH/SOL/XRP 等关键词的市场")
    parser.add_argument("--recent-hours-partitions", type=int, default=72, help="只读取最近 N 小时分区（0=不限制）")
    args = parser.parse_args()

    data_dir = Path(args.data_dir or POLY_DATA_DIR)
    if not data_dir.is_absolute():
        data_dir = (PROJECT_ROOT / data_dir).resolve()
    end_ts_ms = args.end_ts_ms or (int(__import__("time").time() * 1000))
    use_api_ts = not args.use_receipt_time
    if args.trades:
        trade_files = [Path(args.trades)]
        trades = load_trades_in_window(trade_files, end_ts_ms, args.hours, use_api_timestamp=use_api_ts)
    else:
        silver_trades = load_trades_from_silver(
            data_dir, end_ts_ms, args.hours, args.recent_hours_partitions, use_api_timestamp=use_api_ts
        )
        if silver_trades:
            trades = silver_trades
        else:
            raw_root = data_dir / "raw" / "tracker_0x8dxd"
            legacy_file = data_dir / "tracker_0x8dxd" / "trades.jsonl"
            trade_files = _recent_partition_files(raw_root, "trades.jsonl", args.recent_hours_partitions)
            if legacy_file.exists():
                trade_files.append(legacy_file)
            if not trade_files:
                print("trades 文件不存在: lake/silver/tracker_0x8dxd 或 raw/tracker_0x8dxd", file=sys.stderr)
                return 1
            trades = load_trades_in_window(trade_files, end_ts_ms, args.hours, use_api_timestamp=use_api_ts)
    if not trades:
        print("过去", args.hours, "小时内无成交记录", file=sys.stderr)
        return 1

    if not args.no_crypto_filter:
        trades = [r for r in trades if is_crypto_trade(r)]
        if not trades:
            print("过去", args.hours, "小时内无 Crypto 相关成交记录（可加 --no-crypto-filter 查看全部）", file=sys.stderr)
            return 1

    by_market = pnl_per_market(trades)
    if not by_market:
        print("无有效按市场的 PnL", file=sys.stderr)
        return 1

    pm_raw = data_dir / "raw" / "polymarket"
    pm_legacy = data_dir / "polymarket"
    silver_pm = data_dir / "lake" / "silver" / "polymarket"
    _checkpoint_root = (Path(os.environ.get("POLY_CHECKPOINT_DIR", "")).resolve() if os.environ.get("POLY_CHECKPOINT_DIR") else data_dir / "checkpoint")
    index_path = _checkpoint_root / "condition_market_index.json"
    cid_to_mid_existing = _load_index_mapping(index_path)
    if silver_pm.exists() and list(silver_pm.rglob("*.parquet")):
        cid_to_mid_recent = build_condition_id_to_market_id_from_silver(
            data_dir, index_path=index_path, recent_hours=args.recent_hours_partitions
        )
    else:
        cid_to_mid_recent = build_condition_id_to_market_id(
            pm_raw if pm_raw.exists() else pm_legacy,
            index_path=index_path,
            recent_hours=args.recent_hours_partitions,
        )
    cid_to_mid = dict(cid_to_mid_existing)
    cid_to_mid.update(cid_to_mid_recent)
    start_ts_ms = end_ts_ms - int(args.hours * HOUR_MS)

    min_buys = max(0, int(args.min_buys))
    filtered_cids = [c for c in by_market if (by_market[c].get("n_buys") or 0) >= min_buys]
    if not filtered_cids:
        print(f"无买入次数 >= {min_buys} 的市场（可降低 --min-buys）", file=sys.stderr)
        return 1

    # 对当前窗口真正用到但缺失映射的 cid，做一次全量回填（只在必要时触发）
    missing = [c for c in filtered_cids if c not in cid_to_mid]
    if missing:
        if silver_pm.exists() and list(silver_pm.rglob("*.parquet")):
            full_map = build_condition_id_to_market_id_from_silver(data_dir, index_path=None, recent_hours=0)
        else:
            full_map = build_condition_id_to_market_id(
                pm_raw if pm_raw.exists() else pm_legacy,
                index_path=index_path,
                recent_hours=0,
            )
        cid_to_mid.update(full_map)

    sorted_cids = sorted(filtered_cids, key=lambda c: by_market[c]["pnl"])
    best_cid = sorted_cids[-1]
    worst_cid = sorted_cids[0]
    best = by_market[best_cid].copy()
    worst = by_market[worst_cid].copy()
    best["conditionId"] = best_cid
    best["market_id"] = cid_to_mid.get(best_cid)
    worst["conditionId"] = worst_cid
    worst["market_id"] = cid_to_mid.get(worst_cid)

    markets_list = []
    for cid in sorted_cids:
        entry = by_market[cid].copy()
        entry["conditionId"] = cid
        entry["market_id"] = cid_to_mid.get(cid)
        entry["coin"] = coin_from_market(entry)
        markets_list.append(entry)

    total_pnl = sum(by_market[c]["pnl"] for c in by_market)
    coin = coin_from_market(best) or coin_from_market(worst) or "BTC"
    symbol_for_etl = (coin.lower() + "usdt").strip()

    market_ids_for_etl = [m.get("market_id") for m in markets_list if m.get("market_id")]
    market_ids_by_coin = {}
    for m in markets_list:
        mid = m.get("market_id")
        if not mid:
            continue
        c = (m.get("coin") or "").strip().upper() or "BTC"
        market_ids_by_coin.setdefault(c, []).append(mid)

    n_with_mid = sum(1 for m in markets_list if m.get("market_id"))
    out = {
        "window_start_ts_ms": start_ts_ms,
        "window_end_ts_ms": end_ts_ms,
        "window_hours": args.hours,
        "filter_by": "timestamp (API 成交时间)" if use_api_ts else "local_receipt_ts_ms (本机收到时间)",
        "crypto_only": not args.no_crypto_filter,
        "trades_in_window": len(trades),
        "markets_with_trades": len(by_market),
        "min_buys": min_buys,
        "markets_above_min_buys": len(markets_list),
        "markets_with_market_id": n_with_mid,
        "_note_market_id": "market_id 来自本机 Polymarket 录制(market_*.jsonl)；未录制的市场为 null，已从 market_ids_for_etl 排除",
        "total_pnl_usd": round(total_pnl, 2),
        "max_profit": best,
        "max_loss": worst,
        "market_ids_for_etl": market_ids_for_etl,
        "market_ids_by_coin": market_ids_by_coin,
        "markets": markets_list,
        "symbol_for_etl": symbol_for_etl,
    }

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print("已写入", out_path)
    else:
        print(json.dumps(out, ensure_ascii=False, indent=2))

    if args.run_etl and out["market_ids_for_etl"] and out.get("market_ids_by_coin") and args.output:
        script_dir = Path(__file__).resolve().parent
        run_etl = script_dir / "run_etl.py"
        if run_etl.exists():
            import subprocess
            for coin, ids in market_ids_by_coin.items():
                symbol = (coin.lower() + "usdt").strip()
                out_name = "master_%susdt.parquet" % coin.lower()
                cmd = [
                    sys.executable,
                    str(run_etl),
                    "--data-dir", str(data_dir),
                    "--symbol", symbol,
                    "--output-filename", out_name,
                    "--market-ids", ",".join(ids),
                    "--start-ts-ms", str(start_ts_ms),
                    "--end-ts-ms", str(end_ts_ms),
                ]
                print("执行 ETL (symbol=%s, %d 个市场):" % (symbol, len(ids)), " ".join(cmd), file=sys.stderr)
                ret = subprocess.call(cmd)
                if ret != 0:
                    return ret
    return 0


if __name__ == "__main__":
    sys.exit(main())
