#!/usr/bin/env python3
"""
Phase 1 ETL: 以 BookTicker 为主骨架，挂载 AggTrades 窗内特征、Polymarket 盘口、Chainlink 价格。
- 主时间轴: Binance book_ticker 的 local_receipt_ts_ms
- AggTrades: 过去 N ms 的主动买/卖量、成交量、VWAP、笔数（衍生指标）
- Polymarket: 从 targets.json 的 question 解析 label，每 market 合并 best_bid/best_ask
- Chainlink: 按 feed 合并 answer
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # poly repo root
DEFAULT_TARGETS = PROJECT_ROOT / "targets.json"


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


def _collect_source_jsonl_files(data_dir: Path, source: str, preferred_names: list[str]) -> list[Path]:
    """
    优先读取新目录 raw/<source>/dt=.../hour=.../*.jsonl，兼容旧目录 <source>/*.jsonl。
    preferred_names 非空时优先匹配这些文件名。
    """
    files = []
    raw_root = data_dir / "raw" / source
    legacy_root = data_dir / source
    if raw_root.exists():
        files.extend(sorted(raw_root.rglob("*.jsonl"), key=lambda p: str(p)))
    if legacy_root.exists():
        files.extend(sorted(legacy_root.glob("*.jsonl"), key=lambda p: str(p)))
    if not preferred_names:
        return files
    preferred = [p for p in files if p.name in preferred_names]
    others = [p for p in files if p.name not in preferred_names]
    return preferred + others


def _collect_silver_parquet_files(data_dir: Path, source: str, path_substring: str = "") -> list[Path]:
    """
    读取 lake/silver/<source>/ 下所有 *.parquet。
    path_substring 非空时只保留路径中包含该子串的文件（如 "book_ticker"、"agg_trades"、"market_123"）。
    """
    silver_root = data_dir / "lake" / "silver" / source
    if not silver_root.exists():
        return []
    files = sorted(silver_root.rglob("*.parquet"), key=lambda p: str(p))
    if path_substring:
        files = [p for p in files if path_substring in str(p)]
    return files


def _load_silver_parquet(
    files: list[Path], max_rows: int = 0, progress_interval: int = 0, label: str = ""
) -> pd.DataFrame:
    """将多个 silver parquet 读入并合并为一个 DataFrame。"""
    if not files:
        return pd.DataFrame()
    dfs = []
    n_files = len(files)
    for fi, path in enumerate(files):
        if progress_interval and label:
            print(f"  [{label}] 读文件 {fi + 1}/{n_files}: {path.relative_to(path.parents[3])}", flush=True)
        try:
            df = pd.read_parquet(path)
            dfs.append(df)
        except Exception:
            continue
        if max_rows and sum(len(d) for d in dfs) >= max_rows:
            break
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    if max_rows and len(out) > max_rows:
        out = out.head(max_rows)
    return out


def _binance_files_sorted(dir_path: Path, base_name: str, max_files: int = 0):
    base = dir_path / f"{base_name}.jsonl"
    rest = sorted(dir_path.glob(f"{base_name}_*.jsonl"), key=lambda p: p.name)
    combined = (rest + [base]) if base.exists() else rest
    if not combined:
        return []
    return combined[-max_files:] if max_files > 0 else combined


def _load_all_jsonl(
    files: list[Path], max_rows: int = 0, progress_interval: int = 0, label: str = ""
) -> list[dict]:
    rows = []
    n_files = len(files)
    for fi, path in enumerate(files):
        if progress_interval and label:
            print(f"  [{label}] 读文件 {fi + 1}/{n_files}: {path.name}", flush=True)
        for rec in _iter_jsonl(path):
            rows.append(rec)
            if progress_interval and len(rows) > 0 and len(rows) % progress_interval == 0:
                print(f"  [{label}] 已读 {len(rows)} 条", flush=True)
            if max_rows and len(rows) >= max_rows:
                return rows
    return rows


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_polymarket_label(question: str) -> str | None:
    """从 question 解析出资产+时间标签，如 btc_15m, sol_1h。无法解析返回 None。"""
    if not question or not isinstance(question, str):
        return None
    q = question.lower().strip()
    asset = None
    for pattern, tag in [
        (r"\bbitcoin\b|\bbtc\b", "btc"),
        (r"\bethereum\b|\beth\b", "eth"),
        (r"\bsolana\b|\bsol\b", "sol"),
        (r"\bripple\b|\bxrp\b", "xrp"),
    ]:
        if re.search(pattern, q):
            asset = tag
            break
    if not asset:
        return None
    time_part = None
    if re.search(r"\b15\s*(m|min|mins|minute|minutes)\b", q):
        time_part = "15m"
    elif "1h" in q or "1 h" in q or "one hour" in q:
        time_part = "1h"
    elif "february" in q or "feb" in q:
        time_part = "feb"
    elif "december" in q or "2026" in q:
        time_part = "dec26"
    if time_part:
        return f"{asset}_{time_part}"
    return asset


def extract_best_bid_ask(raw_data: dict | list) -> tuple[float | None, float | None]:
    """从 Polymarket raw_data (book 消息，可为 list 快照) 提取 best_bid, best_ask。"""
    data = raw_data
    if isinstance(data, list):
        data = data[0] if data and isinstance(data[0], dict) else {}
    if not isinstance(data, dict):
        return None, None
    bids = data.get("bids") or []
    asks = data.get("asks") or []
    best_bid = None
    for level in bids:
        p = level.get("price") if isinstance(level, dict) else (level[0] if isinstance(level, (list, tuple)) and level else None)
        if p is not None:
            try:
                best_bid = max((best_bid or 0), float(p))
            except (TypeError, ValueError):
                pass
    best_ask = None
    for level in asks:
        p = level.get("price") if isinstance(level, dict) else (level[0] if isinstance(level, (list, tuple)) and level else None)
        if p is not None:
            try:
                v = float(p)
                best_ask = min(best_ask, v) if best_ask is not None else v
            except (TypeError, ValueError):
                pass
    return best_bid, best_ask


def _mart_output_path(
    data_dir: Path,
    symbol_filter: str,
    output_filename: str,
    end_ts_ms: int,
) -> Path:
    """输出到 lake/mart/aligned_master/symbol=<symbol>/dt=<YYYY-MM-DD>/..."""
    if output_filename.endswith(".parquet"):
        filename = output_filename
    else:
        filename = f"{output_filename}.parquet"
    if end_ts_ms > 0:
        dt = datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    else:
        dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return data_dir / "lake" / "mart" / "aligned_master" / f"symbol={symbol_filter}" / f"dt={dt}" / filename


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Phase 1 ETL: BookTicker 主骨架 + Agg 特征 + Polymarket + Chainlink")
    parser.add_argument("--data-dir", default="", help="数据根目录，覆盖 config 与 POLY_DATA_DIR")
    parser.add_argument("--config", default="", help="etl_config.json 路径，默认 01_etl/config/etl_config.json")
    parser.add_argument("--symbol", default="", help="Binance 交易对，覆盖 config 的 symbol_filter；可由 select_top2_markets --run-etl 自动传入，或从 --top2 文件读取")
    parser.add_argument("--top2", default="", help="top2.json 路径；若未传 --symbol 则从该文件读取 symbol_for_etl 作为 Binance 交易对")
    parser.add_argument("--market-ids", default="", help="仅处理这些 Polymarket market_id，逗号分隔，与 select_top2_markets 输出配合")
    parser.add_argument("--start-ts-ms", type=int, default=0, help="只保留 >= 该时间戳(ms) 的数据，0=不限制")
    parser.add_argument("--end-ts-ms", type=int, default=0, help="只保留 <= 该时间戳(ms) 的数据，0=不限制")
    parser.add_argument("--last-hours", type=float, default=0, help="若>0，等价于 --end-ts-ms=now 且 --start-ts-ms=now- last_hours，忽略单独传入的 start/end")
    parser.add_argument("--output-filename", default="", help="输出 parquet 文件名，覆盖 config；用于按币种输出 master_btcusdt.parquet 等")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    config_dir = script_dir.parent / "config"
    output_dir = script_dir.parent.parent / "output"
    config_path = Path(args.config) if args.config else config_dir / "etl_config.json"
    cfg = load_config(config_path)

    _data_dir_raw = (args.data_dir or cfg.get("data_dir") or "").strip() or POLY_DATA_DIR
    data_dir = Path(_data_dir_raw)
    if not data_dir.is_absolute():
        data_dir = (PROJECT_ROOT / data_dir).resolve()
    targets_file = Path(cfg.get("targets_file") or str(DEFAULT_TARGETS))
    window_ms = int(cfg.get("window_ms", 100))
    poly_tolerance_ms = int(cfg.get("polymarket_tolerance_ms", 1000))
    top2_data = {}
    if args.top2:
        top2_path = Path(args.top2)
        if not top2_path.is_absolute():
            top2_path = (PROJECT_ROOT / top2_path).resolve()
        if top2_path.exists():
            try:
                with open(top2_path, "r", encoding="utf-8") as f:
                    top2_data = json.load(f)
            except Exception:
                pass
    symbol_from_top2 = (top2_data.get("symbol_for_etl") or "").strip().lower()
    market_ids_by_coin = top2_data.get("market_ids_by_coin") or {}
    if not isinstance(market_ids_by_coin, dict):
        market_ids_by_coin = {}
    symbol_filter = (args.symbol or symbol_from_top2 or cfg.get("symbol_filter") or "btcusdt").strip().lower()
    chainlink_tolerance_ms = cfg.get("chainlink_tolerance_ms")  # None = no tolerance
    output_filename = (args.output_filename or cfg.get("output_filename") or "master.parquet").strip()
    max_book = int(cfg.get("max_book_rows") or 0)
    max_agg = int(cfg.get("max_agg_rows") or 0)
    max_poly = int(cfg.get("max_poly_rows_per_market") or 0)
    max_chainlink = int(cfg.get("max_chainlink_rows_per_feed") or 0)

    output_dir.mkdir(parents=True, exist_ok=True)

    start_ts_ms = args.start_ts_ms or 0
    end_ts_ms = args.end_ts_ms or 0
    if args.last_hours and args.last_hours > 0:
        import time
        end_ts_ms = int(time.time() * 1000)
        start_ts_ms = end_ts_ms - int(args.last_hours * 3600 * 1000)
    market_ids_filter = [m.strip() for m in (args.market_ids or "").split(",") if m.strip()]
    out_path = _mart_output_path(data_dir, symbol_filter, output_filename, end_ts_ms)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    def _ts_ok(r):
        t = r.get("local_receipt_ts_ms")
        if t is None:
            return False
        try:
            t = int(t)
        except (TypeError, ValueError):
            return False
        if start_ts_ms and t < start_ts_ms:
            return False
        if end_ts_ms and t > end_ts_ms:
            return False
        return True

    use_silver_binance = bool(_collect_silver_parquet_files(data_dir, "binance", "book_ticker"))
    if not use_silver_binance:
        bn_files_probe = _collect_source_jsonl_files(data_dir, "binance", [])
        if not bn_files_probe:
            print("binance 数据不存在: 需 lake/silver/binance 或 raw/binance", file=sys.stderr)
            return 1

    def stream_to_symbol(stream: str) -> str:
        s = (stream or "").lower()
        if "@" in s:
            return s.split("@")[0]
        return s

    # --- BookTicker 主骨架（优先 silver）---
    print("[1/5] BookTicker 主骨架...", flush=True)
    if use_silver_binance:
        book_files = _collect_silver_parquet_files(data_dir, "binance", "book_ticker")
        df_book = _load_silver_parquet(
            book_files, max_book, progress_interval=500_000, label="BookTicker"
        )
        if "stream" in df_book.columns:
            df_book = df_book[df_book["stream"].astype(str).str.contains("bookTicker", na=False)]
        if "instrument_id" in df_book.columns:
            df_book = df_book[df_book["instrument_id"].astype(str).str.lower() == symbol_filter]
        else:
            df_book = df_book[df_book["stream"].astype(str).str.lower().str.split("@").str[0] == symbol_filter]
    else:
        book_files = _collect_source_jsonl_files(data_dir, "binance", ["book_ticker.jsonl"])
        book_files = [p for p in book_files if p.name.startswith("book_ticker")]
        if not book_files:
            print("未找到 book_ticker 文件", file=sys.stderr)
            return 1
        book_raw = _load_all_jsonl(
            book_files, max_book, progress_interval=500_000, label="BookTicker"
        )
        book_raw = [r for r in book_raw if stream_to_symbol(r.get("stream") or "") == symbol_filter]
        df_book = pd.DataFrame(book_raw) if book_raw else pd.DataFrame()

    if start_ts_ms or end_ts_ms:
        ts_col = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df_book.columns else "ingest_time_ms"
        if ts_col in df_book.columns:
            t = pd.to_numeric(df_book[ts_col], errors="coerce")
            if start_ts_ms:
                df_book = df_book[t >= start_ts_ms]
            if end_ts_ms:
                df_book = df_book[pd.to_numeric(df_book[ts_col], errors="coerce") <= end_ts_ms]

    if df_book.empty:
        print(f"过滤 symbol={symbol_filter} 后 book_ticker 无数据", file=sys.stderr)
        return 1
    ts_col = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df_book.columns else "ingest_time_ms"
    df_book["ts_ms"] = pd.to_numeric(df_book[ts_col], errors="coerce").astype("int64")
    df_book = df_book.dropna(subset=["ts_ms"])
    df_book = df_book.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last").reset_index(drop=True)
    df_book["ts"] = pd.to_datetime(df_book["ts_ms"], unit="ms", utc=True)
    for col in ["b", "B", "a", "A"]:
        if col in df_book.columns:
            df_book[col] = pd.to_numeric(df_book[col], errors="coerce")
    print("  BookTicker 主骨架行数:", len(df_book), flush=True)

    # --- AggTrades 窗内特征（优先 silver）---
    print("[2/5] AggTrades 窗内特征...", flush=True)
    use_silver_agg = bool(_collect_silver_parquet_files(data_dir, "binance", "agg_trades"))
    if use_silver_agg:
        agg_files = _collect_silver_parquet_files(data_dir, "binance", "agg_trades")
        df_agg = _load_silver_parquet(
            agg_files, max_agg, progress_interval=500_000, label="AggTrades"
        )
        if "instrument_id" in df_agg.columns:
            df_agg = df_agg[df_agg["instrument_id"].astype(str).str.lower() == symbol_filter]
        else:
            df_agg = df_agg[df_agg["stream"].astype(str).str.lower().str.split("@").str[0] == symbol_filter]
        ts_col_agg = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df_agg.columns else "ingest_time_ms"
        if start_ts_ms or end_ts_ms and ts_col_agg in df_agg.columns:
            t = pd.to_numeric(df_agg[ts_col_agg], errors="coerce")
            if start_ts_ms:
                df_agg = df_agg[t >= start_ts_ms]
            if end_ts_ms:
                df_agg = df_agg[pd.to_numeric(df_agg[ts_col_agg], errors="coerce") <= end_ts_ms]
    else:
        agg_files = _collect_source_jsonl_files(data_dir, "binance", ["agg_trades.jsonl"])
        agg_files = [p for p in agg_files if p.name.startswith("agg_trades")]
        agg_raw = _load_all_jsonl(
            agg_files, max_agg, progress_interval=500_000, label="AggTrades"
        )
        agg_raw = [r for r in agg_raw if stream_to_symbol(r.get("stream") or "") == symbol_filter]
        if start_ts_ms or end_ts_ms:
            agg_raw = [r for r in agg_raw if _ts_ok(r)]
        df_agg = pd.DataFrame(agg_raw) if agg_raw else pd.DataFrame()
        ts_col_agg = "local_receipt_ts_ms"

    w = str(window_ms)
    if not df_agg.empty:
        if "ts_ms" not in df_agg.columns and ts_col_agg in df_agg.columns:
            df_agg["ts_ms"] = pd.to_numeric(df_agg[ts_col_agg], errors="coerce").astype("int64")
        elif "ts_ms" not in df_agg.columns:
            df_agg["ts_ms"] = pd.to_numeric(df_agg.get("local_receipt_ts_ms", df_agg.get("ingest_time_ms")), errors="coerce").astype("int64")
        df_agg = df_agg.dropna(subset=["ts_ms"]).sort_values("ts_ms").reset_index(drop=True)
        df_agg["p"] = pd.to_numeric(df_agg["p"], errors="coerce")
        df_agg["q"] = pd.to_numeric(df_agg["q"], errors="coerce")
        m = df_agg.get("m")
        df_agg["is_buy"] = (m == True) | (m == "true")
        df_agg["is_sell"] = (m == False) | (m == "false")
        ts_agg = df_agg["ts_ms"].values
        ts_book = df_book["ts_ms"].values
        left_idx = np.searchsorted(ts_agg, ts_book - window_ms, side="left")
        right_idx = np.searchsorted(ts_agg, ts_book, side="right")
        p_arr = df_agg["p"].values
        q_arr = df_agg["q"].values
        is_buy = df_agg["is_buy"].values
        is_sell = df_agg["is_sell"].values
        buy_qty_arr = np.zeros(len(df_book))
        sell_qty_arr = np.zeros(len(df_book))
        vol_arr = np.zeros(len(df_book))
        vwap_num = np.zeros(len(df_book))
        vwap_den = np.zeros(len(df_book))
        count_arr = np.zeros(len(df_book), dtype=np.int64)
        n_book = len(df_book)
        step = max(1, n_book // 10)
        for i in range(n_book):
            if step and i > 0 and i % step == 0:
                print(f"  窗内特征进度 {100 * i // n_book}% ({i}/{n_book})", flush=True)
            l, r = int(left_idx[i]), int(right_idx[i])
            if l >= r:
                continue
            q = q_arr[l:r]
            buy_qty_arr[i] = np.where(is_buy[l:r], q, 0).sum()
            sell_qty_arr[i] = np.where(is_sell[l:r], q, 0).sum()
            vol_arr[i] = q.sum()
            count_arr[i] = r - l
            if vol_arr[i] > 0:
                vwap_num[i] = (p_arr[l:r] * q_arr[l:r]).sum()
                vwap_den[i] = vol_arr[i]
        with np.errstate(divide="ignore", invalid="ignore"):
            vwap_arr = np.where(vwap_den > 0, vwap_num / vwap_den, np.nan)
        df_book["agg_buy_qty_" + w + "ms"] = buy_qty_arr
        df_book["agg_sell_qty_" + w + "ms"] = sell_qty_arr
        df_book["agg_vol_" + w + "ms"] = vol_arr
        df_book["agg_vwap_" + w + "ms"] = vwap_arr
        df_book["agg_count_" + w + "ms"] = count_arr
    else:
        df_book["agg_buy_qty_" + w + "ms"] = 0.0
        df_book["agg_sell_qty_" + w + "ms"] = 0.0
        df_book["agg_vol_" + w + "ms"] = 0.0
        df_book["agg_vwap_" + w + "ms"] = np.nan
        df_book["agg_count_" + w + "ms"] = 0

    # --- Polymarket: targets -> label, 再按 market 读盘口合并 ---
    print("[3/5] Polymarket 盘口合并...", flush=True)
    if targets_file.exists():
        with open(targets_file, "r", encoding="utf-8") as f:
            targets = json.load(f)
        if not isinstance(targets, list):
            targets = []
        market_to_label = {}
        for t in targets:
            mid = t.get("market_id") or t.get("id")
            if not mid:
                continue
            if market_ids_filter and str(mid) not in market_ids_filter:
                continue
            label = parse_polymarket_label(t.get("question") or "")
            if label:
                market_to_label[str(mid)] = label
        if market_ids_filter:
            for mid in market_ids_filter:
                if mid and mid not in market_to_label:
                    market_to_label[mid] = "market_" + mid
        pm_silver_files = _collect_silver_parquet_files(data_dir, "polymarket", "")
        use_silver_pm = bool(pm_silver_files)
        if use_silver_pm and market_to_label:
            for market_id, label in market_to_label.items():
                files = [p for p in pm_silver_files if f"market_{market_id}" in str(p) or f"/market_{market_id}/" in str(p)]
                if not files:
                    continue
                df_pm_all = _load_silver_parquet(files, max_poly * 2 if max_poly else 0)
                if df_pm_all.empty:
                    continue
                ts_col_pm = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df_pm_all.columns else "ingest_time_ms"
                df_pm_all["ts_ms"] = pd.to_numeric(df_pm_all[ts_col_pm], errors="coerce").astype("int64")
                df_pm_all = df_pm_all.dropna(subset=["ts_ms"])
                if start_ts_ms:
                    df_pm_all = df_pm_all[df_pm_all["ts_ms"] >= start_ts_ms]
                if end_ts_ms:
                    df_pm_all = df_pm_all[df_pm_all["ts_ms"] <= end_ts_ms]
                rows = []
                for _, row in df_pm_all.iterrows():
                    raw = row.get("raw_data")
                    if pd.isna(raw):
                        continue
                    if isinstance(raw, str):
                        try:
                            raw = json.loads(raw)
                        except (TypeError, json.JSONDecodeError):
                            continue
                    bb, ba = extract_best_bid_ask(raw)
                    if bb is not None or ba is not None:
                        rows.append({"ts_ms": row["ts_ms"], "best_bid": bb, "best_ask": ba})
                    if max_poly and len(rows) >= max_poly:
                        break
                if not rows:
                    continue
                df_pm = pd.DataFrame(rows)
                df_pm["ts"] = pd.to_datetime(df_pm["ts_ms"], unit="ms", utc=True)
                df_pm = df_pm.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last")
                df_pm = df_pm.rename(columns={"best_bid": "best_bid_" + label, "best_ask": "best_ask_" + label})
                tol = pd.Timedelta(milliseconds=poly_tolerance_ms)
                df_book = pd.merge_asof(
                    df_book.sort_values("ts_ms"),
                    df_pm[["ts", "best_bid_" + label, "best_ask_" + label]],
                    on="ts",
                    direction="backward",
                    tolerance=tol,
                )
                print("  Polymarket 已挂载 market", market_id, "label", label, "(silver)", flush=True)
        elif not use_silver_pm:
            pm_files = _collect_source_jsonl_files(data_dir, "polymarket", [])
            if pm_files and market_to_label:
                for market_id, label in market_to_label.items():
                    files = [p for p in pm_files if p.name == f"market_{market_id}.jsonl" or p.name.startswith(f"market_{market_id}_")]
                    if not files:
                        continue
                    rows = []
                    for path in sorted(files, key=lambda p: p.name):
                        n = 0
                        for rec in _iter_jsonl(path):
                            ts = rec.get("local_receipt_ts_ms")
                            if ts is None:
                                continue
                            try:
                                ts = int(ts)
                            except (TypeError, ValueError):
                                continue
                            if start_ts_ms and ts < start_ts_ms:
                                continue
                            if end_ts_ms and ts > end_ts_ms:
                                continue
                            raw = rec.get("raw_data")
                            if raw is None:
                                continue
                            bb, ba = extract_best_bid_ask(raw)
                            if bb is not None or ba is not None:
                                rows.append({"ts_ms": ts, "best_bid": bb, "best_ask": ba})
                            n += 1
                            if max_poly and n >= max_poly:
                                break
                        if max_poly and len(rows) >= max_poly:
                            break
                    if not rows:
                        continue
                    df_pm = pd.DataFrame(rows)
                    df_pm["ts"] = pd.to_datetime(df_pm["ts_ms"], unit="ms", utc=True)
                    df_pm = df_pm.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last")
                    df_pm = df_pm.rename(columns={"best_bid": "best_bid_" + label, "best_ask": "best_ask_" + label})
                    tol = pd.Timedelta(milliseconds=poly_tolerance_ms)
                    df_book = pd.merge_asof(
                        df_book.sort_values("ts_ms"),
                        df_pm[["ts", "best_bid_" + label, "best_ask_" + label]],
                        on="ts",
                        direction="backward",
                        tolerance=tol,
                    )
                    print("  Polymarket 已挂载 market", market_id, "label", label, flush=True)

    # --- Chainlink 按 feed 合并（优先 silver）---
    print("[4/5] Chainlink 合并...", flush=True)
    cl_silver_files = _collect_silver_parquet_files(data_dir, "chainlink", "")
    if cl_silver_files:
        df_cl = _load_silver_parquet(cl_silver_files, max_chainlink)
        if not df_cl.empty:
            ts_col_cl = "local_receipt_ts_ms" if "local_receipt_ts_ms" in df_cl.columns else "ingest_time_ms"
            df_cl["ts_ms"] = pd.to_numeric(df_cl[ts_col_cl], errors="coerce").astype("int64")
    else:
        df_cl = pd.DataFrame()
    if df_cl.empty:
        cl_files = _collect_source_jsonl_files(data_dir, "chainlink", ["updates.jsonl"])
        cl_files = [p for p in cl_files if p.name.startswith("updates")]
        if cl_files:
            cl_rows = _load_all_jsonl(cl_files, max_chainlink)
            if cl_rows:
                df_cl = pd.DataFrame(cl_rows)
                df_cl["ts_ms"] = pd.to_numeric(df_cl["local_receipt_ts_ms"], errors="coerce").astype("int64")
    if not df_cl.empty:
        df_cl = df_cl.dropna(subset=["ts_ms"])
        if start_ts_ms or end_ts_ms:
            if start_ts_ms:
                df_cl = df_cl[df_cl["ts_ms"] >= start_ts_ms]
            if end_ts_ms:
                df_cl = df_cl[df_cl["ts_ms"] <= end_ts_ms]
        df_cl["answer"] = pd.to_numeric(df_cl["answer"], errors="coerce")
        feed_col = df_cl.get("feed")
        if feed_col is not None:
            for feed in df_cl["feed"].dropna().unique():
                sub = df_cl[df_cl["feed"] == feed].sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last")
                if sub.empty:
                    continue
                sub = sub[["ts_ms", "answer"]].copy()
                sub["ts"] = pd.to_datetime(sub["ts_ms"], unit="ms", utc=True)
                col_name = "chainlink_" + feed.replace("/", "_").replace(" ", "_")
                sub = sub.rename(columns={"answer": col_name})[["ts", col_name]]
                tol_cl = pd.Timedelta(milliseconds=chainlink_tolerance_ms) if chainlink_tolerance_ms else None
                df_book = pd.merge_asof(
                    df_book.sort_values("ts_ms"),
                    sub,
                    on="ts",
                    direction="backward",
                    tolerance=tol_cl,
                )
                print("  Chainlink 已挂载 feed", feed, flush=True)
        else:
            df_cl = df_cl.sort_values("ts_ms").drop_duplicates(subset=["ts_ms"], keep="last")
            df_cl["ts"] = pd.to_datetime(df_cl["ts_ms"], unit="ms", utc=True)
            tol_cl = pd.Timedelta(milliseconds=chainlink_tolerance_ms) if chainlink_tolerance_ms else None
            df_book = pd.merge_asof(
                df_book.sort_values("ts_ms"),
                df_cl[["ts", "answer"]].rename(columns={"answer": "chainlink"}),
                on="ts",
                direction="backward",
                tolerance=tol_cl,
            )
            print("  Chainlink 已挂载 (单列)", flush=True)

    print("[5/5] 写入输出...", flush=True)
    df_book = df_book.sort_values("ts_ms").reset_index(drop=True)
    try:
        df_book.to_parquet(out_path, index=False)
    except Exception as e:
        df_book.to_csv(out_path.with_suffix(".csv"), index=False)
        print("parquet 写入失败，已写 csv:", e)
    else:
        print("已写入", out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
