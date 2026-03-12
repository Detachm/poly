#!/usr/bin/env python3
"""
综合拉取 0x8dxd 尽可能最长时间的 Trades + Activity + Positions，供分析用。

- Trades: 分页至 offset 4000 或空/报错（单次约 3.7k 条）
- Activity: 分页至 offset 3500 或空/报错（单次约 3.5k 条，含 TRADE/REDEEM）
- Positions: 当前持仓
- 输出：trades_raw/cleaned、activity_raw/cleaned、positions_raw、pnl_sources_summary，并打印整体时间跨度

用法:
  python scripts/download_0x8dxd_full.py [--user 0x...] [--output-dir eda/01_etl/output/0x8dxd_full]
  HTTPS_PROXY=http://127.0.0.1:7897 python scripts/download_0x8dxd_full.py
"""

import argparse
import csv
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

DATA_API_TRADES = "https://data-api.polymarket.com/trades"
DATA_API_ACTIVITY = "https://data-api.polymarket.com/activity"
DATA_API_POSITIONS = "https://data-api.polymarket.com/positions"
DEFAULT_ADDRESS = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 90
MAX_RETRIES = 3
RETRY_DELAY = 5
MARKET_CATEGORY_KEYWORDS = [
    ("crypto", ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "ripple", "crypto", "usdc"]),
    ("politics", ["trump", "biden", "election", "president", "congress", "vote", "senate"]),
    ("sports", ["nba", "nfl", "super bowl", "world series", "championship", "playoff"]),
    ("macro", ["fed", "rate", "inflation", "gdp", "jobs report", "treasury"]),
]


def _make_session(proxy: str | None):
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "Poly-Full/1.0"})
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _detect_proxy():
    for k in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]:
        v = os.environ.get(k)
        if v:
            return v
    return None


def _safe_float(x, default=0.0):
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# ---- Trades ----
def fetch_all_trades(
    user_address: str, session: requests.Session, limit: int = 1000, verbose: bool = True
) -> tuple[list[dict], str]:
    seen_keys = set()
    out = []
    offset = 0
    page = 0
    stop_reason = "unknown"
    while True:
        page += 1
        rows = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(
                    DATA_API_TRADES,
                    params={"user": user_address, "limit": limit, "offset": offset},
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )
                r.raise_for_status()
                rows = r.json() if r.text else []
                break
            except Exception as e:
                if verbose:
                    print(f"  [WARN] trades offset={offset} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        if rows is None:
            stop_reason = f"error_at_offset_{offset}"
            break
        if not rows:
            stop_reason = "empty_page"
            if verbose:
                print(f"  [INFO] trades offset={offset} 返回空，结束")
            break
        for t in rows:
            key = (t.get("timestamp"), t.get("asset"), t.get("side"), t.get("size"), t.get("price"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(t)
        if verbose:
            print(f"  [INFO] trades 第 {page} 页 offset={offset} 收到 {len(rows)} 条，累计去重 {len(out)} 条")
        if len(rows) < limit:
            stop_reason = "last_page"
            break
        offset += limit
        time.sleep(0.3)
    return out, stop_reason


def infer_market_category(title: str, slug: str) -> str:
    text = f" {title or ''} {slug or ''} ".lower()
    for category, keywords in MARKET_CATEGORY_KEYWORDS:
        if any(kw in text for kw in keywords):
            return category
    return "other"


def clean_trade_row(t: dict) -> dict:
    title = (t.get("title") or "").strip()
    slug = (t.get("slug") or "").strip()
    ts = t.get("timestamp")
    if ts is not None:
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            ts = None
    size = t.get("size")
    try:
        size = float(size) if size is not None else None
    except (TypeError, ValueError):
        size = None
    price = t.get("price")
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    dt_utc = ""
    if ts is not None:
        dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "timestamp": ts,
        "datetime_utc": dt_utc,
        "conditionId": (t.get("conditionId") or "").strip(),
        "question": title,
        "slug": slug,
        "market_category": infer_market_category(title, slug),
        "side": (t.get("side") or "").strip(),
        "outcome": (t.get("outcome") or "").strip(),
        "outcomeIndex": t.get("outcomeIndex"),
        "size": size,
        "price": price,
        "transactionHash": (t.get("transactionHash") or "").strip(),
        "asset": (t.get("asset") or "").strip(),
    }


# ---- Activity ----
def fetch_all_activity(
    user_address: str, session: requests.Session, limit: int = 500, verbose: bool = True
) -> list[dict]:
    out = []
    offset = 0
    page = 0
    while True:
        page += 1
        params = {"user": user_address, "limit": limit, "offset": offset}
        rows = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(DATA_API_ACTIVITY, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                r.raise_for_status()
                rows = r.json() if r.text else []
                break
            except Exception as e:
                if verbose:
                    print(f"  [WARN] activity offset={offset} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        if rows is None:
            break
        if not rows:
            if verbose:
                print(f"  [INFO] activity offset={offset} 返回空，结束")
            break
        out.extend(rows)
        if verbose:
            print(f"  [INFO] activity 第 {page} 页 offset={offset} 收到 {len(rows)} 条，累计 {len(out)} 条")
        if len(rows) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return out


def fetch_all_positions(
    user_address: str, session: requests.Session, limit: int = 500, verbose: bool = True
) -> list[dict]:
    out = []
    offset = 0
    page = 0
    while True:
        page += 1
        params = {"user": user_address, "limit": limit, "offset": offset}
        rows = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(DATA_API_POSITIONS, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                r.raise_for_status()
                rows = r.json() if r.text else []
                break
            except Exception as e:
                if verbose:
                    print(f"  [WARN] positions offset={offset} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        if rows is None:
            break
        if not rows:
            break
        out.extend(rows)
        if verbose:
            print(f"  [INFO] positions 第 {page} 页 offset={offset} 收到 {len(rows)} 条，累计 {len(out)} 条")
        if len(rows) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return out


def _activity_usdc_signed(row: dict) -> float:
    u = _safe_float(row.get("usdcSize"))
    t = (row.get("type") or "").strip()
    if t == "TRADE" and u != 0:
        side = (row.get("side") or "").strip().upper()
        if side == "BUY" and u > 0:
            u = -u
        elif side == "SELL" and u < 0:
            u = -u
    return u


def main():
    parser = argparse.ArgumentParser(description="综合拉取 0x8dxd Trades + Activity + Positions，尽可能最长时间")
    parser.add_argument("--user", default=os.environ.get("TRACKER_USER_ADDRESS", DEFAULT_ADDRESS), help="用户地址")
    parser.add_argument("--output-dir", default="eda/01_etl/output/0x8dxd_full", help="输出目录")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    proxy = None if args.no_proxy else _detect_proxy()
    if proxy:
        print(f"使用代理: {proxy}")
    session = _make_session(proxy)
    user = args.user.strip()

    all_ts: list[float] = []

    # ---------- 1) Trades ----------
    print("\n--- Trades ---")
    trades, trades_stop = fetch_all_trades(user, session)
    n_trades = len(trades)
    print(f"Trades 单次拿到: {n_trades} 条  |  停止原因: {trades_stop}")

    raw_path = out_dir / "trades_raw.jsonl"
    with open(raw_path, "w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    print(f"已写 {raw_path}")

    if trades:
        cleaned = [clean_trade_row(t) for t in trades]
        for r in cleaned:
            if r.get("timestamp") is not None:
                all_ts.append(float(r["timestamp"]))
        csv_path = out_dir / "trades_cleaned.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cleaned[0].keys())
            w.writeheader()
            w.writerows(cleaned)
        print(f"已写 {csv_path}")
        cats = Counter(r["market_category"] for r in cleaned)
        print("按市场类型条数:", dict(cats))

    # ---------- 2) Activity ----------
    print("\n--- Activity ---")
    activity = fetch_all_activity(user, session)
    n_act = len(activity)
    if n_act >= 3500:
        print("  [提示] Activity 约 3500 条后 API 返回 400，当前为单次可拉上限。")

    act_path = out_dir / "activity_raw.jsonl"
    with open(act_path, "w", encoding="utf-8") as f:
        for row in activity:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"已写 {act_path}，共 {n_act} 条")

    act_cleaned = []
    by_type = {}
    for row in activity:
        t = (row.get("type") or "UNKNOWN").strip()
        if t not in by_type:
            by_type[t] = {"count": 0, "usdcSize_sum": 0.0}
        by_type[t]["count"] += 1
        u = _activity_usdc_signed(row)
        by_type[t]["usdcSize_sum"] += u
        ts = row.get("timestamp")
        if ts is not None:
            all_ts.append(_safe_float(ts))
        act_cleaned.append({
            "timestamp": row.get("timestamp"),
            "type": t,
            "conditionId": (row.get("conditionId") or "").strip(),
            "title": (row.get("title") or "").strip(),
            "slug": (row.get("slug") or "").strip(),
            "side": (row.get("side") or "").strip(),
            "outcome": (row.get("outcome") or "").strip(),
            "usdcSize": round(u, 6),
            "size": row.get("size"),
            "price": row.get("price"),
            "transactionHash": (row.get("transactionHash") or "").strip(),
        })
    if act_cleaned:
        ac_path = out_dir / "activity_cleaned.csv"
        with open(ac_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=act_cleaned[0].keys())
            w.writeheader()
            w.writerows(act_cleaned)
        print(f"已写 {ac_path}")

    # ---------- 3) Positions ----------
    print("\n--- Positions ---")
    positions = fetch_all_positions(user, session)
    pos_path = out_dir / "positions_raw.jsonl"
    with open(pos_path, "w", encoding="utf-8") as f:
        for row in positions:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"已写 {pos_path}，共 {len(positions)} 条")

    positions_cash_pnl = sum(_safe_float(p.get("cashPnl")) for p in positions)
    positions_realized_pnl = sum(_safe_float(p.get("realizedPnl")) for p in positions)

    # ---------- 4) PnL 汇总 ----------
    summary_rows = []
    for t, v in sorted(by_type.items()):
        summary_rows.append({
            "source": f"activity_{t}",
            "type": t,
            "count": v["count"],
            "usdc_total": round(v["usdcSize_sum"], 4),
            "note": "TRADE=交易净现金流 REDEEM=结算兑付",
        })
    summary_rows.append({
        "source": "positions_unrealized",
        "type": "POSITIONS_CASH_PNL",
        "count": len(positions),
        "usdc_total": round(positions_cash_pnl, 4),
        "note": "持仓未实现",
    })
    summary_rows.append({
        "source": "positions_realized",
        "type": "POSITIONS_REALIZED_PNL",
        "count": len(positions),
        "usdc_total": round(positions_realized_pnl, 4),
        "note": "持仓已实现",
    })
    summary_path = out_dir / "pnl_sources_summary.csv"
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source", "type", "count", "usdc_total", "note"])
        w.writeheader()
        w.writerows(summary_rows)
    print(f"已写 {summary_path}")

    # ---------- 5) 整体时间跨度 ----------
    print("\n" + "=" * 50)
    print("综合拉取结果（尽可能最长时间）")
    print("=" * 50)
    print(f"  Trades:  {n_trades} 条  (停止: {trades_stop})")
    print(f"  Activity: {n_act} 条")
    print(f"  Positions: {len(positions)} 条")
    if all_ts:
        ts_min, ts_max = min(all_ts), max(all_ts)
        span_sec = ts_max - ts_min
        span_h = span_sec / 3600
        print(f"  时间范围: {datetime.fromtimestamp(ts_min, tz=timezone.utc)} ~ {datetime.fromtimestamp(ts_max, tz=timezone.utc)} UTC")
        print(f"  时间跨度: {span_sec:.0f} 秒 = {span_h:.2f} 小时 = {span_h/24:.2f} 天")
    total_act = sum(v["usdcSize_sum"] for v in by_type.values())
    print(f"  Activity 合计 usdc: {total_act:.2f}  |  持仓未实现: {positions_cash_pnl:.2f}  |  持仓已实现: {positions_realized_pnl:.2f}")
    print("=" * 50)


if __name__ == "__main__":
    main()
