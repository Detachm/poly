#!/usr/bin/env python3
"""
拉取 0x8dxd 全部盈利来源：成交(trades) + 结算(redeem) + 持仓(positions) + 奖励等。

- Activity API: TRADE / REDEEM / REWARD / MAKER_REBATE / CONVERSION / SPLIT / MERGE
- Positions API: 当前持仓的 cashPnl（未实现）、realizedPnl（已实现）
- 输出：activity 与 positions 的 raw JSONL + 按来源汇总的 pnl_sources_summary

用法:
  python scripts/download_0x8dxd_pnl_sources.py [--user 0x...] [--output-dir ...] [--last-hours 24]
  HTTPS_PROXY=... python scripts/download_0x8dxd_pnl_sources.py --last-hours 24   # 与网页「最近24h 净利润」对比
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path

import requests

NOW_TS = int(time.time())

DATA_API_ACTIVITY = "https://data-api.polymarket.com/activity"
DATA_API_POSITIONS = "https://data-api.polymarket.com/positions"
# 0x8dxd 完整地址（Activity/Positions API 要求 0x+40 位 hex）
DEFAULT_ADDRESS = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 90
MAX_RETRIES = 3
RETRY_DELAY = 5


def _make_session(proxy: str | None):
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "Poly-PnL-Sources/1.0"})
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _detect_proxy():
    for k in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]:
        v = os.environ.get(k)
        if v:
            return v
    return None


def _get_paginated(
    session: requests.Session,
    url: str,
    user: str,
    limit: int = 500,
    extra_params: dict | None = None,
) -> list[dict]:
    out = []
    offset = 0
    page = 0
    params = {"user": user, "limit": limit, "offset": offset}
    if extra_params:
        params.update(extra_params)
    while True:
        page += 1
        params["offset"] = offset
        rows = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                r.raise_for_status()
                rows = r.json() if r.text else []
                break
            except Exception as e:
                print(f"  [WARN] {url} offset={offset} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        if rows is None:
            break
        if not rows:
            break
        out.extend(rows)
        print(f"  [INFO] {url.split('/')[-1]} 第 {page} 页 offset={offset} 收到 {len(rows)} 条，累计 {len(out)} 条")
        if len(rows) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return out


def _safe_float(x, default=0.0):
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def main():
    parser = argparse.ArgumentParser(description="拉取 0x8dxd 盈利来源：activity + positions")
    parser.add_argument("--user", default=os.environ.get("TRACKER_USER_ADDRESS", DEFAULT_ADDRESS), help="用户地址")
    parser.add_argument("--output-dir", default="eda/01_etl/output/0x8dxd_full", help="输出目录")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--last-hours", type=float, default=None, help="额外输出最近 N 小时的汇总（便于与网页「最近24h」对比）")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    proxy = None if args.no_proxy else _detect_proxy()
    if proxy:
        print(f"使用代理: {proxy}")
    session = _make_session(proxy)
    user = args.user.strip()

    # 1) Activity 全量（含 TRADE / REDEEM / REWARD / MAKER_REBATE 等）
    print("拉取 Activity（成交+结算+奖励等）...")
    activity = _get_paginated(session, DATA_API_ACTIVITY, user)
    activity_path = out_dir / "activity_raw.jsonl"
    with open(activity_path, "w", encoding="utf-8") as f:
        for row in activity:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"已写 {activity_path}，共 {len(activity)} 条")
    if len(activity) >= 3500:
        print("  [提示] Activity 在 offset=3500 起 API 返回 400，当前仅含前 3500 条；若历史更多可能被截断。")

    # 2) Positions 全量（当前持仓：未实现 + 已实现 PnL）
    print("拉取 Positions（当前持仓）...")
    positions = _get_paginated(session, DATA_API_POSITIONS, user)
    positions_path = out_dir / "positions_raw.jsonl"
    with open(positions_path, "w", encoding="utf-8") as f:
        for row in positions:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"已写 {positions_path}，共 {len(positions)} 条")

    # 3) 按来源汇总
    # Activity: type -> 条数、usdcSize 之和。TRADE 若 API 只返回正数则按 side 取符号：BUY=-，SELL=+
    by_type = {}
    for row in activity:
        t = (row.get("type") or "UNKNOWN").strip()
        if t not in by_type:
            by_type[t] = {"count": 0, "usdcSize_sum": 0.0}
        by_type[t]["count"] += 1
        u = _safe_float(row.get("usdcSize"))
        # TRADE: 若 API 返回的 usdcSize 恒为正，则按 side 取符号（BUY=支出为负）
        if t == "TRADE" and u != 0:
            side = (row.get("side") or "").strip().upper()
            if side == "BUY" and u > 0:
                u = -u
            elif side == "SELL" and u < 0:
                u = -u
        by_type[t]["usdcSize_sum"] += u

    # Positions: 未实现 cashPnl、已实现 realizedPnl
    positions_cash_pnl = sum(_safe_float(p.get("cashPnl")) for p in positions)
    positions_realized_pnl = sum(_safe_float(p.get("realizedPnl")) for p in positions)

    # 汇总表
    summary_rows = []
    for t, v in sorted(by_type.items()):
        summary_rows.append({
            "source": f"activity_{t}",
            "type": t,
            "count": v["count"],
            "usdc_total": round(v["usdcSize_sum"], 4),
            "note": "TRADE=交易净现金流 REDEEM=结算兑付 REWARD/MAKER_REBATE=奖励",
        })
    summary_rows.append({
        "source": "positions_unrealized",
        "type": "POSITIONS_CASH_PNL",
        "count": len(positions),
        "usdc_total": round(positions_cash_pnl, 4),
        "note": "当前持仓未实现盈亏",
    })
    summary_rows.append({
        "source": "positions_realized",
        "type": "POSITIONS_REALIZED_PNL",
        "count": len(positions),
        "usdc_total": round(positions_realized_pnl, 4),
        "note": "持仓上已实现部分（如部分平仓）",
    })

    summary_path = out_dir / "pnl_sources_summary.csv"
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source", "type", "count", "usdc_total", "note"])
        w.writeheader()
        w.writerows(summary_rows)
    print(f"已写 {summary_path}")

    # 清洗 activity 为每笔现金流一行（便于按市场/类型分析）
    activity_cleaned_path = out_dir / "activity_cleaned.csv"
    act_rows = []
    for row in activity:
        u = _safe_float(row.get("usdcSize"))
        t = (row.get("type") or "UNKNOWN").strip()
        if t == "TRADE" and u != 0:
            side = (row.get("side") or "").strip().upper()
            if side == "BUY" and u > 0:
                u = -u
            elif side == "SELL" and u < 0:
                u = -u
        act_rows.append({
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
    if act_rows:
        with open(activity_cleaned_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=act_rows[0].keys())
            w.writeheader()
            w.writerows(act_rows)
        print(f"已写 {activity_cleaned_path}，共 {len(act_rows)} 条")

    # 4) 控制台简要汇总
    total_activity_usdc = sum(v["usdcSize_sum"] for v in by_type.values())
    print("\n--- 盈利来源汇总 (usdc) ---")
    for row in summary_rows:
        print(f"  {row['source']}: count={row['count']}, usdc_total={row['usdc_total']:.2f}  # {row['note']}")
    print(f"  Activity 合计: {total_activity_usdc:.2f}")
    print(f"  持仓未实现: {positions_cash_pnl:.2f}")
    print(f"  持仓已实现: {positions_realized_pnl:.2f}")
    total_sum = total_activity_usdc + positions_cash_pnl + positions_realized_pnl
    print(f"  总计(上述相加): {total_sum:.2f}  # 注：持仓已实现可能与 activity TRADE 有重叠，仅供参考")

    # 5) 最近 N 小时汇总（与网页「最近24h 净利润」对比用）
    if args.last_hours and activity:
        cutoff = NOW_TS - int(args.last_hours * 3600)
        activity_24h = [r for r in activity if _safe_float(r.get("timestamp"), default=0) >= cutoff]
        by_type_24h = {}
        for row in activity_24h:
            t = (row.get("type") or "UNKNOWN").strip()
            if t not in by_type_24h:
                by_type_24h[t] = {"count": 0, "usdcSize_sum": 0.0}
            by_type_24h[t]["count"] += 1
            u = _safe_float(row.get("usdcSize"))
            if t == "TRADE" and u != 0:
                side = (row.get("side") or "").strip().upper()
                if side == "BUY" and u > 0:
                    u = -u
                elif side == "SELL" and u < 0:
                    u = -u
            by_type_24h[t]["usdcSize_sum"] += u
        total_24h = sum(v["usdcSize_sum"] for v in by_type_24h.values())
        print(f"\n--- 最近 {args.last_hours} 小时 (仅对已拉取 activity 过滤，共 {len(activity_24h)} 条) ---")
        for t, v in sorted(by_type_24h.items()):
            print(f"  {t}: count={v['count']}, usdc={v['usdcSize_sum']:.2f}")
        print(f"  合计: {total_24h:.2f}  # 可与网页「最近24h 净利润」对比（网页可能只含 REDEEM 或口径不同）")


if __name__ == "__main__":
    main()
