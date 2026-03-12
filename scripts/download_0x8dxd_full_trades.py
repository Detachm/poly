#!/usr/bin/env python3
"""
临时脚本：下载 0x8dxd 全量交易记录并清洗，用于按市场类型分析胜率与交易行为。

- 分页拉取 Polymarket Data API /trades?user=0x8dxd
- 去重、类型规范化、按 title/slug 推断市场种类（market_category）
- 输出：raw 全量 JSONL + 清洗后 CSV（便于按 conditionId/市场类型做胜率与行为分析）

用法:
  python scripts/download_0x8dxd_full_trades.py [--user 0x8dxd] [--output-dir eda/01_etl/output/0x8dxd_full]
  python scripts/download_0x8dxd_full_trades.py --count-only   # 仅探测 Trades 单次最多能拉多少条，不写文件
  HTTPS_PROXY=http://127.0.0.1:7897 python scripts/download_0x8dxd_full_trades.py
"""

import argparse
import csv
import json
import os
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests

DATA_API_TRADES = "https://data-api.polymarket.com/trades"
# 0x8dxd 完整地址（Polymarket 链上）
DEFAULT_ADDRESS = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"
# 请求超时（秒）：连接 / 读取（代理下读取可能较慢）
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 90
# 单次请求失败后的重试次数与间隔（秒）
MAX_RETRIES = 3
RETRY_DELAY = 5
# 用于「不同种类市场」的简单标签：按 title/slug 关键词匹配
MARKET_CATEGORY_KEYWORDS = [
    ("crypto", ["bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "ripple", "crypto", "usdc"]),
    ("politics", ["trump", "biden", "election", "president", "congress", "vote", "senate"]),
    ("sports", ["nba", "nfl", "super bowl", "world series", "championship", "playoff"]),
    ("macro", ["fed", "rate", "inflation", "gdp", "jobs report", "treasury"]),
]


def _make_session(proxy: str | None):
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": "Poly-FullDownload/1.0"})
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _detect_proxy():
    for k in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]:
        v = os.environ.get(k)
        if v:
            return v
    return None


def fetch_all_trades(
    user_address: str, session: requests.Session, limit: int = 1000, verbose: bool = True
) -> tuple[list[dict], str]:
    """分页拉取该用户全量成交，去重后返回 (列表, 停止原因)。"""
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
                    print(f"  [WARN] 请求 offset={offset} 失败 (尝试 {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    if verbose:
                        print(f"  [INFO] {RETRY_DELAY}s 后重试...")
                    time.sleep(RETRY_DELAY)
        if rows is None:
            stop_reason = f"error_at_offset_{offset}"
            break
        if not rows:
            if verbose:
                print(f"  [INFO] offset={offset} 返回空，结束分页")
            stop_reason = "empty_page"
            break
        for t in rows:
            key = (
                t.get("timestamp"),
                t.get("asset"),
                t.get("side"),
                t.get("size"),
                t.get("price"),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(t)
        if verbose:
            print(f"  [INFO] 第 {page} 页 offset={offset} 收到 {len(rows)} 条，累计去重后 {len(out)} 条")
        if len(rows) < limit:
            stop_reason = "last_page"
            break
        offset += limit
        time.sleep(0.3)
    return out, stop_reason


def infer_market_category(title: str, slug: str) -> str:
    """根据 title/slug 推断市场种类，用于按类型分析胜率与行为。"""
    text = f" {title or ''} {slug or ''} ".lower()
    for category, keywords in MARKET_CATEGORY_KEYWORDS:
        if any(kw in text for kw in keywords):
            return category
    return "other"


def clean_row(t: dict) -> dict:
    """单条 API 记录转清洗后扁平行：类型规范 + market_category。"""
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
    return {
        "timestamp": ts,
        "datetime_utc": datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else "",
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


def main():
    parser = argparse.ArgumentParser(description="下载 0x8dxd 全量交易并清洗，用于胜率/行为分析")
    parser.add_argument("--user", default=os.environ.get("TRACKER_USER_ADDRESS", DEFAULT_ADDRESS), help="用户地址")
    parser.add_argument("--output-dir", default="eda/01_etl/output/0x8dxd_full", help="输出目录")
    parser.add_argument("--no-proxy", action="store_true", help="不使用代理")
    parser.add_argument("--count-only", action="store_true", help="仅探测单次最多能拉多少条，不写文件")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    proxy = None if args.no_proxy else _detect_proxy()
    if proxy:
        print(f"使用代理: {proxy}")
    session = _make_session(proxy)

    user = args.user.strip()
    print(f"开始拉取用户 {user} 全量成交...")
    trades, stop_reason = fetch_all_trades(user, session, verbose=not args.count_only)
    n = len(trades)
    print(f"Trades 单次最多拿到: {n} 条  |  停止原因: {stop_reason}")
    if args.count_only:
        return

    print(f"共获取 {n} 条去重后成交")

    # 1) 原始 JSONL（完整 API 返回字段）
    raw_path = out_dir / "trades_raw.jsonl"
    with open(raw_path, "w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    print(f"已写 raw: {raw_path}")

    # 2) 清洗后 CSV（便于按 market_category / conditionId 分析胜率与行为）
    cleaned = [clean_row(t) for t in trades]
    if not cleaned:
        print("无清洗后数据，跳过 CSV")
        return
    fieldnames = list(cleaned[0].keys())
    csv_path = out_dir / "trades_cleaned.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(cleaned)
    print(f"已写清洗: {csv_path}")

    # 简单统计：按 market_category 条数
    cats = Counter(r["market_category"] for r in cleaned)
    print("按市场类型条数:", dict(cats))


if __name__ == "__main__":
    main()
