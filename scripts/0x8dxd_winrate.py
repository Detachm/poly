#!/usr/bin/env python3
"""
0x8dxd 方向胜率分析工具（可复用）

数据源:
  1. 实时 tracker: /vault/core/data/poly/raw/tracker_0x8dxd/dt=YYYY-MM-DD/
  2. 历史导出: eda/01_etl/output/0x8dxd_full/trades_cleaned.csv

用法:
    python scripts/0x8dxd_winrate.py                           # 所有可用数据
    python scripts/0x8dxd_winrate.py --days 2                  # 最近 2 天
    python scripts/0x8dxd_winrate.py --start 2026-03-11        # 从指定日期起
    python scripts/0x8dxd_winrate.py --start 2026-03-11 --end 2026-03-12
    python scripts/0x8dxd_winrate.py --asset btc               # 只看 BTC
    python scripts/0x8dxd_winrate.py --window 5m               # 只看 5min
"""

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

TRACKER_DIR = Path("/vault/core/data/poly/raw/tracker_0x8dxd")

# ── slug 解析 ──────────────────────────────────────────────────

ASSET_PATTERNS = [
    (re.compile(r"^btc-|^bitcoin-", re.I), "BTC"),
    (re.compile(r"^eth-|^ethereum-", re.I), "ETH"),
    (re.compile(r"^sol-|^solana-", re.I), "SOL"),
    (re.compile(r"^xrp-", re.I), "XRP"),
    (re.compile(r"^doge-|^dogecoin-", re.I), "DOGE"),
]

WINDOW_PATTERNS = [
    (re.compile(r"updown-5m-"), "5m"),
    (re.compile(r"updown-15m-"), "15m"),
    (re.compile(r"up-or-down-"), "1h"),    # hourly 格式
]


def parse_slug(slug: str) -> dict:
    """从 slug 解析资产和窗口类型。"""
    asset = None
    for pat, name in ASSET_PATTERNS:
        if pat.search(slug):
            asset = name
            break

    window = None
    for pat, name in WINDOW_PATTERNS:
        if pat.search(slug):
            window = name
            break

    is_crypto = asset is not None and window is not None
    return {"asset": asset, "window": window, "is_crypto": is_crypto}


def extract_window_ts(slug: str) -> int | None:
    """从 slug 提取窗口 unix timestamp（仅 updown-Xm-NNNNN 格式）。"""
    m = re.search(r"updown-\d+m-(\d+)$", slug)
    return int(m.group(1)) if m else None


def extract_window_duration(slug: str) -> int:
    """从 slug 提取窗口时长（秒）。"""
    m = re.search(r"updown-(\d+)m-", slug)
    if m:
        return int(m.group(1)) * 60
    if "up-or-down-" in slug:
        return 3600  # 1h
    return 300  # default 5m


# ── Binance klines 加载 ──────────────────────────────────────

HIST_DIR = Path("/vault/core/data/poly/historical/binance")
KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "n_trades",
    "taker_buy_vol", "taker_buy_quote_vol", "ignore",
]

ASSET_TO_SYMBOL = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}

_klines_cache = {}  # symbol -> {ts_s: (open, close)}


def _load_klines(symbol: str) -> dict:
    """加载 klines 到内存，返回 {ts_s: (open, close)}。"""
    if symbol in _klines_cache:
        return _klines_cache[symbol]

    sym_dir = HIST_DIR / symbol.lower() / "klines_5m"
    if not sym_dir.exists():
        _klines_cache[symbol] = {}
        return {}

    import glob as globmod
    files = sorted(globmod.glob(str(sym_dir / f"{symbol}-5m-*.csv")))
    if not files:
        _klines_cache[symbol] = {}
        return {}

    data = {}
    for f in files:
        try:
            import csv
            with open(f) as fh:
                reader = csv.reader(fh)
                for row in reader:
                    if len(row) < 5:
                        continue
                    try:
                        ot = int(float(row[0]))
                        # 统一到秒级
                        if ot > 1e14:
                            ot = ot // 1000
                        ts_s = ot // 1000
                        o = float(row[1])
                        c = float(row[4])
                        data[ts_s] = (o, c)
                    except (ValueError, IndexError):
                        continue
        except Exception:
            continue

    _klines_cache[symbol] = data
    return data


def determine_winner_by_price(asset: str, window_ts: int, window: str) -> str | None:
    """用 Binance klines 判定结算方向。返回 'yes'(Up)/'no'(Down)/None。"""
    symbol = ASSET_TO_SYMBOL.get(asset)
    if not symbol:
        return None

    klines = _load_klines(symbol)
    if not klines:
        return None

    if window == "5m":
        # 直接找这个 5min bar
        bar = klines.get(window_ts)
        if bar:
            return "yes" if bar[1] > bar[0] else "no"
    elif window == "15m":
        # 需要第一个 bar 的 open 和第三个 bar 的 close
        bar0 = klines.get(window_ts)
        bar2 = klines.get(window_ts + 600)
        if bar0 and bar2:
            return "yes" if bar2[1] > bar0[0] else "no"
    elif window == "1h":
        # 第一个 bar 的 open 和最后一个 bar 的 close
        bar0 = klines.get(window_ts)
        bar11 = klines.get(window_ts + 3300)  # 第 12 个 5min bar
        if bar0 and bar11:
            return "yes" if bar11[1] > bar0[0] else "no"

    return None


# ── 数据加载 ──────────────────────────────────────────────────

def load_tracker_trades(dates: list[str]) -> list[dict]:
    """从 tracker 目录加载指定日期的 trades。"""
    trades = []
    for dt in dates:
        dt_dir = TRACKER_DIR / f"dt={dt}"
        if not dt_dir.exists():
            continue
        for hour_dir in sorted(dt_dir.iterdir()):
            trades_file = hour_dir / "trades.jsonl"
            if not trades_file.exists():
                continue
            with open(trades_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        t = json.loads(line)
                        trades.append(t)
                    except json.JSONDecodeError:
                        continue
    return trades


def load_all_settlements(dates: list[str]) -> dict:
    """扫描所有 positions 快照，找出已结算市场的赢家。
    返回 {conditionId: 'yes'/'no'}。
    """
    settlements = {}
    for dt in dates:
        dt_dir = TRACKER_DIR / f"dt={dt}"
        if not dt_dir.exists():
            continue
        for hour_dir in sorted(dt_dir.iterdir()):
            pos_file = hour_dir / "positions.jsonl"
            if not pos_file.exists() or pos_file.stat().st_size == 0:
                continue
            with open(pos_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("type") != "positions_snapshot":
                        continue
                    pos_list = record.get("positions", "[]")
                    if isinstance(pos_list, str):
                        try:
                            pos_list = json.loads(pos_list)
                        except json.JSONDecodeError:
                            continue
                    for p in pos_list:
                        cid = p.get("conditionId", "")
                        if cid in settlements:
                            continue
                        cp = float(p.get("curPrice", 0.5))
                        outcome = p.get("outcome", "").lower()
                        side = "yes" if outcome in ("up", "yes") else "no"
                        if cp > 0.90:
                            settlements[cid] = side
                        elif cp < 0.10:
                            settlements[cid] = "no" if side == "yes" else "yes"
    return settlements


# ── 聚合与分析 ────────────────────────────────────────────────

def aggregate_trades(trades: list[dict]) -> dict:
    """按 conditionId 聚合 trades，计算各方向的净持仓。"""
    markets = {}
    for t in trades:
        cid = t.get("conditionId", "")
        if not cid:
            continue

        slug = t.get("slug", "")
        info = parse_slug(slug)
        if not info["is_crypto"]:
            continue

        if cid not in markets:
            markets[cid] = {
                "slug": slug,
                "title": t.get("title", ""),
                "asset": info["asset"],
                "window": info["window"],
                "window_ts": extract_window_ts(slug),
                "buy_yes_size": 0.0, "buy_yes_cost": 0.0,
                "buy_no_size": 0.0, "buy_no_cost": 0.0,
                "sell_yes_size": 0.0, "sell_yes_cost": 0.0,
                "sell_no_size": 0.0, "sell_no_cost": 0.0,
                "first_ts": t.get("timestamp", 0),
                "last_ts": t.get("timestamp", 0),
                "n_trades": 0,
            }

        m = markets[cid]
        m["n_trades"] += 1
        ts = t.get("timestamp", 0)
        if ts < m["first_ts"]:
            m["first_ts"] = ts
        if ts > m["last_ts"]:
            m["last_ts"] = ts

        outcome = t.get("outcome", "").lower()
        oi = str(t.get("outcomeIndex", "0"))
        side_key = "yes" if outcome in ("up", "yes") or oi == "0" else "no"

        size = float(t.get("size", 0))
        price = float(t.get("price", 0))
        action = t.get("side", "BUY").upper()

        if action == "BUY":
            m[f"buy_{side_key}_size"] += size
            m[f"buy_{side_key}_cost"] += size * price
        else:
            m[f"sell_{side_key}_size"] += size
            m[f"sell_{side_key}_cost"] += size * price

    return markets


def determine_winner(cid: str, settlements: dict,
                     asset: str = None, window_ts: int = None,
                     window: str = None) -> str | None:
    """判断赢家。优先 positions，fallback 到 Binance 价格。"""
    # 方法 1: positions 快照
    if cid in settlements:
        return settlements[cid]
    # 方法 2: Binance klines
    if asset and window_ts and window:
        return determine_winner_by_price(asset, window_ts, window)
    return None


def compute_direction(m: dict) -> tuple:
    """计算方向和净方向仓位。返回 (direction, directional_size, total_cost)。
    direction: 'yes'(Up) / 'no'(Down) / None(完全对冲)
    """
    net_yes = m["buy_yes_size"] - m["sell_yes_size"]
    net_no = m["buy_no_size"] - m["sell_no_size"]

    if net_yes > net_no:
        direction = "yes"
        directional_size = net_yes - max(net_no, 0)
        avg_price = m["buy_yes_cost"] / m["buy_yes_size"] if m["buy_yes_size"] > 0 else 0
    elif net_no > net_yes:
        direction = "no"
        directional_size = net_no - max(net_yes, 0)
        avg_price = m["buy_no_cost"] / m["buy_no_size"] if m["buy_no_size"] > 0 else 0
    else:
        return None, 0, 0

    total_cost = (m["buy_yes_cost"] + m["buy_no_cost"]
                  - m["sell_yes_cost"] - m["sell_no_cost"])

    return direction, directional_size, total_cost


def analyze(markets: dict, settlements: dict,
            asset_filter: str = None, window_filter: str = None) -> dict:
    """分析所有市场的方向胜率。"""
    results = []

    for cid, m in markets.items():
        # 过滤
        if asset_filter and m["asset"] != asset_filter.upper():
            continue
        if window_filter and m["window"] != window_filter:
            continue

        direction, dir_size, total_cost = compute_direction(m)
        if direction is None or dir_size < 1:
            continue  # 完全对冲或极小仓位

        winner = determine_winner(cid, settlements,
                                  asset=m["asset"],
                                  window_ts=m["window_ts"],
                                  window=m["window"])
        settled = winner is not None

        r = {
            "cid": cid,
            "slug": m["slug"],
            "title": m["title"],
            "asset": m["asset"],
            "window": m["window"],
            "window_ts": m["window_ts"],
            "direction": "Up" if direction == "yes" else "Down",
            "directional_size": dir_size,
            "total_cost": total_cost,
            "n_trades": m["n_trades"],
            "settled": settled,
            "winner": None,
            "won": None,
        }

        if settled:
            winner_label = "Up" if winner == "yes" else "Down"
            r["winner"] = winner_label
            r["won"] = direction == winner

        results.append(r)

    # 汇总
    settled = [r for r in results if r["settled"]]
    unsettled = [r for r in results if not r["settled"]]
    wins = [r for r in settled if r["won"]]
    losses = [r for r in settled if not r["won"]]

    # 按资产分组
    by_asset = defaultdict(lambda: {"win": 0, "lose": 0, "unsettled": 0})
    for r in results:
        a = r["asset"]
        if r["settled"]:
            if r["won"]:
                by_asset[a]["win"] += 1
            else:
                by_asset[a]["lose"] += 1
        else:
            by_asset[a]["unsettled"] += 1

    # 按窗口分组
    by_window = defaultdict(lambda: {"win": 0, "lose": 0, "unsettled": 0})
    for r in results:
        w = r["window"]
        if r["settled"]:
            if r["won"]:
                by_window[w]["win"] += 1
            else:
                by_window[w]["lose"] += 1
        else:
            by_window[w]["unsettled"] += 1

    # 资金加权胜率
    win_cost = sum(r["total_cost"] for r in wins)
    lose_cost = sum(r["total_cost"] for r in losses)
    total_settled_cost = win_cost + lose_cost

    # 按日分组（用 window_ts 推断日期）
    by_date = defaultdict(lambda: {"win": 0, "lose": 0, "cost_win": 0, "cost_lose": 0})
    for r in settled:
        if r["window_ts"]:
            dt = datetime.fromtimestamp(r["window_ts"], tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            continue
        if r["won"]:
            by_date[dt]["win"] += 1
            by_date[dt]["cost_win"] += r["total_cost"]
        else:
            by_date[dt]["lose"] += 1
            by_date[dt]["cost_lose"] += r["total_cost"]

    return {
        "total_markets": len(results),
        "settled": len(settled),
        "unsettled": len(unsettled),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(settled) * 100 if settled else 0,
        "win_rate_weighted": win_cost / total_settled_cost * 100 if total_settled_cost > 0 else 0,
        "by_asset": dict(by_asset),
        "by_window": dict(by_window),
        "by_date": dict(by_date),
        "details": results,
    }


# ── 输出 ──────────────────────────────────────────────────────

def print_report(result: dict, title: str = ""):
    """打印分析报告。"""
    if title:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")

    s = result
    print(f"\n  总市场数: {s['total_markets']}  (已结算 {s['settled']}, 未结算 {s['unsettled']})")

    if s["settled"] == 0:
        print("  无已结算市场")
        return

    print(f"  方向胜率: {s['win_rate']:.1f}%  ({s['wins']}W / {s['losses']}L)")
    print(f"  资金加权胜率: {s['win_rate_weighted']:.1f}%")

    # 按资产
    print(f"\n  {'资产':>6} | {'胜':>4} | {'负':>4} | {'胜率':>6} | {'未结算':>4}")
    print(f"  {'-'*6}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*4}")
    for asset in sorted(s["by_asset"].keys()):
        d = s["by_asset"][asset]
        total = d["win"] + d["lose"]
        wr = d["win"] / total * 100 if total > 0 else 0
        print(f"  {asset:>6} | {d['win']:>4} | {d['lose']:>4} | {wr:>5.1f}% | {d['unsettled']:>4}")

    # 按窗口
    print(f"\n  {'窗口':>6} | {'胜':>4} | {'负':>4} | {'胜率':>6} | {'未结算':>4}")
    print(f"  {'-'*6}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*4}")
    for window in sorted(s["by_window"].keys()):
        d = s["by_window"][window]
        total = d["win"] + d["lose"]
        wr = d["win"] / total * 100 if total > 0 else 0
        print(f"  {window:>6} | {d['win']:>4} | {d['lose']:>4} | {wr:>5.1f}% | {d['unsettled']:>4}")

    # 分日统计
    if s.get("by_date"):
        print(f"\n  {'日期':>12} | {'胜':>4} | {'负':>4} | {'胜率':>6} | {'资金胜率':>6}")
        print(f"  {'-'*12}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*6}")
        for dt in sorted(s["by_date"].keys()):
            d = s["by_date"][dt]
            total = d["win"] + d["lose"]
            wr = d["win"] / total * 100 if total > 0 else 0
            tc = d["cost_win"] + d["cost_lose"]
            wrc = d["cost_win"] / tc * 100 if tc > 0 else 0
            print(f"  {dt:>12} | {d['win']:>4} | {d['lose']:>4} | {wr:>5.1f}% | {wrc:>5.1f}%")

    # 最近的市场明细（最后 10 个已结算）
    settled_details = [r for r in s["details"] if r["settled"]]
    settled_details.sort(key=lambda x: x.get("window_ts") or 0, reverse=True)

    if settled_details:
        print(f"\n  最近已结算市场 (最新 10 个):")
        print(f"  {'结果':>4} | {'方向':>4} | {'赢家':>4} | {'仓位':>8} | 市场")
        print(f"  {'-'*4}-+-{'-'*4}-+-{'-'*4}-+-{'-'*8}-+{'-'*30}")
        for r in settled_details[:10]:
            result_str = " W " if r["won"] else " L "
            print(f"  {result_str:>4} | {r['direction']:>4} | {r['winner']:>4} | "
                  f"${r['total_cost']:>7.1f} | {r['title'][:50]}")

    print()


# ── CLI ───────────────────────────────────────────────────────

def get_available_dates() -> list[str]:
    """获取所有可用的 tracker 日期。"""
    dates = []
    if TRACKER_DIR.exists():
        for d in sorted(TRACKER_DIR.iterdir()):
            if d.name.startswith("dt="):
                dates.append(d.name[3:])
    return dates


def main():
    parser = argparse.ArgumentParser(description="0x8dxd 方向胜率分析")
    parser.add_argument("--start", default="", help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default="", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=0, help="最近 N 天")
    parser.add_argument("--asset", default="", help="过滤资产 (btc/eth/sol)")
    parser.add_argument("--window", default="", help="过滤窗口 (5m/15m/1h)")
    parser.add_argument("--json", action="store_true", help="输出 JSON")
    args = parser.parse_args()

    # 确定日期范围
    all_dates = get_available_dates()
    if not all_dates:
        print("未找到 tracker 数据")
        return

    if args.days > 0:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start = (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime("%Y-%m-%d")
        dates = [d for d in all_dates if start <= d <= today]
    elif args.start:
        end = args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dates = [d for d in all_dates if args.start <= d <= end]
    else:
        dates = all_dates

    if not dates:
        print(f"指定日期范围内无数据 (可用: {all_dates[0]} ~ {all_dates[-1]})")
        return

    print(f"加载数据: {dates[0]} ~ {dates[-1]} ({len(dates)} 天)")

    # 加载数据
    trades = load_tracker_trades(dates)
    print(f"  trades: {len(trades)} 条")

    # 扫描所有 positions 快照获取结算结果
    settlements = load_all_settlements(all_dates)
    print(f"  positions 结算: {len(settlements)} 个市场")

    # 聚合
    markets = aggregate_trades(trades)
    print(f"  crypto 市场: {len(markets)} 个")

    # 加载 Binance klines (用于 positions 覆盖不到的市场)
    needed_assets = set(m["asset"] for m in markets.values() if m["asset"])
    for asset in needed_assets:
        sym = ASSET_TO_SYMBOL.get(asset)
        if sym:
            kl = _load_klines(sym)
            print(f"  {sym} klines: {len(kl)} bars")

    # 分析
    result = analyze(markets, settlements,
                     asset_filter=args.asset or None,
                     window_filter=args.window or None)

    if args.json:
        # JSON 输出（去掉 details）
        out = {k: v for k, v in result.items() if k != "details"}
        print(json.dumps(out, indent=2, ensure_ascii=False))
    else:
        title = "0x8dxd Crypto 方向胜率"
        if args.asset:
            title += f" ({args.asset.upper()})"
        if args.window:
            title += f" ({args.window})"
        title += f" [{dates[0]} ~ {dates[-1]}]"
        print_report(result, title)


if __name__ == "__main__":
    main()
