#!/usr/bin/env python3
"""
验证两个假说：
1. 0x8dxd 是否在现货动量出现后才出手？（选择性入场）
2. 出手时 Binance vs Chainlink 是否有价差？（预言机延迟套利）

数据源：
  - trades_cleaned.csv
  - silver/binance book_ticker
  - silver/chainlink updates
"""

import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pandas as pd

DATA_DIR = Path("/vault/core/data/poly")
TRADES_CSV = Path("eda/01_etl/output/0x8dxd_full/trades_cleaned.csv")
OUT_DIR = Path("eda/03_analysis/output/0x8dxd_momentum")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Chainlink answer 精度
CL_DECIMALS = 1e8

# ── slug → coin 映射 ─────────────────────────────────────────────
COIN_MAP = {
    "btcusdt": "BTC/USD",
    "ethusdt": "ETH/USD",
    "solusdt": "SOL/USD",
    "xrpusdt": "XRP/USD",
}
BN_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


def slug_to_coin(slug, question):
    """从 slug/question 提取对应的 Binance symbol"""
    q = question.lower()
    if "bitcoin" in q or "btc" in q:
        return "BTCUSDT"
    elif "ethereum" in q or "eth" in q:
        return "ETHUSDT"
    elif "solana" in q or "sol" in q:
        return "SOLUSDT"
    elif "xrp" in q:
        return "XRPUSDT"
    return None


def parse_window_type(slug, question):
    """从 slug 解析窗口类型"""
    if "-5m-" in slug or "5m" in slug:
        return "5min"
    elif "-15m-" in slug or "15m" in slug:
        return "15min"
    elif "-1h-" in slug or "1hour" in slug:
        return "1hour"
    # 从 question 推断
    q = question.lower()
    if any(x in q for x in [":05", ":10", ":15", ":20", ":25", ":30",
                              ":35", ":40", ":45", ":50", ":55"]):
        # 有分钟范围的通常是 5min 或 15min
        # 尝试从时间跨度判断
        pass
    # 默认 1hour（从 question 中含 "AM ET" / "PM ET" 不带分钟范围的）
    return "1hour"


def parse_window_start_from_slug(slug):
    """从 slug 提取窗口开始时间戳（5min/15min 的 slug 尾部是 unix ts）"""
    m = re.search(r'-(\d{10,})$', slug)
    if m:
        return int(m.group(1))
    return None


# ── 1. 加载交易数据 ──────────────────────────────────────────────

def load_trades():
    markets = defaultdict(lambda: {
        "question": "", "slug": "", "category": "", "coin": None,
        "window_type": None, "window_start_ts": None,
        "trades": [], "first_ts": 1e18, "last_ts": 0,
        "buy_yes_cost": 0, "buy_yes_size": 0,
        "buy_no_cost": 0, "buy_no_size": 0,
    })
    with open(TRADES_CSV) as f:
        for r in csv.DictReader(f):
            cid = r["conditionId"]
            m = markets[cid]
            m["question"] = r["question"]
            m["slug"] = r["slug"]
            m["category"] = r.get("market_category", "")
            m["coin"] = slug_to_coin(r["slug"], r["question"])
            m["window_type"] = parse_window_type(r["slug"], r["question"])
            if m["window_start_ts"] is None:
                m["window_start_ts"] = parse_window_start_from_slug(r["slug"])

            ts = int(r["timestamp"])
            m["first_ts"] = min(m["first_ts"], ts)
            m["last_ts"] = max(m["last_ts"], ts)
            m["trades"].append({
                "ts": ts, "side": r["side"],
                "outcome": r["outcome"], "size": float(r["size"]),
                "price": float(r["price"]),
            })
            outcome = r["outcome"].lower()
            oi = "yes" if outcome in ("up", "yes") else "no"
            if r["side"] == "BUY":
                m[f"buy_{oi}_cost"] += float(r["size"]) * float(r["price"])
                m[f"buy_{oi}_size"] += float(r["size"])
    return dict(markets)


# ── 2. 加载 Binance book_ticker ──────────────────────────────────

def load_binance_prices():
    """加载 book_ticker，按 symbol 构建 (ts_ms, mid_price) 数组"""
    print("加载 Binance book_ticker...")
    # 只加载交易覆盖时段 Feb 12 hour=20 到 Feb 13 hour=09
    dfs = []
    for dt_str in ["2026-02-12", "2026-02-13"]:
        dt_path = DATA_DIR / f"lake/silver/binance/dt={dt_str}"
        if not dt_path.exists():
            continue
        for hour_dir in sorted(dt_path.iterdir()):
            hour = int(hour_dir.name.split("=")[1])
            # Feb 12: only need hour >= 20 (buffer before first trade)
            if dt_str == "2026-02-12" and hour < 20:
                continue
            # Feb 13: only need hour <= 9
            if dt_str == "2026-02-13" and hour > 9:
                continue
            bt_dir = hour_dir / "book_ticker"
            if not bt_dir.exists():
                # try book_ticker_001 etc
                for alt in hour_dir.iterdir():
                    if "book_ticker" in alt.name:
                        bt_dir = alt
                        break
                if not bt_dir.exists():
                    continue
            try:
                t = pq.read_table(bt_dir, columns=["local_receipt_ts_ms", "s", "b", "a"])
                dfs.append(t.to_pandas())
            except Exception as e:
                print(f"  跳过 {bt_dir}: {e}")

    if not dfs:
        print("  WARNING: 无 Binance 数据")
        return {}
    df = pd.concat(dfs, ignore_index=True)
    df["ts_ms"] = df["local_receipt_ts_ms"].astype(np.int64)
    df["mid"] = (df["b"].astype(float) + df["a"].astype(float)) / 2

    prices = {}
    for sym in BN_SYMBOLS:
        sub = df[df["s"] == sym].sort_values("ts_ms")
        if len(sub) > 0:
            prices[sym] = sub[["ts_ms", "mid"]].values  # numpy array
            print(f"  {sym}: {len(sub)} ticks, {sub['ts_ms'].min()} - {sub['ts_ms'].max()}")
    return prices


# ── 3. 加载 Chainlink ───────────────────────────────────────────

def load_chainlink_prices():
    """加载 Chainlink，按 feed 构建 (ts_ms, price) 数组"""
    print("加载 Chainlink...")
    dfs = []
    for dt_str in ["2026-02-12", "2026-02-13"]:
        dt_path = DATA_DIR / f"lake/silver/chainlink/dt={dt_str}"
        if not dt_path.exists():
            continue
        try:
            t = pq.read_table(dt_path, columns=["local_receipt_ts_ms", "feed", "answer", "updatedAt"])
            dfs.append(t.to_pandas())
        except Exception as e:
            print(f"  跳过 {dt_path}: {e}")

    if not dfs:
        print("  WARNING: 无 Chainlink 数据")
        return {}
    df = pd.concat(dfs, ignore_index=True)
    df["ts_ms"] = df["local_receipt_ts_ms"].astype(np.int64)
    df["price"] = df["answer"].astype(float) / CL_DECIMALS

    prices = {}
    for feed in ["BTC/USD", "ETH/USD", "SOL/USD"]:
        sub = df[df["feed"] == feed].sort_values("ts_ms")
        if len(sub) > 0:
            prices[feed] = sub[["ts_ms", "price"]].values
            print(f"  {feed}: {len(sub)} updates, {sub['ts_ms'].min()} - {sub['ts_ms'].max()}")
    return prices


# ── 4. 价格查询工具 ─────────────────────────────────────────────

def find_price_at(price_arr, ts_ms):
    """二分查找 ts_ms 对应的最近价格（向前取）"""
    if price_arr is None or len(price_arr) == 0:
        return None
    idx = np.searchsorted(price_arr[:, 0], ts_ms, side="right") - 1
    if idx < 0:
        return None
    return float(price_arr[idx, 1])


def compute_momentum(price_arr, ts_ms, lookback_sec):
    """计算 ts_ms 前 lookback_sec 秒的价格变化率"""
    p_now = find_price_at(price_arr, ts_ms)
    p_before = find_price_at(price_arr, ts_ms - lookback_sec * 1000)
    if p_now is None or p_before is None or p_before == 0:
        return None
    return (p_now - p_before) / p_before


# ── 5. 分析 1：动量与出手 ───────────────────────────────────────

def analyze_momentum(markets, bn_prices):
    print("\n" + "=" * 70)
    print("  分析 1：现货动量与出手选择性")
    print("=" * 70)

    # 按 coin + window_type 分组
    results = []
    for cid, m in markets.items():
        coin = m["coin"]
        if coin not in bn_prices:
            continue
        wtype = m["window_type"]
        first_ts_ms = int(m["first_ts"]) * 1000

        # 他在这个市场的方向偏好
        if m["buy_yes_cost"] + m["buy_no_cost"] == 0:
            continue
        direction_bias = (m["buy_yes_cost"] - m["buy_no_cost"]) / (m["buy_yes_cost"] + m["buy_no_cost"])
        # >0 偏 Yes/Up, <0 偏 No/Down

        # 入场前动量
        mom_1m = compute_momentum(bn_prices[coin], first_ts_ms, 60)
        mom_5m = compute_momentum(bn_prices[coin], first_ts_ms, 300)
        mom_15m = compute_momentum(bn_prices[coin], first_ts_ms, 900)

        # 入场时 Binance 价格
        bn_price = find_price_at(bn_prices[coin], first_ts_ms)

        # 入场距窗口开始的延迟
        ws = m["window_start_ts"]
        entry_delay = m["first_ts"] - ws if ws else None

        results.append({
            "conditionId": cid,
            "question": m["question"],
            "coin": coin,
            "window_type": wtype,
            "window_start_ts": ws,
            "first_trade_ts": m["first_ts"],
            "entry_delay_sec": entry_delay,
            "direction_bias": round(direction_bias, 4),
            "bias_label": "Up偏重" if direction_bias > 0.1 else ("Down偏重" if direction_bias < -0.1 else "均衡"),
            "mom_1m_pct": round(mom_1m * 100, 4) if mom_1m is not None else None,
            "mom_5m_pct": round(mom_5m * 100, 4) if mom_5m is not None else None,
            "mom_15m_pct": round(mom_15m * 100, 4) if mom_15m is not None else None,
            "bn_price_at_entry": round(bn_price, 2) if bn_price else None,
            "buy_yes_cost": round(m["buy_yes_cost"], 2),
            "buy_no_cost": round(m["buy_no_cost"], 2),
        })

    if not results:
        print("  无数据")
        return results

    df = pd.DataFrame(results)

    # ── 总体统计 ──
    for wtype in sorted(df["window_type"].dropna().unique()):
        sub = df[df["window_type"] == wtype]
        print(f"\n{'─'*70}")
        print(f"  窗口类型: {wtype} (n={len(sub)})")
        print(f"{'─'*70}")

        # 入场延迟
        delays = sub["entry_delay_sec"].dropna()
        if len(delays) > 0:
            print(f"  入场延迟 (距窗口开始):")
            print(f"    中位数: {delays.median():.0f}s, 均值: {delays.mean():.0f}s")
            print(f"    最快: {delays.min():.0f}s, 最慢: {delays.max():.0f}s")
            # 分桶
            for bucket, label in [(10, "≤10s"), (30, "≤30s"), (60, "≤60s"), (120, "≤120s")]:
                n = (delays <= bucket).sum()
                print(f"    {label}: {n} ({n/len(delays)*100:.0f}%)")

    # ── 动量 vs 方向偏好 ──
    print(f"\n{'─'*70}")
    print(f"  动量与方向偏好的关系")
    print(f"{'─'*70}")

    for lookback, col in [("1min", "mom_1m_pct"), ("5min", "mom_5m_pct"), ("15min", "mom_15m_pct")]:
        valid = df.dropna(subset=[col, "direction_bias"])
        if len(valid) < 5:
            continue
        # 动量 > 0 → 现货涨；direction_bias > 0 → 偏 Up
        # 如果他看动量下注，两者应该同向
        corr = valid[col].corr(valid["direction_bias"])

        # 按象限统计
        momentum_up = valid[valid[col] > 0]
        momentum_down = valid[valid[col] < 0]
        bias_agree_up = len(momentum_up[momentum_up["direction_bias"] > 0.1])
        bias_agree_down = len(momentum_down[momentum_down["direction_bias"] < -0.1])
        bias_disagree = len(valid) - bias_agree_up - bias_agree_down - len(valid[(valid[col].abs() < 0.001) | (valid["direction_bias"].abs() <= 0.1)])

        n_agree = bias_agree_up + bias_agree_down
        n_total = len(valid)

        print(f"\n  {lookback} 动量 (n={n_total}):")
        print(f"    相关系数: {corr:.4f}")
        print(f"    动量方向与下注方向一致: {n_agree} ({n_agree/n_total*100:.1f}%)")
        print(f"      涨势→偏Up: {bias_agree_up}, 跌势→偏Down: {bias_agree_down}")

        # 按动量大小分桶
        print(f"    按动量强度分桶:")
        for threshold in [0.05, 0.1, 0.2, 0.5]:
            strong_up = valid[valid[col] > threshold]
            strong_down = valid[valid[col] < -threshold]
            if len(strong_up) > 0:
                up_bias_up = len(strong_up[strong_up["direction_bias"] > 0.1])
                print(f"      动量 > +{threshold}%: {len(strong_up)} 个, 偏Up: {up_bias_up} ({up_bias_up/len(strong_up)*100:.0f}%)")
            if len(strong_down) > 0:
                dn_bias_dn = len(strong_down[strong_down["direction_bias"] < -0.1])
                print(f"      动量 < -{threshold}%: {len(strong_down)} 个, 偏Down: {dn_bias_dn} ({dn_bias_dn/len(strong_down)*100:.0f}%)")

    # ── 5min 窗口覆盖率 ──
    print(f"\n{'─'*70}")
    print(f"  5min 窗口选择性（他跳过了多少？）")
    print(f"{'─'*70}")
    fivemin = df[df["window_type"] == "5min"]
    for coin in sorted(fivemin["coin"].dropna().unique()):
        coin_sub = fivemin[fivemin["coin"] == coin]
        ws_list = sorted(coin_sub["window_start_ts"].dropna().astype(int).tolist())
        if len(ws_list) < 2:
            continue
        # 计算理论上有多少 5min 窗口
        ws_min, ws_max = min(ws_list), max(ws_list)
        total_possible = (ws_max - ws_min) // 300 + 1
        traded = len(set(ws_list))
        skipped = total_possible - traded
        print(f"  {coin}: 理论窗口 {total_possible}, 出手 {traded} ({traded/total_possible*100:.0f}%), 跳过 {skipped} ({skipped/total_possible*100:.0f}%)")

    # 保存
    csv_path = OUT_DIR / "momentum_analysis.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n  详细数据已保存: {csv_path}")
    return results


# ── 6. 分析 2：Binance vs Chainlink 价差 ────────────────────────

def analyze_oracle_lag(markets, bn_prices, cl_prices):
    print("\n" + "=" * 70)
    print("  分析 2：出手时 Binance vs Chainlink 价差")
    print("=" * 70)

    BN_TO_CL = {"BTCUSDT": "BTC/USD", "ETHUSDT": "ETH/USD", "SOLUSDT": "SOL/USD"}
    results = []

    for cid, m in markets.items():
        coin = m["coin"]
        if coin not in BN_TO_CL:
            continue
        cl_feed = BN_TO_CL[coin]
        if coin not in bn_prices or cl_feed not in cl_prices:
            continue

        first_ts_ms = int(m["first_ts"]) * 1000
        bn_price = find_price_at(bn_prices[coin], first_ts_ms)
        cl_price = find_price_at(cl_prices[cl_feed], first_ts_ms)

        if bn_price is None or cl_price is None or bn_price == 0:
            continue

        spread_pct = (bn_price - cl_price) / bn_price * 100
        spread_abs = bn_price - cl_price

        # 方向偏好
        total_cost = m["buy_yes_cost"] + m["buy_no_cost"]
        if total_cost == 0:
            continue
        direction_bias = (m["buy_yes_cost"] - m["buy_no_cost"]) / total_cost

        # 动量 (Binance 1min)
        mom_1m = compute_momentum(bn_prices[coin], first_ts_ms, 60)

        results.append({
            "conditionId": cid,
            "question": m["question"],
            "coin": coin,
            "window_type": m["window_type"],
            "first_trade_ts": m["first_ts"],
            "bn_price": round(bn_price, 4),
            "cl_price": round(cl_price, 4),
            "spread_abs": round(spread_abs, 4),
            "spread_pct": round(spread_pct, 6),
            "spread_direction": "BN>CL" if spread_abs > 0 else "BN<CL",
            "direction_bias": round(direction_bias, 4),
            "bias_label": "Up偏重" if direction_bias > 0.1 else ("Down偏重" if direction_bias < -0.1 else "均衡"),
            "mom_1m_pct": round(mom_1m * 100, 4) if mom_1m is not None else None,
        })

    if not results:
        print("  无数据")
        return

    df = pd.DataFrame(results)

    # ── 总体价差统计 ──
    print(f"\n  总样本: {len(df)} 个市场")
    print(f"\n  Binance vs Chainlink 价差统计:")
    print(f"    均值:   {df['spread_pct'].mean():>8.4f}%  (${df['spread_abs'].mean():>8.2f})")
    print(f"    中位数: {df['spread_pct'].median():>8.4f}%  (${df['spread_abs'].median():>8.2f})")
    print(f"    标准差: {df['spread_pct'].std():>8.4f}%")
    print(f"    最大值: {df['spread_pct'].max():>8.4f}%  (${df['spread_abs'].max():>8.2f})")
    print(f"    最小值: {df['spread_pct'].min():>8.4f}%  (${df['spread_abs'].min():>8.2f})")
    print(f"    BN>CL:  {(df['spread_pct']>0).sum()} ({(df['spread_pct']>0).mean()*100:.1f}%)")
    print(f"    BN<CL:  {(df['spread_pct']<0).sum()} ({(df['spread_pct']<0).mean()*100:.1f}%)")

    # ── 按币种 ──
    for coin in sorted(df["coin"].unique()):
        sub = df[df["coin"] == coin]
        cl_feed = BN_TO_CL[coin]
        print(f"\n  {coin} (n={len(sub)}):")
        print(f"    价差均值: {sub['spread_pct'].mean():.4f}%, 中位数: {sub['spread_pct'].median():.4f}%")
        print(f"    |价差|均值: {sub['spread_pct'].abs().mean():.4f}%")

    # ── 关键问题：价差方向是否与他的下注方向一致？──
    print(f"\n{'─'*70}")
    print(f"  价差方向 vs 下注方向 (核心问题)")
    print(f"{'─'*70}")
    print(f"  假说: 如果 Chainlink 滞后 Binance，且他看到 BN 已涨但 CL 还没更新，")
    print(f"        他会偏 Up (因为知道链上 oracle 还没反映真实价格)")
    print(f"  即: BN>CL (正价差) → 偏 Up, BN<CL (负价差) → 偏 Down\n")

    # BN > CL (Binance 更高 → 现货已涨，oracle 滞后)
    bn_higher = df[df["spread_pct"] > 0.01]
    if len(bn_higher) > 0:
        bias_up = len(bn_higher[bn_higher["direction_bias"] > 0.1])
        print(f"  BN > CL 时 (n={len(bn_higher)}): 偏Up = {bias_up} ({bias_up/len(bn_higher)*100:.0f}%)")

    bn_lower = df[df["spread_pct"] < -0.01]
    if len(bn_lower) > 0:
        bias_down = len(bn_lower[bn_lower["direction_bias"] < -0.1])
        print(f"  BN < CL 时 (n={len(bn_lower)}): 偏Down = {bias_down} ({bias_down/len(bn_lower)*100:.0f}%)")

    # 相关性
    corr = df["spread_pct"].corr(df["direction_bias"])
    print(f"\n  价差 vs 方向偏好 相关系数: {corr:.4f}")

    # ── 价差大小 vs 方向一致性 ──
    print(f"\n  按|价差|大小分桶:")
    for threshold in [0.01, 0.02, 0.05, 0.1]:
        big_spread = df[df["spread_pct"].abs() > threshold]
        if len(big_spread) < 3:
            continue
        agree = len(big_spread[
            ((big_spread["spread_pct"] > 0) & (big_spread["direction_bias"] > 0.1)) |
            ((big_spread["spread_pct"] < 0) & (big_spread["direction_bias"] < -0.1))
        ])
        print(f"    |价差| > {threshold}%: n={len(big_spread)}, 方向一致: {agree} ({agree/len(big_spread)*100:.0f}%)")

    # 保存
    csv_path = OUT_DIR / "oracle_lag_analysis.csv"
    df.to_csv(csv_path, index=False)
    print(f"\n  详细数据已保存: {csv_path}")


# ── 主流程 ───────────────────────────────────────────────────────

def main():
    markets = load_trades()
    print(f"加载 {len(markets)} 个市场")

    bn_prices = load_binance_prices()
    cl_prices = load_chainlink_prices()

    analyze_momentum(markets, bn_prices)
    analyze_oracle_lag(markets, bn_prices, cl_prices)


if __name__ == "__main__":
    main()
