#!/usr/bin/env python3
"""
market_plot.py: 单市场分析图 (1×3) + 全市场结算 PnL 计算

用法:
  # 出图（单个市场）
  python eda/02_visual/scripts/market_plot.py plot --condition-id 0xabc...
  # 算所有市场 PnL（已结算）
  python eda/02_visual/scripts/market_plot.py pnl --top-n 50 --output pnl.csv
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import requests

# ─── 路径默认值 ──────────────────────────────────────────────
DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")
OUTPUT_DIR = Path("eda/02_visual/output")

# ─── CLOB API ───────────────────────────────────────────────
CLOB_BASE = "https://clob.polymarket.com"

# ─── 符号检测：市场标题 → Binance instrument_id / Chainlink feed ──
SYMBOL_MAP = {
    "bitcoin": ("btcusdt", "BTC/USD"),
    "btc":     ("btcusdt", "BTC/USD"),
    "ethereum": ("ethusdt", "ETH/USD"),
    "eth":     ("ethusdt", "ETH/USD"),
    "solana":  ("solusdt", "SOL/USD"),
    "sol":     ("solusdt", "SOL/USD"),
    "xrp":     ("xrpusdt", "XRP/USD"),
    "ripple":  ("xrpusdt", "XRP/USD"),
}


# ════════════════════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════════════════════

def make_session(proxy: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers["Accept"] = "application/json"
    p = proxy or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if p:
        s.proxies = {"http": p, "https": p}
    return s


def detect_symbol(title: str) -> tuple[str | None, str | None]:
    """从市场标题检测 Binance instrument_id 和 Chainlink feed。"""
    tl = title.lower()
    for kw, (binance_id, cl_feed) in SYMBOL_MAP.items():
        if kw in tl:
            return binance_id, cl_feed
    return None, None


def ts_to_dt(ts: int | float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ════════════════════════════════════════════════════════════
# 数据加载
# ════════════════════════════════════════════════════════════

def load_tracker_silver(data_dir: str, condition_id: str | None = None) -> pd.DataFrame:
    """加载 tracker silver，可选按 conditionId 过滤。按 transactionHash+asset+side 去重。"""
    files = list(Path(data_dir, "lake/silver/tracker_0x8dxd").rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"tracker silver 为空: {data_dir}/lake/silver/tracker_0x8dxd")
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["transactionHash", "asset", "side"])
    if condition_id:
        df = df[df["conditionId"] == condition_id].copy()
    return df


def get_market_info(condition_id: str, session: requests.Session) -> dict:
    """从 CLOB API 获取市场信息（结算价格、outcomes、token_ids）。"""
    r = session.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=15)
    r.raise_for_status()
    return r.json()


def get_prices_history(token_id: str, session: requests.Session, fidelity: int = 1) -> pd.DataFrame:
    """从 CLOB API 拉取某 token 的价格历史，返回 DataFrame(ts, price)。"""
    r = session.get(
        f"{CLOB_BASE}/prices-history",
        params={"market": token_id, "interval": "max", "fidelity": fidelity},
        timeout=15,
    )
    r.raise_for_status()
    history = r.json().get("history", [])
    if not history:
        return pd.DataFrame(columns=["ts", "price"])
    df = pd.DataFrame(history)
    df.rename(columns={"t": "ts", "p": "price"}, inplace=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df


def _dt_range_globs(base: Path, start_ts: int, end_ts: int) -> list[Path]:
    """按时间范围生成需要扫描的 dt=.../hour=... 目录列表。"""
    dirs = []
    t = start_ts - 3600  # 多往前一小时
    while t <= end_ts + 3600:
        dt = datetime.fromtimestamp(t, tz=timezone.utc)
        d = base / f"dt={dt.strftime('%Y-%m-%d')}" / f"hour={dt.hour:02d}"
        if d.exists() and d not in dirs:
            dirs.append(d)
        t += 3600
    return dirs


def load_binance_silver(data_dir: str, instrument_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """加载指定 instrument 和时间范围的 binance book_ticker silver。"""
    base = Path(data_dir, "lake/silver/binance")
    dirs = _dt_range_globs(base, start_ts, end_ts)
    dfs = []
    for d in dirs:
        for f in (d / "book_ticker").rglob("*.parquet") if (d / "book_ticker").exists() else []:
            try:
                tmp = pd.read_parquet(f)
                filt = tmp[tmp["instrument_id"] == instrument_id]
                if not filt.empty:
                    dfs.append(filt)
            except Exception:
                pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["local_receipt_ts_ms"])
    df["mid"] = (df["b"].astype(float) + df["a"].astype(float)) / 2
    df["dt"] = pd.to_datetime(df["local_receipt_ts_ms"], unit="ms", utc=True)
    df = df[(df["local_receipt_ts_ms"] // 1000 >= start_ts) & (df["local_receipt_ts_ms"] // 1000 <= end_ts)]
    return df.sort_values("local_receipt_ts_ms")


def load_chainlink_silver(data_dir: str, feed: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """加载指定 feed 和时间范围的 chainlink silver。"""
    base = Path(data_dir, "lake/silver/chainlink")
    dirs = _dt_range_globs(base, start_ts, end_ts)
    dfs = []
    for d in dirs:
        for f in d.rglob("*.parquet"):
            try:
                tmp = pd.read_parquet(f)
                filt = tmp[tmp["feed"] == feed]
                if not filt.empty:
                    dfs.append(filt)
            except Exception:
                pass
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["roundId"])
    df["price"] = df["answer"] / 1e8
    df["dt"] = pd.to_datetime(df["updatedAt"], unit="s", utc=True)
    df = df[(df["updatedAt"] >= start_ts) & (df["updatedAt"] <= end_ts)]
    return df.sort_values("updatedAt")


# ════════════════════════════════════════════════════════════
# PnL 计算
# ════════════════════════════════════════════════════════════

def calc_market_pnl(trades: pd.DataFrame, tokens: list[dict]) -> dict:
    """
    计算单市场 PnL。
    trades: conditionId 过滤后的 tracker silver rows
    tokens: CLOB /markets 返回的 tokens 列表（含 outcome, price, winner）
    返回: {outcome: {cost, sell_rev, remaining, settlement_price, pnl}, total_pnl}
    """
    token_map = {t["outcome"]: t for t in tokens}
    result: dict = {}
    total_pnl = 0.0

    for outcome, tok in token_map.items():
        sub = trades[trades["outcome"] == outcome]
        buys = sub[sub["side"] == "BUY"]
        sells = sub[sub["side"] == "SELL"]

        cost = float((buys["size"] * buys["price"]).sum())
        sell_rev = float((sells["size"] * sells["price"]).sum())
        remaining = float(buys["size"].sum() - sells["size"].sum())
        settlement_price = float(tok.get("price", 0))
        settlement_rev = remaining * settlement_price
        pnl = sell_rev + settlement_rev - cost

        result[outcome] = {
            "cost": round(cost, 4),
            "sell_rev": round(sell_rev, 4),
            "remaining_shares": round(remaining, 4),
            "settlement_price": settlement_price,
            "settlement_rev": round(settlement_rev, 4),
            "pnl": round(pnl, 4),
            "winner": tok.get("winner", False),
        }
        total_pnl += pnl

    result["__total_pnl__"] = round(total_pnl, 4)
    return result


# ════════════════════════════════════════════════════════════
# 绘图辅助
# ════════════════════════════════════════════════════════════

def _outcome_colors(tokens: list[dict], trades: pd.DataFrame) -> dict[str, str]:
    """
    决定每个 outcome 的颜色：
    - Yes/Up → 绿；No/Down → 红
    - 其他名称：winner → 绿，loser → 红
    - 无 tokens 时：第一个 outcome → 绿，第二个 → 红
    """
    colors: dict[str, str] = {}
    for tok in tokens:
        o = tok["outcome"]
        if o.lower() in ("up", "yes"):
            colors[o] = "#27ae60"
        elif o.lower() in ("down", "no"):
            colors[o] = "#e74c3c"
        elif tok.get("winner"):
            colors[o] = "#27ae60"
        else:
            colors[o] = "#e74c3c"
    if not colors:
        for i, o in enumerate(trades["outcome"].unique()):
            colors[o] = "#27ae60" if i == 0 else "#e74c3c"
    return colors


def _build_price_series(
    trades: pd.DataFrame,
    ph: pd.DataFrame,
    outcome: str,
    start_ts: int,
    end_ts: int,
    settlement_price: float,
) -> pd.DataFrame:
    """
    构建单个 outcome 的价格时间序列。
    短市场（<2h）或 CLOB 数据稀疏：直接用成交记录里的 price 字段，标记自然落在线上。
    长市场：用 CLOB prices-history，末尾追加结算价格点。
    """
    duration = end_ts - start_ts
    use_trades = duration < 7200 or ph is None or ph.empty or len(ph) < 8

    end_dt = pd.to_datetime(end_ts + max(60, duration // 20), unit="s", utc=True)
    settlement_row = pd.DataFrame({"dt": [end_dt], "price": [float(settlement_price)]})

    if use_trades:
        sub = trades[trades["outcome"] == outcome].sort_values("timestamp").copy()
        if sub.empty:
            return settlement_row
        sub["dt"] = pd.to_datetime(sub["timestamp"], unit="s", utc=True)
        pts = sub[["dt", "price"]].copy()
        return pd.concat([pts, settlement_row], ignore_index=True)
    else:
        base = ph[["dt", "price"]].copy() if not ph.empty else pd.DataFrame(columns=["dt", "price"])
        return pd.concat([base, settlement_row], ignore_index=True)


def _auto_date_fmt(duration_s: int) -> mdates.DateFormatter:
    if duration_s < 7200:
        return mdates.DateFormatter("%H:%M")
    elif duration_s < 86400:
        return mdates.DateFormatter("%m/%d %H:%M")
    else:
        return mdates.DateFormatter("%m/%d")


def plot_market(
    condition_id: str,
    trades: pd.DataFrame,
    market_info: dict,
    price_hists: dict[str, pd.DataFrame],
    binance_df: pd.DataFrame,
    chainlink_df: pd.DataFrame,
    pnl_result: dict,
    output_path: Path,
):
    title = market_info.get("question", condition_id)
    tokens = market_info.get("tokens", [])
    closed = market_info.get("closed", False)
    token_map = {t["outcome"]: t for t in tokens}
    colors = _outcome_colors(tokens, trades)

    # 时间范围
    trades_dt = pd.to_datetime(trades["timestamp"], unit="s", utc=True)
    start_ts = int(trades["timestamp"].min())
    end_ts = int(trades["timestamp"].max())
    duration_s = end_ts - start_ts
    margin = pd.Timedelta(seconds=max(60, duration_s // 15))
    t_min = trades_dt.min() - margin
    t_max = trades_dt.max() + margin * 2  # 右边多留一点显示结算点
    date_fmt = _auto_date_fmt(duration_s)

    # 柱宽：基于时间跨度自适应
    bar_width = pd.Timedelta(seconds=max(10, duration_s / 60))

    total_pnl = pnl_result.get("__total_pnl__", float("nan"))
    color_pnl = "#27ae60" if total_pnl >= 0 else "#e74c3c"

    # ── 3 行 1 列布局，共享 x 轴 ─────────────────────────────
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True,
                             gridspec_kw={"height_ratios": [3, 2, 2]})
    fig.suptitle(
        f"{title}\n"
        f"PnL: {total_pnl:+.2f} USDC  |  closed={closed}  |  [{condition_id[:24]}...]",
        fontsize=11, fontweight="bold",
    )

    # ══ Panel 1: 市场价格 + 0x8dxd 操作标记 ══════════════════
    ax1 = axes[0]
    ax1.set_ylabel("Outcome Price (USDC/share)", fontsize=9)
    ax1.set_ylim(-0.03, 1.08)
    ax1.grid(True, alpha=0.25)

    for outcome, color in colors.items():
        tok = token_map.get(outcome, {})
        settlement_price = float(tok.get("price", 0))
        ph = price_hists.get(outcome, pd.DataFrame())
        series = _build_price_series(trades, ph, outcome, start_ts, end_ts, settlement_price)
        if not series.empty:
            ax1.plot(series["dt"], series["price"],
                     color=color, linewidth=1.8, label=outcome, alpha=0.85, zorder=3)

    # 买卖标记
    for outcome, color in colors.items():
        sub = trades[trades["outcome"] == outcome].copy()
        sub["dt"] = pd.to_datetime(sub["timestamp"], unit="s", utc=True)
        buys = sub[sub["side"] == "BUY"]
        sells = sub[sub["side"] == "SELL"]
        if not buys.empty:
            ax1.scatter(buys["dt"], buys["price"],
                        marker="^", s=55, color=color, edgecolors="black",
                        linewidths=0.6, zorder=6, label=f"BUY {outcome}")
        if not sells.empty:
            ax1.scatter(sells["dt"], sells["price"],
                        marker="v", s=55, color=color, edgecolors="white",
                        linewidths=0.6, zorder=6, label=f"SELL {outcome}")

    # 结算价格水平虚线
    for tok in tokens:
        if tok.get("winner"):
            c = colors.get(tok["outcome"], "blue")
            ax1.axhline(1.0, color=c, linestyle=":", linewidth=1.2, alpha=0.6,
                        label=f"↑ settle={tok['outcome']}")

    # PnL 文字
    ax1.text(0.01, 0.04, f"Total PnL: {total_pnl:+.2f} USDC",
             transform=ax1.transAxes, fontsize=10, color=color_pnl,
             fontweight="bold", va="bottom",
             bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7, ec=color_pnl))
    ax1.legend(fontsize=8, loc="upper left", ncol=2, framealpha=0.8)

    # ══ Panel 2: Binance mid vs Chainlink 价差 ════════════════
    ax2 = axes[1]
    ax2.set_ylabel("Crypto Price (USDC)", fontsize=9)
    ax2.grid(True, alpha=0.25)

    has_data = False
    if not binance_df.empty:
        ax2.plot(binance_df["dt"], binance_df["mid"],
                 color="#2980b9", linewidth=1.2, label="Binance mid", alpha=0.9)
        has_data = True
    if not chainlink_df.empty:
        ax2.plot(chainlink_df["dt"], chainlink_df["price"],
                 color="#8e44ad", linewidth=1.4, label="Chainlink", alpha=0.9,
                 linestyle="--", marker="o", markersize=3)
        has_data = True

    if has_data and not binance_df.empty and not chainlink_df.empty:
        # 价差：右轴
        bin_s = binance_df.set_index("dt")["mid"].resample("1s").last().ffill()
        cl_s = chainlink_df.set_index("dt")["price"].resample("1s").last().ffill()
        common = bin_s.index.intersection(cl_s.index)
        if len(common) > 1:
            spread = bin_s.loc[common] - cl_s.loc[common]
            ax2r = ax2.twinx()
            ax2r.fill_between(common, spread, alpha=0.25, color="#e67e22", label="Spread")
            ax2r.axhline(0, color="gray", linewidth=0.6, linestyle="--")
            ax2r.set_ylabel("Spread (USDC)", fontsize=8)
            ax2r.legend(fontsize=7, loc="lower right")
        ax2.legend(fontsize=8, loc="upper left")
    elif has_data:
        ax2.legend(fontsize=8, loc="upper left")
    else:
        ax2.text(0.5, 0.5, "No Binance/Chainlink data  (non-crypto market)",
                 ha="center", va="center", transform=ax2.transAxes,
                 fontsize=9, color="#7f8c8d", style="italic")

    # ══ Panel 3: 逐笔成交量 ════════════════════════════════════
    ax3 = axes[2]
    ax3.set_ylabel("Notional (USDC)\n▲ Buy  ▼ Sell", fontsize=9)
    ax3.grid(True, alpha=0.25)

    trades_p = trades.copy()
    trades_p["dt"] = pd.to_datetime(trades_p["timestamp"], unit="s", utc=True)
    trades_p["notional"] = trades_p["size"] * trades_p["price"]
    buys_all = trades_p[trades_p["side"] == "BUY"]
    sells_all = trades_p[trades_p["side"] == "SELL"]

    if not buys_all.empty:
        ax3.bar(buys_all["dt"], buys_all["notional"],
                width=bar_width, color="#27ae60", alpha=0.8, label="Buy")
    if not sells_all.empty:
        ax3.bar(sells_all["dt"], -sells_all["notional"],
                width=bar_width, color="#e74c3c", alpha=0.8, label="Sell")
    ax3.axhline(0, color="gray", linewidth=0.6)
    ax3.legend(fontsize=8, loc="upper left")

    # 底部共享 x 轴格式
    ax3.xaxis.set_major_formatter(date_fmt)
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=20, ha="right")
    ax3.set_xlim(t_min, t_max)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] 图已保存: {output_path}")


# ════════════════════════════════════════════════════════════
# 命令：plot
# ════════════════════════════════════════════════════════════

def cmd_plot(args):
    session = make_session(args.proxy)
    condition_id = args.condition_id

    print(f"[plot] condition_id: {condition_id}")

    # 1) 加载 tracker trades
    print("[plot] 加载 tracker silver...")
    trades = load_tracker_silver(args.data_dir, condition_id)
    if trades.empty:
        print("[plot] 该市场无成交记录，退出")
        return

    print(f"[plot] 共 {len(trades)} 条成交，outcome: {trades['outcome'].unique().tolist()}")
    title_str = trades["title"].iloc[0] if "title" in trades.columns else condition_id
    print(f"[plot] 市场: {title_str}")

    # 时间范围
    start_ts = int(trades["timestamp"].min())
    end_ts = int(trades["timestamp"].max())
    print(f"[plot] 时间范围: {ts_to_dt(start_ts)} ~ {ts_to_dt(end_ts)}")

    # 2) CLOB API: 市场信息 + 结算
    print("[plot] 查询 CLOB API 市场信息...")
    market_info = get_market_info(condition_id, session)
    tokens = market_info.get("tokens", [])
    print(f"[plot] tokens: {[(t['outcome'], t.get('price'), t.get('winner')) for t in tokens]}")

    # 3) CLOB 价格历史
    print("[plot] 拉取 CLOB prices-history...")
    price_hists: dict[str, pd.DataFrame] = {}
    for tok in tokens:
        outcome = tok["outcome"]
        ph = get_prices_history(tok["token_id"], session, fidelity=1)
        # 截取到trades时间范围前后1小时
        if not ph.empty:
            ph = ph[(ph["ts"] >= start_ts - 3600) & (ph["ts"] <= end_ts + 3600)]
        price_hists[outcome] = ph
        print(f"  {outcome}: {len(ph)} 个价格点")

    # 4) PnL 计算
    pnl_result = calc_market_pnl(trades, tokens)
    print("[plot] PnL 结果:")
    for outcome, info in pnl_result.items():
        if outcome == "__total_pnl__":
            print(f"  Total PnL: {info:+.4f} USDC")
        else:
            print(f"  {outcome}: cost={info['cost']:.2f}, sell_rev={info['sell_rev']:.2f}, "
                  f"remaining={info['remaining_shares']:.2f}×{info['settlement_price']} "
                  f"→ pnl={info['pnl']:+.4f} USDC  winner={info['winner']}")

    # 5) Binance / Chainlink
    binance_id, cl_feed = detect_symbol(title_str)
    binance_df = pd.DataFrame()
    chainlink_df = pd.DataFrame()

    if binance_id:
        print(f"[plot] 加载 Binance silver ({binance_id})...")
        binance_df = load_binance_silver(args.data_dir, binance_id, start_ts, end_ts)
        print(f"  {len(binance_df)} 行")
    if cl_feed:
        print(f"[plot] 加载 Chainlink silver ({cl_feed})...")
        chainlink_df = load_chainlink_silver(args.data_dir, cl_feed, start_ts, end_ts)
        print(f"  {len(chainlink_df)} 行")

    # 6) 出图
    slug = market_info.get("market_slug", condition_id[:16])
    out_file = Path(args.output or f"{OUTPUT_DIR}/market_{slug}.png")
    plot_market(
        condition_id=condition_id,
        trades=trades,
        market_info=market_info,
        price_hists=price_hists,
        binance_df=binance_df,
        chainlink_df=chainlink_df,
        pnl_result=pnl_result,
        output_path=out_file,
    )


# ════════════════════════════════════════════════════════════
# 命令：pnl（全市场结算 PnL）
# ════════════════════════════════════════════════════════════

def cmd_pnl(args):
    session = make_session(args.proxy)

    print("[pnl] 加载全部 tracker silver...")
    all_trades = load_tracker_silver(args.data_dir, condition_id=None)
    print(f"[pnl] 总成交 {len(all_trades)} 条，conditionId 数: {all_trades['conditionId'].nunique()}")

    # 按成交数排序，取 top-N
    counts = all_trades.groupby("conditionId").size().sort_values(ascending=False)
    top_cids = counts.head(args.top_n).index.tolist()
    print(f"[pnl] 计算前 {len(top_cids)} 个市场的 PnL")

    rows = []
    for i, cid in enumerate(top_cids, 1):
        trades = all_trades[all_trades["conditionId"] == cid]
        title = trades["title"].iloc[0] if "title" in trades.columns else cid[:20]
        try:
            market_info = get_market_info(cid, session)
            tokens = market_info.get("tokens", [])
            closed = market_info.get("closed", False)
            pnl_res = calc_market_pnl(trades, tokens)
            total_pnl = pnl_res.get("__total_pnl__", float("nan"))
            winner = next((t["outcome"] for t in tokens if t.get("winner")), "open")
            rows.append({
                "conditionId": cid,
                "title": title[:60],
                "trade_count": len(trades),
                "closed": closed,
                "winner": winner,
                "total_pnl_usdc": total_pnl,
            })
            status = f"{'closed' if closed else 'open ':6s}  PnL={total_pnl:+8.2f}  winner={winner}"
            print(f"  [{i:3d}/{len(top_cids)}] {title[:45]:45s}  {status}")
        except Exception as e:
            print(f"  [{i:3d}] {title[:45]}  ERROR: {e}")
            rows.append({"conditionId": cid, "title": title[:60], "trade_count": len(trades),
                         "closed": None, "winner": None, "total_pnl_usdc": float("nan")})
        # 简单限速
        time.sleep(0.15)

    df_out = pd.DataFrame(rows)
    closed_df = df_out[df_out["closed"] == True].copy()
    if not closed_df.empty:
        print(f"\n[pnl] 已结算市场共 {len(closed_df)} 个")
        print(f"  总 PnL: {closed_df['total_pnl_usdc'].sum():+.2f} USDC")
        print(f"  胜率:   {(closed_df['total_pnl_usdc'] > 0).mean()*100:.1f}%")

    out_path = Path(args.output or "eda/02_visual/output/pnl_summary.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False)
    print(f"[pnl] 结果已保存: {out_path}")

    # 打印 Top 10 盈亏
    if not closed_df.empty:
        print("\n[pnl] 已结算市场 Top PnL:")
        for _, r in closed_df.sort_values("total_pnl_usdc", ascending=False).head(10).iterrows():
            print(f"  {r['total_pnl_usdc']:+8.2f} USDC  {r['title'][:55]}")


# ════════════════════════════════════════════════════════════
# 入口
# ════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="market_plot: 市场分析图 + PnL 计算")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # plot 子命令
    p_plot = sub.add_parser("plot", help="单市场三联图")
    p_plot.add_argument("--condition-id", required=True, help="Polymarket conditionId (0x...)")
    p_plot.add_argument("--data-dir", default=DATA_DIR)
    p_plot.add_argument("--output", default=None, help="输出 PNG 路径")
    p_plot.add_argument("--proxy", default=None)

    # pnl 子命令
    p_pnl = sub.add_parser("pnl", help="全市场结算 PnL 计算")
    p_pnl.add_argument("--data-dir", default=DATA_DIR)
    p_pnl.add_argument("--top-n", type=int, default=100, help="按成交数取前 N 个市场")
    p_pnl.add_argument("--output", default=None, help="输出 CSV 路径")
    p_pnl.add_argument("--proxy", default=None)

    args = parser.parse_args()
    if args.cmd == "plot":
        cmd_plot(args)
    elif args.cmd == "pnl":
        cmd_pnl(args)


if __name__ == "__main__":
    main()
