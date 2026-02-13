#!/usr/bin/env python3
"""
第二阶段可视化：读取 master.parquet，按 top2 市场分别出图。
每图包含：对应币种 BN 盘口价、逐笔聚合(VWAP/量)、Chainlink、价差、0x8dxd 买卖点。
时间窗口为「该市场在数据中的存续期间」（该市场 best_bid/best_ask 有值的区间）。
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")


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


def _collect_trade_files(data_dir: Path) -> list[Path]:
    files = []
    raw_root = data_dir / "raw" / "tracker_0x8dxd"
    if raw_root.exists():
        files.extend(sorted(raw_root.rglob("trades.jsonl"), key=lambda p: str(p)))
    legacy = data_dir / "tracker_0x8dxd" / "trades.jsonl"
    if legacy.exists():
        files.append(legacy)
    return files


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_path(p: str | Path, base: Path) -> Path:
    path = Path(p) if isinstance(p, str) else p
    if not path.is_absolute():
        path = base / path
    return path.resolve()


def infer_chainlink_column(df: pd.DataFrame, prefer: str = "", coin: str = "", strict_coin: bool = False) -> str | None:
    """返回 Chainlink 列名。prefer 优先；否则按 coin 匹配（如 ETH->chainlink_ETH_USD）。strict_coin=True 时仅当列名含该币种才返回，否则不退回首列避免错配。"""
    cl_cols = [c for c in df.columns if c == "chainlink" or c.startswith("chainlink_")]
    if prefer and prefer in cl_cols:
        return prefer
    if coin:
        coin_upper = coin.upper()
        for c in cl_cols:
            if coin_upper in c.upper():
                return c
        if strict_coin:
            return None
    return cl_cols[0] if cl_cols else None


def load_trades_in_window(
    trade_files: list[Path],
    start_ts_ms: int,
    end_ts_ms: int,
    condition_ids: list[str] | None = None,
    use_receipt_ts: bool = True,
) -> pd.DataFrame:
    """读取 trades.jsonl，过滤时间窗口与可选 conditionId。"""
    rows = []
    for trades_path in trade_files:
        for rec in _iter_jsonl(trades_path):
            ts_ms = rec.get("local_receipt_ts_ms")
            if use_receipt_ts and ts_ms is not None:
                try:
                    t = int(ts_ms)
                except (TypeError, ValueError):
                    continue
            else:
                ts = rec.get("timestamp")
                if ts is None:
                    continue
                try:
                    t = int(ts) * 1000
                except (TypeError, ValueError):
                    continue
            if t < start_ts_ms or t > end_ts_ms:
                continue
            cid = rec.get("conditionId") or rec.get("condition_id")
            if condition_ids is not None and cid not in condition_ids:
                continue
            side = (rec.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                continue
            rows.append({"ts_ms": t, "side": side, "conditionId": cid, "price": rec.get("price")})
    if not rows:
        return pd.DataFrame(columns=["ts_ms", "side", "conditionId", "price"])
    return pd.DataFrame(rows)


def _pm_labels_from_parquet(df: pd.DataFrame) -> list[str]:
    """从 parquet 的 best_bid_* / best_ask_* 列得到 Polymarket 的 label 列表（去重）。"""
    labels = set()
    for c in df.columns:
        if c.startswith("best_bid_") or c.startswith("best_ask_"):
            label = c.replace("best_bid_", "").replace("best_ask_", "")
            labels.add(label)
    return sorted(labels)


def _market_id_to_label(top2_market_ids: list[str], pm_labels: list[str]) -> dict[str, str]:
    """top2 的 market_id -> parquet 中对应的 best_bid_<label> 的 label。"""
    mid_to_label = {}
    # 列名中带 market_<id> 的直接对应
    for mid in top2_market_ids:
        want = "market_" + str(mid)
        if want in pm_labels:
            mid_to_label[mid] = want
    # 剩余 label（如 eth_feb）按顺序对应剩余 market_id
    remaining_mids = [m for m in top2_market_ids if m not in mid_to_label]
    remaining_labels = [l for l in pm_labels if l not in mid_to_label.values()]
    for i, mid in enumerate(remaining_mids):
        if i < len(remaining_labels):
            mid_to_label[mid] = remaining_labels[i]
    return mid_to_label


def _coin_from_title(title: str) -> str:
    """从市场 title 推断币种符号。"""
    if not title:
        return ""
    t = title.upper()
    if "ETH" in t or "ETHEREUM" in t:
        return "ETH"
    if "BTC" in t or "BITCOIN" in t:
        return "BTC"
    if "SOL" in t or "SOLANA" in t:
        return "SOL"
    if "XRP" in t or "RIPPLE" in t:
        return "XRP"
    return ""


def _market_active_window(df_full: pd.DataFrame, label: str) -> tuple[int, int] | None:
    """该市场 label 在 parquet 中的存续区间 (start_ts_ms, end_ts_ms)。"""
    bid_col = "best_bid_" + label
    ask_col = "best_ask_" + label
    if bid_col not in df_full.columns and ask_col not in df_full.columns:
        return None
    mask = pd.Series(True, index=df_full.index)
    if bid_col in df_full.columns:
        mask = mask & df_full[bid_col].notna()
    if ask_col in df_full.columns:
        mask = mask & df_full[ask_col].notna()
    if not mask.any():
        return None
    return int(df_full.loc[mask, "ts_ms"].min()), int(df_full.loc[mask, "ts_ms"].max())


def _draw_one(
    df: pd.DataFrame,
    start_ts_ms: int,
    end_ts_ms: int,
    t0_ms: int,
    cl_col: str,
    cfg: dict,
    buy_sec: list,
    sell_sec: list,
    market_title: str,
    coin: str,
    symbol_hint: str,
    out_path: Path,
    dpi: int,
) -> None:
    """绘制单张图：BN 盘口、Chainlink、价差、逐笔聚合、0x 买卖点；保存后关闭。"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    max_plot_points = int(cfg.get("max_plot_points") or 1500)
    agg_cols = [c for c in df.columns if c.startswith("agg_")]
    # 降采样（含 agg 列）
    if len(df) > max_plot_points:
        df = df.copy()
        df["_bucket_sec"] = np.floor(df["t_sec"].values).astype(int)
        agg_dict = {
            "_binance_price": "mean",
            "_chainlink": "last",
            "_diff": "mean",
            "t_sec": "first",
        }
        for c in agg_cols:
            if c in df.columns:
                if "vol" in c or "qty" in c or "count" in c:
                    agg_dict[c] = "sum"
                else:
                    agg_dict[c] = "mean"
        df = df.groupby("_bucket_sec", as_index=False).agg(agg_dict).copy()

    fig, (ax1, ax_vol) = plt.subplots(2, 1, figsize=(14, 9), height_ratios=[1.5, 0.8], sharex=True)
    ax1.set_ylabel("Price (USD)", fontsize=11)
    ax1.plot(df["t_sec"], df["_binance_price"], color="tab:blue", label="Binance book (mid)", linewidth=1.5)
    ax1.step(df["t_sec"], df["_chainlink"], where="post", color="darkorange", linewidth=1.5, label=f"Chainlink ({cl_col})")
    ax1.tick_params(axis="both", labelsize=10)
    ax1.grid(True, alpha=0.4)
    ax1.set_xlim(df["t_sec"].min(), df["t_sec"].max())

    # 逐笔聚合 VWAP（若有）画在价格图
    vwap_col = None
    for c in ["agg_vwap_100ms", "agg_vwap_50ms", "agg_vwap_200ms"]:
        if c in df.columns:
            vwap_col = c
            break
    if vwap_col is not None:
        ax1.plot(df["t_sec"], df[vwap_col], color="teal", linewidth=0.8, alpha=0.8, label="Agg VWAP (100ms)")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Binance − Chainlink", color="tab:purple", fontsize=11)
    ax2.plot(df["t_sec"], df["_diff"], color="tab:purple", linewidth=1, alpha=0.8, label="Spread")
    ax2.axhline(0, color="gray", linestyle="-", linewidth=0.5)
    ax2.tick_params(axis="y", labelcolor="tab:purple", labelsize=10)

    if buy_sec:
        buy_y = np.interp(buy_sec, df["t_sec"].values, df["_binance_price"].values)
        ax1.scatter(buy_sec, buy_y, marker="^", color="red", s=100, zorder=5, label="0x8dxd Buy")
    if sell_sec:
        sell_y = np.interp(sell_sec, df["t_sec"].values, df["_binance_price"].values)
        ax1.scatter(sell_sec, sell_y, marker="v", color="green", s=100, zorder=5, label="0x8dxd Sell")

    ax1.legend(loc="upper left", fontsize=9)
    title = f"{coin or symbol_hint} | {market_title or 'Market'}"
    if len(title) > 80:
        title = (coin or symbol_hint) + " | " + (market_title[:76] + "...")
    ax1.set_title(title, fontsize=12)

    # 下子图：逐笔聚合量
    ax_vol.set_ylabel("Agg vol (100ms)", fontsize=11)
    vol_col = None
    for c in ["agg_vol_100ms", "agg_vol_50ms", "agg_vol_200ms"]:
        if c in df.columns:
            vol_col = c
            break
    if vol_col is not None:
        ax_vol.fill_between(df["t_sec"], 0, df[vol_col], color="tab:blue", alpha=0.4, label="Vol")
    buy_col = next((c for c in df.columns if "agg_buy_qty" in c), None)
    sell_col = next((c for c in df.columns if "agg_sell_qty" in c), None)
    if buy_col is not None:
        ax_vol.plot(df["t_sec"], df[buy_col], color="green", linewidth=0.8, alpha=0.7, label="Buy qty")
    if sell_col is not None:
        ax_vol.plot(df["t_sec"], -df[sell_col], color="red", linewidth=0.8, alpha=0.7, label="-Sell qty")
    ax_vol.axhline(0, color="gray", linestyle="-", linewidth=0.5)
    ax_vol.legend(loc="upper right", fontsize=9)
    ax_vol.grid(True, alpha=0.4)
    ax_vol.tick_params(axis="both", labelsize=10)
    ax_vol.set_xlabel("Time (sec)", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="按市场出图：BN 盘口 + 逐笔聚合 + Chainlink + 价差 + 0x8dxd；时间为该市场存续期间"
    )
    parser.add_argument("--parquet", default="", help="master.parquet 路径")
    parser.add_argument("--top2", default="", help="top2.json 路径，用于市场列表与存续期")
    parser.add_argument("--data-dir", default="", help="数据根目录（trades.jsonl）")
    parser.add_argument("--start-ts-ms", type=int, default=0, help="单图模式：窗口起始 ms")
    parser.add_argument("--end-ts-ms", type=int, default=0, help="单图模式：窗口结束 ms")
    parser.add_argument("--last-minutes", type=float, default=0, help="单图模式：最近 N 分钟")
    parser.add_argument("--output", default="", help="单图模式：输出路径")
    parser.add_argument("--single", action="store_true", help="只出一张图（用 top2 总窗口或 --start/--end/--last-minutes）")
    parser.add_argument("--config", default="", help="visual_config.json 路径")
    parser.add_argument("--no-trades", action="store_true", help="不画 0x8dxd 买卖点")
    parser.add_argument("--use-receipt-time", action="store_true", help="trades 用 local_receipt_ts_ms（默认 True）")
    args = parser.parse_args()

    config_path = resolve_path(args.config or "eda/02_visual/config/visual_config.json", PROJECT_ROOT)
    cfg = load_config(config_path)
    parquet_path = args.parquet or cfg.get("master_parquet") or "lake/mart/aligned_master/symbol=btcusdt"
    parquet_path = resolve_path(parquet_path, PROJECT_ROOT)
    top2_path = args.top2 or cfg.get("top2_json") or "eda/01_etl/output/top2.json"
    top2_path = resolve_path(top2_path, PROJECT_ROOT)
    data_dir = Path(args.data_dir or cfg.get("data_dir") or POLY_DATA_DIR)
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    data_dir = data_dir.resolve()
    output_dir = resolve_path(cfg.get("output_dir", "eda/02_visual/output"), PROJECT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    chainlink_feed = cfg.get("chainlink_feed") or ""
    # 若 data_dir 下存在 lake/mart/aligned_master，优先使用（与 ETL 输出一致；Docker 挂载 /app/data 时必需）
    mart_under_data = data_dir / "lake" / "mart" / "aligned_master"
    if parquet_path.is_dir():
        parquet_dir = parquet_path
    elif parquet_path.exists():
        parquet_dir = parquet_path.parent
    else:
        parquet_dir = resolve_path("lake/mart/aligned_master", PROJECT_ROOT)
    if mart_under_data.exists():
        parquet_dir = mart_under_data
    elif not parquet_dir.exists():
        if (data_dir / "lake" / "mart" / "aligned_master").exists():
            parquet_dir = data_dir / "lake" / "mart" / "aligned_master"

    df_by_coin = {}  # coin -> DataFrame；按币种用对应 parquet，保证 BN + Chainlink 与市场一致
    markets_to_plot = []

    if top2_path.exists():
        with open(top2_path, "r", encoding="utf-8") as f:
            top2 = json.load(f)
        entries = top2.get("markets")
        if not isinstance(entries, list) or len(entries) == 0:
            entries = []
            for key in ("max_profit", "max_loss"):
                e = top2.get(key)
                if isinstance(e, dict) and e.get("conditionId"):
                    entries.append(e)
        # 有多个市场时按币种加载 parquet（master_btcusdt.parquet 等），避免 BTC 市场用 SOL 价格
        # 只加载 ETL 实际跑过的币种（market_ids_by_coin），否则会尝试加载 SOL/XRP 等未生成 mart 的币种并报「跳过」
        if entries:
            market_ids_by_coin = top2.get("market_ids_by_coin") or {}
            coins_to_load = set((k or "").strip().upper() for k in market_ids_by_coin.keys() if (k or "").strip())
            if not coins_to_load:
                for e in entries:
                    c = (e.get("coin") or _coin_from_title(e.get("title") or e.get("slug")) or "").strip().upper()
                    if c:
                        coins_to_load.add(c)
            for coin in coins_to_load:
                coin_dir = parquet_dir / f"symbol={coin.lower()}usdt"
                candidates = sorted(coin_dir.rglob("*.parquet")) if coin_dir.exists() else []
                path_coin = candidates[-1] if candidates else None
                if path_coin and path_coin.exists():
                    try:
                        df_by_coin[coin] = pd.read_parquet(path_coin)
                    except Exception as ex:
                        print("读取 %s 失败: %s" % (path_coin, ex), file=sys.stderr)
                else:
                    print("跳过币种 %s：未找到 lake/mart/aligned_master/symbol=%susdt/*.parquet" % (coin, coin.lower()), file=sys.stderr)
        if not df_by_coin and entries and parquet_path.exists():
            try:
                df_by_coin[""] = pd.read_parquet(parquet_path)
            except Exception:
                pass
        if df_by_coin:
            for coin, df_coin in df_by_coin.items():
                if "ts_ms" not in df_coin.columns or "b" not in df_coin.columns or "a" not in df_coin.columns:
                    continue
                if coin:
                    coin_entries = [e for e in entries if (e.get("coin") or _coin_from_title(e.get("title") or e.get("slug")) or "").strip().upper() == coin]
                else:
                    coin_entries = entries
                if not coin_entries:
                    continue
                pm_labels = _pm_labels_from_parquet(df_coin)
                market_ids = [str(e.get("market_id") or "") for e in coin_entries if e.get("market_id")]
                mid_to_label = _market_id_to_label(market_ids, pm_labels) if market_ids else {}
                for entry in coin_entries:
                    if not isinstance(entry, dict) or not entry.get("conditionId"):
                        continue
                    mid = str(entry.get("market_id") or "")
                    label = mid_to_label.get(mid)
                    if not label:
                        continue
                    window = _market_active_window(df_coin, label)
                    if not window:
                        continue
                    title = entry.get("title") or entry.get("slug") or mid
                    slug = entry.get("slug") or ""
                    c = (entry.get("coin") or _coin_from_title(title) or "").strip().upper() or coin
                    markets_to_plot.append((
                        mid, [entry["conditionId"]], title, slug, label, window[0], window[1], c or coin
                    ))

    if not df_by_coin and parquet_path.exists():
        df_full_one = pd.read_parquet(parquet_path)
        if "ts_ms" in df_full_one.columns and "b" in df_full_one.columns and "a" in df_full_one.columns:
            df_by_coin[""] = df_full_one
    if not df_by_coin:
        print("未找到可用 parquet（请指定 --parquet 或先运行按币种 ETL）", file=sys.stderr)
        return 1

    # 无 top2 或多市场时用单 parquet 建表（兼容 max_profit/max_loss 或单图）
    if not markets_to_plot and top2_path.exists():
        with open(top2_path, "r", encoding="utf-8") as f:
            top2 = json.load(f)
        df_fallback = df_by_coin.get("") or next(iter(df_by_coin.values()))
        pm_labels = _pm_labels_from_parquet(df_fallback)
        entries = top2.get("markets") or [top2.get("max_profit"), top2.get("max_loss")]
        entries = [e for e in entries if isinstance(e, dict) and e.get("conditionId")]
        market_ids = [str(e.get("market_id") or "") for e in entries if e.get("market_id")]
        mid_to_label = _market_id_to_label(market_ids, pm_labels) if market_ids else {}
        for entry in entries:
            mid = str(entry.get("market_id") or "")
            label = mid_to_label.get(mid)
            if not label:
                continue
            window = _market_active_window(df_fallback, label)
            if not window:
                continue
            title = entry.get("title") or entry.get("slug") or mid
            slug = entry.get("slug") or ""
            coin = _coin_from_title(title)
            markets_to_plot.append((mid, [entry["conditionId"]], title, slug, label, window[0], window[1], coin))

    df_default = next(iter(df_by_coin.values()))
    if args.single or not markets_to_plot:
        # 单图：用指定窗口或 top2 总窗口或全量
        if args.last_minutes and args.last_minutes > 0:
            end_ts_ms = int(df_default["ts_ms"].max())
            start_ts_ms = int(end_ts_ms - args.last_minutes * 60 * 1000)
        elif args.start_ts_ms or args.end_ts_ms:
            start_ts_ms = args.start_ts_ms or int(df_default["ts_ms"].min())
            end_ts_ms = args.end_ts_ms or int(df_default["ts_ms"].max())
        elif top2_path.exists():
            with open(top2_path, "r", encoding="utf-8") as f:
                top2 = json.load(f)
            start_ts_ms = top2.get("window_start_ts_ms") or int(df_default["ts_ms"].min())
            end_ts_ms = top2.get("window_end_ts_ms") or int(df_default["ts_ms"].max())
        else:
            start_ts_ms = int(df_default["ts_ms"].min())
            end_ts_ms = int(df_default["ts_ms"].max())
        condition_ids = None
        if top2_path.exists() and not args.no_trades:
            with open(top2_path, "r", encoding="utf-8") as f:
                top2 = json.load(f)
            cids = []
            for e in top2.get("markets") or []:
                if isinstance(e, dict) and e.get("conditionId"):
                    cids.append(e["conditionId"])
            if not cids:
                for k in ("max_profit", "max_loss"):
                    e = top2.get(k)
                    if isinstance(e, dict) and e.get("conditionId"):
                        cids.append(e["conditionId"])
            condition_ids = cids if cids else None
        markets_to_plot = [(None, condition_ids, "Full window", "", "", start_ts_ms, end_ts_ms, "")]

    # 若没有 top2 且未指定窗口，用全量
    if not markets_to_plot:
        start_ts_ms = int(df_default["ts_ms"].min())
        end_ts_ms = int(df_default["ts_ms"].max())
        markets_to_plot = [("full", None, "Full data", "", "", start_ts_ms, end_ts_ms, "")]

    trade_files = _collect_trade_files(data_dir)
    dpi = int(cfg.get("figure_dpi") or 200)
    saved = []

    for item in markets_to_plot:
        mid, condition_ids, title, slug, label, start_ts_ms, end_ts_ms, coin = item
        if condition_ids is not None and not isinstance(condition_ids, list):
            condition_ids = [condition_ids]
        _df = df_by_coin.get(coin)
        if _df is None:
            _df = df_by_coin.get("")
        if _df is None and df_by_coin:
            _df = next(iter(df_by_coin.values()))
        df_full = _df
        cl_col = infer_chainlink_column(df_full, prefer=chainlink_feed, coin=coin or None, strict_coin=bool(coin))
        if not cl_col:
            if coin:
                print("跳过 Chainlink（无 %s 对应 feed）：%s" % (coin, title[:50]), file=sys.stderr)
            else:
                cl_col = infer_chainlink_column(df_full, prefer=chainlink_feed)
        if not cl_col:
            continue
        df = df_full[(df_full["ts_ms"] >= start_ts_ms) & (df_full["ts_ms"] <= end_ts_ms)].copy()
        if df.empty:
            continue
        df["_binance_price"] = (df["b"].astype(float) + df["a"].astype(float)) / 2
        raw_cl = df[cl_col].astype(float)
        # Chainlink answer 通常为 8 位小数（USD 对），需换算为美元
        chainlink_decimals = int(cfg.get("chainlink_decimals") or 8)
        df["_chainlink"] = raw_cl / (10 ** chainlink_decimals)
        df["_diff"] = df["_binance_price"] - df["_chainlink"]
        t0_ms = df["ts_ms"].min()
        df["t_sec"] = (df["ts_ms"] - t0_ms) / 1000.0

        buy_sec = []
        sell_sec = []
        if not args.no_trades and trade_files:
            trades_df = load_trades_in_window(
                trade_files, start_ts_ms, end_ts_ms,
                condition_ids=condition_ids,
                use_receipt_ts=args.use_receipt_time,
            )
            for _, row in trades_df.iterrows():
                t_sec = (row["ts_ms"] - t0_ms) / 1000.0
                if row["side"] == "BUY":
                    buy_sec.append(t_sec)
                else:
                    sell_sec.append(t_sec)

        if args.output and len(saved) == 0:
            out_path = Path(args.output)
            if not out_path.is_absolute():
                out_path = output_dir / out_path
        else:
            start_dt = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d_%H%M")
            safe_slug = (slug or mid or "market")[:40].replace("/", "_")
            out_path = output_dir / f"window_{start_dt}_{mid or 'full'}_{safe_slug}.png"
        out_path = out_path.resolve()
        _draw_one(
            df, start_ts_ms, end_ts_ms, t0_ms, cl_col, cfg,
            buy_sec, sell_sec, title, coin, "BN",
            out_path, dpi,
        )
        print("已保存:", out_path, flush=True)
        saved.append(out_path)

    return 0 if saved else 1


if __name__ == "__main__":
    sys.exit(main())
