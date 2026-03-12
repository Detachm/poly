#!/usr/bin/env python3
"""
pnl_top10_plot.py: 根据 pnl_summary.csv 生成总体 PnL Top10 与加密货币 PnL Top10 柱状图，
分别输出到 output/pnl_overall/ 与 output/pnl_crypto/。
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
PNL_CSV = OUTPUT_DIR / "pnl_summary.csv"

# 加密货币市场：标题包含以下关键词之一即视为 crypto
CRYPTO_KEYWORDS = re.compile(
    r"bitcoin|btc\b|ethereum|eth\b|crypto|solana|sol\b|xrp|ripple",
    re.I,
)


def load_pnl(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["total_pnl_usdc"] = pd.to_numeric(df["total_pnl_usdc"], errors="coerce")
    return df.dropna(subset=["total_pnl_usdc"])


def is_crypto(title: str) -> bool:
    return bool(CRYPTO_KEYWORDS.search(str(title)))


def short_title(title: str, max_len: int = 50) -> str:
    s = (title or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def plot_top10_bar(
    df: pd.DataFrame,
    title: str,
    out_path: Path,
    bar_color_positive: str = "#27ae60",
    bar_color_negative: str = "#e74c3c",
) -> None:
    """画 Top10 横向柱状图，按 PnL 从高到低。"""
    df = df.sort_values("total_pnl_usdc", ascending=True)  # 画图时从下往上为 1..10
    labels = [short_title(t) for t in df["title"]]
    values = df["total_pnl_usdc"].values
    colors = [bar_color_positive if v >= 0 else bar_color_negative for v in values]

    fig, ax = plt.subplots(figsize=(10, 6))
    y_pos = range(len(labels))
    bars = ax.barh(y_pos, values, color=colors, edgecolor="none", height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("PnL (USDC)")
    ax.set_title(title)
    ax.axvline(0, color="gray", linewidth=0.8, linestyle="-")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  已保存: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="PnL Top10 可视化：总体 + 加密货币")
    parser.add_argument(
        "--csv",
        type=Path,
        default=PNL_CSV,
        help="pnl_summary.csv 路径",
    )
    parser.add_argument(
        "--overall-dir",
        type=Path,
        default=OUTPUT_DIR / "pnl_overall",
        help="总体 Top10 图输出目录",
    )
    parser.add_argument(
        "--crypto-dir",
        type=Path,
        default=OUTPUT_DIR / "pnl_crypto",
        help="加密货币 Top10 图输出目录",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"未找到 CSV: {args.csv}，请先运行 market_plot.py pnl 生成 pnl_summary.csv")

    df = load_pnl(args.csv)
    print(f"[pnl_top10] 已加载 {len(df)} 条记录")

    # 总体 Top10（按 total_pnl_usdc 降序）
    top_overall = df.nlargest(10, "total_pnl_usdc")
    print(f"[pnl_top10] 总体 Top10 PnL 范围: {top_overall['total_pnl_usdc'].min():.2f} ~ {top_overall['total_pnl_usdc'].max():.2f}")
    plot_top10_bar(
        top_overall,
        "PnL Top 10 (Overall)",
        args.overall_dir / "pnl_top10_overall.png",
    )

    # 加密货币子集 Top10
    crypto_df = df[df["title"].astype(str).apply(is_crypto)]
    print(f"[pnl_top10] 加密货币相关市场数: {len(crypto_df)}")
    if crypto_df.empty:
        print("[pnl_top10] 无加密货币市场，跳过 crypto 图")
    else:
        top_crypto = crypto_df.nlargest(10, "total_pnl_usdc")
        plot_top10_bar(
            top_crypto,
            "PnL Top 10 (Cryptocurrency)",
            args.crypto_dir / "pnl_top10_crypto.png",
        )


if __name__ == "__main__":
    main()
