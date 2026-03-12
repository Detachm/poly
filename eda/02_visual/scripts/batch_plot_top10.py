#!/usr/bin/env python3
"""
batch_plot_top10.py: 按 PnL Top10（总体 + 加密货币）调用 market_plot.py plot，
为每个市场生成一幅图，输出到 pnl_overall/ 与 pnl_crypto/，文件名为 market_<标题slug>.png。
不修改 market_plot.py，仅通过命令行调用。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR.parent / "output"
PNL_CSV = OUTPUT_DIR / "pnl_summary.csv"

CRYPTO_KEYWORDS = re.compile(
    r"bitcoin|btc\b|ethereum|eth\b|crypto|solana|sol\b|xrp|ripple",
    re.I,
)


def title_to_slug(title: str, max_len: int = 80) -> str:
    """将市场标题转为文件名用 slug，如：Another US government shutdown? -> another-us-government-shutdown."""
    s = (title or "").strip()
    s = re.sub(r"[^\w\s\-]", "", s)   # 去掉标点
    s = re.sub(r"\s+", "-", s).strip("-").lower()
    s = re.sub(r"-+", "-", s)          # 多连字符合并
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    return s or "unknown"


def load_pnl(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["total_pnl_usdc"] = pd.to_numeric(df["total_pnl_usdc"], errors="coerce")
    return df.dropna(subset=["total_pnl_usdc"])


def is_crypto(title: str) -> bool:
    return bool(CRYPTO_KEYWORDS.search(str(title)))


def run_plot(condition_id: str, output_path: Path, data_dir: str | None = None, proxy: str | None = None) -> bool:
    """调用 market_plot.py plot --condition-id <id> --output <path>。"""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "market_plot.py"),
        "plot",
        "--condition-id",
        condition_id,
        "--output",
        str(output_path),
    ]
    if data_dir:
        cmd.extend(["--data-dir", data_dir])
    if proxy:
        cmd.extend(["--proxy", proxy])
    project_root = SCRIPT_DIR.parent.parent.parent  # poly 项目根
    ret = subprocess.run(cmd, cwd=project_root)
    return ret.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="PnL Top10 市场批量出图，调用 market_plot.py plot")
    parser.add_argument("--csv", type=Path, default=PNL_CSV, help="pnl_summary.csv 路径")
    parser.add_argument("--overall-dir", type=Path, default=OUTPUT_DIR / "pnl_overall", help="总体 Top10 图输出目录")
    parser.add_argument("--crypto-dir", type=Path, default=OUTPUT_DIR / "pnl_crypto", help="加密货币 Top10 图输出目录")
    parser.add_argument("--data-dir", default=None, help="传给 market_plot 的 --data-dir")
    parser.add_argument("--proxy", default=None, help="传给 market_plot 的 --proxy")
    parser.add_argument("--overall-only", action="store_true", help="只画总体 Top10")
    parser.add_argument("--crypto-only", action="store_true", help="只画加密货币 Top10")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"未找到 CSV: {args.csv}，请先运行: python eda/02_visual/scripts/market_plot.py pnl")

    df = load_pnl(args.csv)
    overall_top = df.nlargest(10, "total_pnl_usdc")
    crypto_df = df[df["title"].astype(str).apply(is_crypto)]
    crypto_top = crypto_df.nlargest(10, "total_pnl_usdc") if not crypto_df.empty else pd.DataFrame()

    def plot_list(subdf: pd.DataFrame, out_dir: Path, label: str) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        for i, row in subdf.iterrows():
            cid = row["conditionId"]
            title = row["title"]
            slug = title_to_slug(title)
            out_path = (out_dir / f"market_{slug}.png").resolve()
            print(f"[{label}] {title[:55]}... -> {out_path.name}")
            ok = run_plot(cid, out_path, args.data_dir, args.proxy)
            if not ok:
                print(f"  失败: condition_id={cid}")

    if not args.crypto_only:
        print("--- 总体 PnL Top10 ---")
        plot_list(overall_top, args.overall_dir, "overall")

    if not args.overall_only and not crypto_top.empty:
        print("--- 加密货币 PnL Top10 ---")
        plot_list(crypto_top, args.crypto_dir, "crypto")

    print("done.")


if __name__ == "__main__":
    main()
