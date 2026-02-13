#!/usr/bin/env python3
"""
生成自动日报（统计 + 样例图文件列表）。
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path


def _count_files(root: Path, suffix: str) -> int:
    if not root.exists():
        return 0
    return sum(1 for _ in root.rglob(f"*{suffix}"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate daily markdown report")
    parser.add_argument("--data-dir", default="/vault/core/data/poly")
    parser.add_argument("--visual-dir", default="eda/02_visual/output")
    parser.add_argument("--output", default="docs/daily_report.md")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    visual_dir = Path(args.visual_dir)
    report_path = Path(args.output)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    raw_jsonl = _count_files(data_dir / "raw", ".jsonl")
    bronze_parquet = _count_files(data_dir / "lake" / "bronze", ".parquet")
    mart_parquet = _count_files(data_dir / "lake" / "mart", ".parquet")
    figures = sorted(visual_dir.glob("*.png"))[-10:] if visual_dir.exists() else []

    lines = [
        f"# Daily Report ({now})",
        "",
        "## Stats",
        f"- raw jsonl files: {raw_jsonl}",
        f"- bronze parquet files: {bronze_parquet}",
        f"- mart parquet files: {mart_parquet}",
        "",
        "## Sample Figures",
    ]
    if figures:
        lines.extend([f"- {p}" for p in figures])
    else:
        lines.append("- no figures")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
