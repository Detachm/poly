#!/usr/bin/env python3
"""
Polymarket 跟单模拟盘 — 主入口

用法:
  # 启动持续运行的模拟盘
  python -m simulate.run_paper

  # 指定配置文件
  python -m simulate.run_paper --config data/simulate/config.json

  # 只生成报告（不启动引擎）
  python -m simulate.run_paper --report
  python -m simulate.run_paper --report --report-out reports/paper_report.md
"""
from __future__ import annotations

import argparse
from pathlib import Path

from .config import Config
from .engine import PaperEngine
from .portfolio import Portfolio
from .reporter import generate_report


def main():
    parser = argparse.ArgumentParser(description="Polymarket 跟单模拟盘")
    parser.add_argument("--config",     default="data/simulate/config.json", help="配置文件路径")
    parser.add_argument("--report",     action="store_true",                  help="仅生成报告，不启动引擎")
    parser.add_argument("--report-out", default="data/simulate/report.md",    help="报告输出路径")
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if cfg_path.exists():
        cfg = Config.from_file(str(cfg_path))
    else:
        cfg = Config()
        cfg.save(str(cfg_path))
        print(f"已生成默认配置: {cfg_path}")

    if args.report:
        portfolio = Portfolio(
            portfolio_path=cfg.portfolio_file,
            conflicts_path=cfg.conflicts_log,
            initial_capital=cfg.initial_capital,
        )
        report = generate_report(portfolio, output_path=args.report_out)
        print(report)
        return

    engine = PaperEngine(cfg)
    engine.run()


if __name__ == "__main__":
    main()
