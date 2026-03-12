"""模拟盘报告生成"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .portfolio import Portfolio


def _fmt_ts(ts: int | None) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")


def generate_report(portfolio: Portfolio, output_path: str | None = None) -> str:
    stats = portfolio.stats()
    now   = datetime.now().strftime("%Y-%m-%d %H:%M")

    payoff = (
        abs(stats["avg_win_usdc"]) / max(abs(stats["avg_loss_usdc"]), 0.01)
    )

    lines = [
        f"# 模拟盘报告  {now}", "",
        "## 账户总览",
        "| 指标 | 数值 |",
        "|------|------|",
        f"| 初始资金     | {stats['initial_capital']:,.2f} USDC |",
        f"| 当前 NAV     | {stats['nav']:,.2f} USDC |",
        f"| 已实现 PnL   | {stats['realized_pnl']:+,.2f} USDC |",
        f"| 收益率       | {stats['pnl_pct']:+.2f}% |",
        f"| 现金         | {stats['cash']:,.2f} USDC |",
        f"| 开仓投入     | {stats['open_cost_usdc']:,.2f} USDC |",
        "",
        "## 结算统计",
        "| 指标 | 数值 |",
        "|------|------|",
        f"| 已结算仓位   | {stats['resolved_count']} |",
        f"| 盈利         | {stats['wins']} |",
        f"| 亏损         | {stats['losses']} |",
        f"| 胜率         | {stats['win_rate']:.1%} |",
        f"| 平均盈利     | {stats['avg_win_usdc']:+.2f} USDC |",
        f"| 平均亏损     | {stats['avg_loss_usdc']:+.2f} USDC |",
        f"| 盈亏比       | {payoff:.2f}x |",
        "",
    ]

    # 当前开仓明细
    open_pos = portfolio.open_positions()
    if open_pos:
        lines += [
            "## 当前持仓",
            "| 开仓时间 | 市场 | Outcome | 入场均价 | 成本(U) | 份数 | 归属目标 |",
            "|----------|------|---------|----------|---------|------|---------|",
        ]
        for p in sorted(open_pos, key=lambda x: x.open_ts):
            lines.append(
                f"| {_fmt_ts(p.open_ts)} | {p.title[:30]} | {p.outcome} "
                f"| {p.entry_price:.4f} | {p.cost_usdc:.2f} "
                f"| {p.shares:.2f} | {p.owner[:10]}… |"
            )
        lines.append("")

    # 最近 20 笔成交
    trades = portfolio._state.get("trades", [])[-20:][::-1]
    if trades:
        lines += [
            "## 最近成交（最新 20 笔）",
            "| 时间 | 操作 | 市场 | Outcome | 成交价 | 金额(U) | PnL(U) |",
            "|------|------|------|---------|--------|---------|--------|",
        ]
        for t in trades:
            pnl_str = f"{t['pnl_usdc']:+.2f}" if t.get("pnl_usdc") is not None else "-"
            lines.append(
                f"| {_fmt_ts(t['ts'])} | {t['action']} | {t['title'][:28]} "
                f"| {t['outcome']} | {t['price']:.4f} | {t['usdc']:.2f} | {pnl_str} |"
            )
        lines.append("")

    # 最近 10 条冲突
    cpath = portfolio._cpath
    if cpath.exists():
        try:
            raw = [
                json.loads(line)
                for line in cpath.read_text(encoding="utf-8").strip().splitlines()
                if line.strip()
            ]
            recent_conflicts = raw[-10:][::-1]
            if recent_conflicts:
                lines += [
                    "## 最近冲突记录（最新 10 条）",
                    "| 时间 | 类型 | 触发方 | 市场 | 信号价 | 补充说明 |",
                    "|------|------|--------|------|--------|---------|",
                ]
                for c in recent_conflicts:
                    label    = c.get("triggered_by_label") or c.get("triggered_by", "")[:10]
                    extra    = ""
                    ctype    = c.get("type", "")
                    if ctype == "IGNORED_OPEN":
                        extra = f"owner={c.get('owner','')[:10]}…"
                    elif ctype == "IGNORED_SELL":
                        upnl = c.get("position_unrealized_pnl")
                        extra = f"owner={c.get('owner','')[:10]}… unrealized={upnl:+.2f}U" if upnl is not None else ""
                    elif ctype in ("SELL_NO_POSITION", "SELL_AFTER_SETTLE"):
                        extra = f"status={c.get('position_status','')}"
                    elif ctype == "SKIP_NO_CLOB":
                        extra = f"side={c.get('side','')}"
                    lines.append(
                        f"| {_fmt_ts(c.get('ts'))} | {ctype} "
                        f"| {label} | {c.get('title','')[:28]} "
                        f"| {c.get('signal_price', '-')} | {extra} |"
                    )
                lines.append("")
        except Exception:
            pass

    report = "\n".join(lines)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

    return report
