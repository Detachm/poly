#!/usr/bin/env python3
"""
Oracle 回测：不同信号准确率下的策略 PnL
========================================

用 0x8dxd 的实际市场数据（价格、成本），
模拟如果方向预测准确率为 X%，能赚多少钱。

确定：
  1. 多准的模型才能回本？（break-even accuracy）
  2. 多准能匹配他的实际收益？
  3. 完美预知的上界是多少？
"""

import csv
import json
import sys
from pathlib import Path

import numpy as np

DECOMP_CSV = Path("eda/03_analysis/output/0x8dxd_pnl_decomp/pnl_decomposition.csv")
OUT_DIR = Path("eda/03_analysis/output/0x8dxd_oracle")
OUT_DIR.mkdir(parents=True, exist_ok=True)

N_SIM = 10000  # Monte Carlo 轮数


def load_settled_markets():
    """加载已结算市场数据"""
    with open(DECOMP_CSV) as f:
        rows = list(csv.DictReader(f))

    markets = []
    for r in rows:
        if r["settled"] != "True" or not r["winner"]:
            continue
        yes_cost = float(r["buy_yes_cost"])
        no_cost = float(r["buy_no_cost"])
        yes_size = float(r["buy_yes_size"])
        no_size = float(r["buy_no_size"])
        total_cost = yes_cost + no_cost
        if total_cost < 1:
            continue

        avg_p_yes = float(r["avg_p_yes"]) if r["avg_p_yes"] else 0
        avg_p_no = float(r["avg_p_no"]) if r["avg_p_no"] else 0
        winner = r["winner"]

        # 他实际的偏好方向和分配比例
        if yes_cost >= no_cost:
            his_favored = "yes"
            his_favored_pct = yes_cost / total_cost
        else:
            his_favored = "no"
            his_favored_pct = no_cost / total_cost

        markets.append({
            "conditionId": r["conditionId"],
            "question": r["question"],
            "winner": winner,
            "total_cost": total_cost,
            "avg_p_yes": avg_p_yes,
            "avg_p_no": avg_p_no,
            "yes_cost": yes_cost,
            "no_cost": no_cost,
            "yes_size": yes_size,
            "no_size": no_size,
            "both_sides": r["both_sides"] == "True",
            "his_favored": his_favored,
            "his_favored_pct": his_favored_pct,
            "his_correct": his_favored == winner,
            "actual_pnl": float(r["pnl_total"]) if r["pnl_total"] else 0,
        })
    return markets


def simulate_pnl(markets, accuracy, n_sim, allocation_mode="empirical"):
    """
    Monte Carlo 模拟：给定信号准确率，计算 PnL 分布

    allocation_mode:
      "empirical" - 每个市场用他实际的分配比例，只改方向
      "fixed"     - 固定 72% 到偏好方向
    """
    rng = np.random.default_rng(42)

    # 从他的实际数据提取分配比例分布
    favored_pcts = np.array([m["his_favored_pct"] for m in markets])
    actual_total_pnl = sum(m["actual_pnl"] for m in markets)

    pnl_totals = np.zeros(n_sim)

    for sim in range(n_sim):
        total_pnl = 0.0
        for i, m in enumerate(markets):
            # 信号方向：accuracy 概率正确
            signal_correct = rng.random() < accuracy

            if signal_correct:
                favored_side = m["winner"]
            else:
                favored_side = "no" if m["winner"] == "yes" else "yes"

            # 分配比例
            if allocation_mode == "empirical":
                # 用他在该市场的实际比例
                fav_pct = m["his_favored_pct"]
            elif allocation_mode == "fixed":
                fav_pct = 0.72  # 他的中位数
            else:
                # 从实际分布中随机抽样
                fav_pct = rng.choice(favored_pcts)

            # 计算各方资金分配
            total_budget = m["total_cost"]
            if favored_side == "yes":
                cost_yes = total_budget * fav_pct
                cost_no = total_budget * (1 - fav_pct)
            else:
                cost_yes = total_budget * (1 - fav_pct)
                cost_no = total_budget * fav_pct

            # 用实际买入价计算 shares
            # (如果该方向实际没有买入价，用对手方价格的互补值估计)
            p_yes = m["avg_p_yes"] if m["avg_p_yes"] > 0 else (1 - m["avg_p_no"])
            p_no = m["avg_p_no"] if m["avg_p_no"] > 0 else (1 - m["avg_p_yes"])

            # 防止除零
            p_yes = max(p_yes, 0.01)
            p_no = max(p_no, 0.01)

            shares_yes = cost_yes / p_yes
            shares_no = cost_no / p_no

            # 结算 PnL
            if m["winner"] == "yes":
                revenue = shares_yes * 1.0  # Yes 赢，每 share 得 $1
            else:
                revenue = shares_no * 1.0   # No 赢

            market_pnl = revenue - total_budget
            total_pnl += market_pnl

        pnl_totals[sim] = total_pnl

    return pnl_totals, actual_total_pnl


def run_backtest(markets):
    print("=" * 70)
    print("  Oracle 回测：方向预测准确率 vs 策略 PnL")
    print("=" * 70)
    print(f"\n  已结算市场: {len(markets)}")
    print(f"  总投入: ${sum(m['total_cost'] for m in markets):,.0f}")
    print(f"  实际总 PnL: ${sum(m['actual_pnl'] for m in markets):,.0f}")
    print(f"  实际方向胜率: {sum(m['his_correct'] for m in markets)/len(markets)*100:.1f}%")
    print(f"  Monte Carlo 轮数: {N_SIM}")
    print(f"  分配模式: empirical (每市场用实际分配比例)")

    actual_pnl = sum(m["actual_pnl"] for m in markets)

    # 模拟不同准确率
    accuracies = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00]

    print(f"\n{'─'*70}")
    print(f"  {'准确率':>6s}  {'PnL均值':>10s}  {'PnL中位':>10s}  {'P5':>10s}  {'P95':>10s}  {'胜率':>6s}  {'vs实际':>8s}")
    print(f"{'─'*70}")

    csv_rows = []
    breakeven_acc = None
    match_acc = None

    for acc in accuracies:
        pnl_dist, _ = simulate_pnl(markets, acc, N_SIM)

        mean_pnl = pnl_dist.mean()
        med_pnl = np.median(pnl_dist)
        p5 = np.percentile(pnl_dist, 5)
        p95 = np.percentile(pnl_dist, 95)
        win_rate = (pnl_dist > 0).mean() * 100

        vs_actual = mean_pnl / actual_pnl * 100 if actual_pnl != 0 else 0

        print(f"  {acc*100:>5.0f}%  ${mean_pnl:>10,.0f}  ${med_pnl:>10,.0f}  ${p5:>10,.0f}  ${p95:>10,.0f}  {win_rate:>5.1f}%  {vs_actual:>6.0f}%")

        if breakeven_acc is None and mean_pnl > 0:
            breakeven_acc = acc
        if match_acc is None and mean_pnl >= actual_pnl * 0.9:
            match_acc = acc

        csv_rows.append({
            "accuracy": acc,
            "pnl_mean": round(mean_pnl, 2),
            "pnl_median": round(med_pnl, 2),
            "pnl_p5": round(p5, 2),
            "pnl_p95": round(p95, 2),
            "pnl_std": round(pnl_dist.std(), 2),
            "win_rate_pct": round(win_rate, 2),
            "vs_actual_pct": round(vs_actual, 2),
        })

    print(f"\n{'─'*70}")
    print(f"  关键阈值:")
    print(f"{'─'*70}")
    if breakeven_acc:
        print(f"  盈亏平衡准确率:   ≥ {breakeven_acc*100:.0f}%")
    if match_acc:
        print(f"  匹配实际收益(90%): ≥ {match_acc*100:.0f}%")
    print(f"  实际方向胜率:      {sum(m['his_correct'] for m in markets)/len(markets)*100:.1f}%")
    print(f"  实际 PnL:          ${actual_pnl:,.0f}")

    # ── 更细粒度：在关键区间细扫 ──
    print(f"\n{'─'*70}")
    print(f"  细粒度扫描 (55%-80%，步长 2.5%)")
    print(f"{'─'*70}")
    print(f"  {'准确率':>6s}  {'PnL均值':>10s}  {'PnL中位':>10s}  {'P5':>10s}  {'P95':>10s}  {'胜率':>6s}")
    print(f"{'─'*70}")

    fine_accs = np.arange(0.525, 0.825, 0.025)
    for acc in fine_accs:
        pnl_dist, _ = simulate_pnl(markets, acc, N_SIM)
        mean_pnl = pnl_dist.mean()
        med_pnl = np.median(pnl_dist)
        p5 = np.percentile(pnl_dist, 5)
        p95 = np.percentile(pnl_dist, 95)
        win_rate = (pnl_dist > 0).mean() * 100
        print(f"  {acc*100:>5.1f}%  ${mean_pnl:>10,.0f}  ${med_pnl:>10,.0f}  ${p5:>10,.0f}  ${p95:>10,.0f}  {win_rate:>5.1f}%")

        csv_rows.append({
            "accuracy": round(acc, 4),
            "pnl_mean": round(mean_pnl, 2),
            "pnl_median": round(med_pnl, 2),
            "pnl_p5": round(p5, 2),
            "pnl_p95": round(p95, 2),
            "pnl_std": round(pnl_dist.std(), 2),
            "win_rate_pct": round(win_rate, 2),
            "vs_actual_pct": round(mean_pnl / actual_pnl * 100 if actual_pnl else 0, 2),
        })

    # ── 对比：fixed 72% vs empirical ──
    print(f"\n{'─'*70}")
    print(f"  分配模式对比 (准确率=75%)")
    print(f"{'─'*70}")
    for mode in ["empirical", "fixed"]:
        pnl_dist, _ = simulate_pnl(markets, 0.75, N_SIM, allocation_mode=mode)
        print(f"  {mode:>12s}: PnL均值=${pnl_dist.mean():>8,.0f}, 中位=${np.median(pnl_dist):>8,.0f}, std=${pnl_dist.std():>6,.0f}")

    # ── 单市场视角：完美预知 vs 随机 ──
    print(f"\n{'─'*70}")
    print(f"  单市场 PnL 贡献（完美预知 vs 随机）")
    print(f"{'─'*70}")

    # 完美预知下每市场的 PnL
    perfect_pnls = []
    random_pnls = []
    for m in markets:
        # 完美：100% 偏好到赢家
        fav_pct = m["his_favored_pct"]
        budget = m["total_cost"]

        p_yes = max(m["avg_p_yes"] if m["avg_p_yes"] > 0 else (1 - m["avg_p_no"]), 0.01)
        p_no = max(m["avg_p_no"] if m["avg_p_no"] > 0 else (1 - m["avg_p_yes"]), 0.01)

        # 完美预知
        if m["winner"] == "yes":
            shares_win = (budget * fav_pct) / p_yes
            shares_lose = (budget * (1 - fav_pct)) / p_no
            rev = shares_win
        else:
            shares_win = (budget * fav_pct) / p_no
            shares_lose = (budget * (1 - fav_pct)) / p_yes
            rev = shares_win
        perfect_pnls.append(rev - budget)

        # 随机 (50/50 → 期望 PnL)
        # E[PnL] = 0.5 × (bigger_on_winner_pnl) + 0.5 × (bigger_on_loser_pnl)
        if m["winner"] == "yes":
            pnl_fav_yes = (budget * fav_pct / p_yes) - budget
            pnl_fav_no = (budget * (1 - fav_pct) / p_yes) - budget
        else:
            pnl_fav_no_win = (budget * fav_pct / p_no) - budget
            pnl_fav_yes_win = (budget * (1 - fav_pct) / p_no) - budget
            pnl_fav_yes = pnl_fav_yes_win
            pnl_fav_no = pnl_fav_no_win
        random_pnls.append((pnl_fav_yes + pnl_fav_no) * 0.5)  # 简化

    print(f"  完美预知总 PnL: ${sum(perfect_pnls):>10,.0f}")
    print(f"  实际总 PnL:     ${actual_pnl:>10,.0f}")
    print(f"  实际 / 完美:    {actual_pnl/sum(perfect_pnls)*100:.1f}%")

    # 保存 CSV
    csv_path = OUT_DIR / "oracle_backtest_results.csv"
    csv_rows.sort(key=lambda x: x["accuracy"])
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        w.writeheader()
        w.writerows(csv_rows)
    print(f"\n  结果已保存: {csv_path}")


def main():
    markets = load_settled_markets()
    run_backtest(markets)


if __name__ == "__main__":
    main()
