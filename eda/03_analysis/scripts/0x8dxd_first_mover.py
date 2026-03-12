#!/usr/bin/env python3
"""
0x8dxd 先发优势分析：
核心问题：他入场时是否已有方向性判断？

方法：
1. 用 positions 的 curPrice 判断市场最终结果（>0.5 = 该 outcome 赢）
2. 看他首先买入的是赢家侧还是输家侧
3. 看他在赢家侧 vs 输家侧的仓位比例/成本比例
4. 看首笔交易的价格是否比后续更优（先手价格优势）
5. 按市场窗口类型（5m/15m/1h）分别分析
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("eda/01_etl/output/0x8dxd_full")

# ── 加载数据 ──────────────────────────────────────────────────────────────────

df = pd.read_csv(DATA_DIR / "trades_cleaned.csv")
df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
df["size"] = pd.to_numeric(df["size"], errors="coerce")
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df = df.dropna(subset=["conditionId", "side", "size", "price"])
df = df.sort_values("timestamp").reset_index(drop=True)
df["is_yes"] = (df["outcomeIndex"].fillna(0).astype(int) == 0)
df["usdc_cost"] = df["size"] * df["price"]

# 加载 positions（有最终结果信息）
positions = []
with open(DATA_DIR / "positions_raw.jsonl") as f:
    for line in f:
        if line.strip():
            positions.append(json.loads(line))
pos_df = pd.DataFrame(positions)

# 建立 conditionId → outcome 结果映射
# curPrice > 0.5 → 该 outcome 是赢家
outcome_map = {}  # conditionId → {"winner": "Up"/"Down", "settled": True/False}
for cid, grp in pos_df.groupby("conditionId"):
    best = grp.loc[grp["curPrice"].idxmax()]
    worst = grp.loc[grp["curPrice"].idxmin()]
    settled = best["curPrice"] > 0.9  # 接近结算
    outcome_map[cid] = {
        "winner_outcome": best["outcome"],
        "winner_idx": best["outcomeIndex"],
        "winner_curPrice": best["curPrice"],
        "loser_curPrice": worst["curPrice"],
        "settled": settled,
    }

print(f"有 outcome 结果的市场: {len(outcome_map)} 个")
print(f"  已结算(curPrice>0.9): {sum(1 for v in outcome_map.values() if v['settled'])} 个")
print(f"  未结算: {sum(1 for v in outcome_map.values() if not v['settled'])} 个")
print()

# ── 1. 首笔下注方向 vs 最终赢家 ──────────────────────────────────────────────

print("=" * 80)
print("分析一：首笔下注方向 vs 最终赢家")
print("=" * 80)

records = []
for cid, g in df.groupby("conditionId"):
    if cid not in outcome_map:
        continue

    buys = g[g["side"] == "BUY"].sort_values("timestamp")
    if len(buys) == 0:
        continue

    om = outcome_map[cid]
    winner_is_yes = (om["winner_idx"] == 0)

    # 首笔买入
    first_buy = buys.iloc[0]
    first_is_yes = first_buy["is_yes"]
    first_on_winner = (first_is_yes == winner_is_yes)

    # 各侧汇总
    yes_buys = buys[buys["is_yes"]]
    no_buys = buys[~buys["is_yes"]]

    yes_cost = (yes_buys["size"] * yes_buys["price"]).sum()
    no_cost = (no_buys["size"] * no_buys["price"]).sum()
    yes_size = yes_buys["size"].sum()
    no_size = no_buys["size"].sum()

    winner_cost = yes_cost if winner_is_yes else no_cost
    loser_cost = no_cost if winner_is_yes else yes_cost
    winner_size = yes_size if winner_is_yes else no_size
    loser_size = no_size if winner_is_yes else yes_size

    # 赢家侧加权均价
    winner_buys = yes_buys if winner_is_yes else no_buys
    loser_buys = no_buys if winner_is_yes else yes_buys
    winner_avg_p = (winner_buys["size"] * winner_buys["price"]).sum() / winner_buys["size"].sum() if len(winner_buys) > 0 else np.nan
    loser_avg_p = (loser_buys["size"] * loser_buys["price"]).sum() / loser_buys["size"].sum() if len(loser_buys) > 0 else np.nan

    # 时间维度
    first_yes_t = yes_buys["timestamp"].min() if len(yes_buys) > 0 else np.nan
    first_no_t = no_buys["timestamp"].min() if len(no_buys) > 0 else np.nan

    both_sides = len(yes_buys) > 0 and len(no_buys) > 0

    if both_sides:
        winner_entered_first = (first_is_yes == winner_is_yes)
        time_gap = abs(first_yes_t - first_no_t)
    else:
        winner_entered_first = first_on_winner
        time_gap = 0

    # 提取市场窗口类型
    slug = g["slug"].iloc[0] if "slug" in g.columns else ""
    import re
    if re.search(r"updown-1m-", str(slug)):
        window = "1min"
    elif re.search(r"updown-5m-", str(slug)):
        window = "5min"
    elif re.search(r"updown-15m-", str(slug)):
        window = "15min"
    elif re.search(r"up-or-down", str(slug)):
        window = "1hour"
    elif re.search(r"above-\d+k", str(slug)):
        window = "threshold"
    else:
        window = "other"

    records.append({
        "conditionId": cid,
        "question": g["question"].iloc[0],
        "window_type": window,
        "settled": om["settled"],
        "winner": om["winner_outcome"],
        "first_buy_side": "Yes" if first_is_yes else "No",
        "first_on_winner": first_on_winner,
        "both_sides": both_sides,
        "winner_entered_first": winner_entered_first,
        "winner_cost": round(winner_cost, 2),
        "loser_cost": round(loser_cost, 2),
        "winner_size": round(winner_size, 2),
        "loser_size": round(loser_size, 2),
        "winner_avg_price": round(winner_avg_p, 4) if not np.isnan(winner_avg_p) else None,
        "loser_avg_price": round(loser_avg_p, 4) if not np.isnan(loser_avg_p) else None,
        "cost_on_winner_pct": round(winner_cost / (winner_cost + loser_cost) * 100, 1) if (winner_cost + loser_cost) > 0 else 0,
        "size_on_winner_pct": round(winner_size / (winner_size + loser_size) * 100, 1) if (winner_size + loser_size) > 0 else 0,
        "time_gap_sec": time_gap,
        "n_trades": len(g),
    })

result = pd.DataFrame(records)

# ── 整体统计 ──────────────────────────────────────────────────────────────────

settled = result[result["settled"]]
print(f"\n已结算市场 ({len(settled)} 个):\n")

print(f"首笔买入就压中赢家: {settled['first_on_winner'].sum()} / {len(settled)} = {settled['first_on_winner'].mean():.1%}")
print(f"  (随机基准 = 50%)")
print()

both = settled[settled["both_sides"]]
print(f"双边下注且已结算 ({len(both)} 个):")
print(f"  赢家侧先入场: {both['winner_entered_first'].sum()} / {len(both)} = {both['winner_entered_first'].mean():.1%}")
print()

print(f"资金分配偏向赢家侧:")
print(f"  赢家侧成本占比: 均值={both['cost_on_winner_pct'].mean():.1f}%, 中位数={both['cost_on_winner_pct'].median():.1f}%")
print(f"  赢家侧持仓占比: 均值={both['size_on_winner_pct'].mean():.1f}%, 中位数={both['size_on_winner_pct'].median():.1f}%")
print(f"  (50% = 无偏, >50% = 偏向赢家)")
print()

print(f"赢家侧 vs 输家侧买入均价:")
w_prices = both["winner_avg_price"].dropna()
l_prices = both["loser_avg_price"].dropna()
print(f"  赢家侧均价: 均值={w_prices.mean():.4f}, 中位数={w_prices.median():.4f}")
print(f"  输家侧均价: 均值={l_prices.mean():.4f}, 中位数={l_prices.median():.4f}")
print(f"  差值 (赢家-输家): {w_prices.mean() - l_prices.mean():+.4f}")
print(f"  (如果赢家侧买入更便宜 → 有方向性判断的证据)")
print()

# ── 2. 按市场窗口类型分析 ────────────────────────────────────────────────────

print("=" * 80)
print("分析二：按市场窗口类型的先发优势")
print("=" * 80)

for wt in ["5min", "15min", "1hour"]:
    sub = settled[settled["window_type"] == wt]
    if len(sub) < 3:
        continue
    sub_both = sub[sub["both_sides"]]
    print(f"\n[{wt}] 市场 ({len(sub)} 个, 双边 {len(sub_both)} 个):")
    print(f"  首笔压中赢家: {sub['first_on_winner'].mean():.1%}")
    if len(sub_both) > 0:
        print(f"  赢家先入场: {sub_both['winner_entered_first'].mean():.1%}")
        print(f"  赢家侧成本占比: {sub_both['cost_on_winner_pct'].mean():.1f}%")
        print(f"  赢家侧持仓占比: {sub_both['size_on_winner_pct'].mean():.1f}%")
        w_p = sub_both["winner_avg_price"].dropna()
        l_p = sub_both["loser_avg_price"].dropna()
        if len(w_p) > 0 and len(l_p) > 0:
            print(f"  赢家侧均价: {w_p.mean():.4f}, 输家侧均价: {l_p.mean():.4f}")

# ── 3. 入场时间分析：在窗口的多早进入？ ──────────────────────────────────────

print()
print("=" * 80)
print("分析三：建仓时间序列——先手侧 vs 对冲侧的节奏差异")
print("=" * 80)

# 对于双边市场，比较赢家侧和输家侧的建仓节奏
tempo_records = []
for _, row in both.iterrows():
    cid = row["conditionId"]
    g = df[df["conditionId"] == cid]
    buys = g[g["side"] == "BUY"].sort_values("timestamp")

    om = outcome_map[cid]
    winner_is_yes = (om["winner_idx"] == 0)

    winner_buys = buys[buys["is_yes"] == winner_is_yes].sort_values("timestamp")
    loser_buys = buys[buys["is_yes"] != winner_is_yes].sort_values("timestamp")

    if len(winner_buys) == 0 or len(loser_buys) == 0:
        continue

    # 赢家侧：前半建仓的 USDC vs 后半
    w_half = max(1, len(winner_buys) // 2)
    w_early_cost = (winner_buys.head(w_half)["usdc_cost"]).sum()
    w_late_cost = (winner_buys.tail(len(winner_buys) - w_half)["usdc_cost"]).sum()

    # 输家侧同理
    l_half = max(1, len(loser_buys) // 2)
    l_early_cost = (loser_buys.head(l_half)["usdc_cost"]).sum()
    l_late_cost = (loser_buys.tail(len(loser_buys) - l_half)["usdc_cost"]).sum()

    # 赢家侧第一笔和最后一笔的价格变化
    w_first_p = winner_buys.iloc[0]["price"]
    w_last_p = winner_buys.iloc[-1]["price"]
    l_first_p = loser_buys.iloc[0]["price"]
    l_last_p = loser_buys.iloc[-1]["price"]

    tempo_records.append({
        "conditionId": cid,
        "winner_early_pct": w_early_cost / (w_early_cost + w_late_cost) * 100 if (w_early_cost + w_late_cost) > 0 else 50,
        "loser_early_pct": l_early_cost / (l_early_cost + l_late_cost) * 100 if (l_early_cost + l_late_cost) > 0 else 50,
        "winner_price_drift": w_last_p - w_first_p,
        "loser_price_drift": l_last_p - l_first_p,
        "winner_n_buys": len(winner_buys),
        "loser_n_buys": len(loser_buys),
    })

tempo_df = pd.DataFrame(tempo_records)
if len(tempo_df) > 0:
    print(f"\n前半建仓资金占比 (>50% = 前期下重注):")
    print(f"  赢家侧: 均值={tempo_df['winner_early_pct'].mean():.1f}%, 中位数={tempo_df['winner_early_pct'].median():.1f}%")
    print(f"  输家侧: 均值={tempo_df['loser_early_pct'].mean():.1f}%, 中位数={tempo_df['loser_early_pct'].median():.1f}%")
    print()

    print(f"价格漂移 (末笔-首笔, 正=越来越贵):")
    print(f"  赢家侧: 均值={tempo_df['winner_price_drift'].mean():+.4f}")
    print(f"  输家侧: 均值={tempo_df['loser_price_drift'].mean():+.4f}")
    print(f"  (赢家侧越买越贵=说明方向判断正确后持续加仓)")
    print()

    print(f"下单笔数:")
    print(f"  赢家侧: 均值={tempo_df['winner_n_buys'].mean():.1f}笔")
    print(f"  输家侧: 均值={tempo_df['loser_n_buys'].mean():.1f}笔")
    print(f"  (赢家侧笔数更多 → 在赢的方向上持续加码)")

# ── 4. PnL 计算：先手判断正确 vs 错误的盈亏对比 ──────────────────────────────

print()
print("=" * 80)
print("分析四：先手正确 vs 先手错误的盈亏对比")
print("=" * 80)

correct_first = both[both["first_on_winner"]]
wrong_first = both[~both["first_on_winner"]]

print(f"\n先手正确 ({len(correct_first)} 个市场):")
if len(correct_first) > 0:
    # 估算 PnL: 赢家侧结算收到 winner_size * 1.0, 减去两侧总成本
    correct_first = correct_first.copy()
    correct_first["est_pnl"] = correct_first["winner_size"] - correct_first["winner_cost"] - correct_first["loser_cost"]
    print(f"  估算总 PnL: ${correct_first['est_pnl'].sum():.2f}")
    print(f"  均值: ${correct_first['est_pnl'].mean():.2f}/市场")
    print(f"  盈利市场: {(correct_first['est_pnl'] > 0).sum()} / {len(correct_first)}")

print(f"\n先手错误 ({len(wrong_first)} 个市场):")
if len(wrong_first) > 0:
    wrong_first = wrong_first.copy()
    wrong_first["est_pnl"] = wrong_first["winner_size"] - wrong_first["winner_cost"] - wrong_first["loser_cost"]
    print(f"  估算总 PnL: ${wrong_first['est_pnl'].sum():.2f}")
    print(f"  均值: ${wrong_first['est_pnl'].mean():.2f}/市场")
    print(f"  盈利市场: {(wrong_first['est_pnl'] > 0).sum()} / {len(wrong_first)}")

# ── 5. 综合判断 ──────────────────────────────────────────────────────────────

print()
print("=" * 80)
print("综合判断：入场时是否有方向性先发优势？")
print("=" * 80)
print()

first_hit = settled['first_on_winner'].mean()
winner_first_pct = both['winner_entered_first'].mean() if len(both) > 0 else 0
cost_bias = both['cost_on_winner_pct'].mean() if len(both) > 0 else 50
size_bias = both['size_on_winner_pct'].mean() if len(both) > 0 else 50

print(f"证据汇总:")
print(f"  首笔压中赢家: {first_hit:.1%} (基准50%)")
print(f"  赢家侧先入场: {winner_first_pct:.1%} (基准50%)")
print(f"  赢家侧成本占比: {cost_bias:.1f}% (基准50%)")
print(f"  赢家侧持仓占比: {size_bias:.1f}% (基准50%)")
print()

if first_hit > 0.6:
    print("结论: 有较强的方向性先发优势。首笔下注方向显著偏向最终赢家。")
elif first_hit > 0.5:
    print("结论: 有轻微的方向性倾向，但不够显著。可能有弱信号。")
else:
    print("结论: 无方向性先发优势。首笔方向接近随机。")

if size_bias > 55:
    print("补充: 尽管两边下注，赢家侧的仓位明显更重 → 他在正确方向上加码更多。")
elif size_bias > 50:
    print("补充: 赢家侧仓位略重，但差异不大。")
else:
    print("补充: 赢家侧仓位并不更重，纯对冲模式。")

# 导出
result.to_csv(Path("eda/03_analysis/output/0x8dxd_hedge") / "first_mover_analysis.csv", index=False)
print(f"\n详细结果已导出到 eda/03_analysis/output/0x8dxd_hedge/first_mover_analysis.csv")
