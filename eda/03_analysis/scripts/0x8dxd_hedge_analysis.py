#!/usr/bin/env python3
"""
0x8dxd 策略逆向分析 — 第一轮：双边对冲套利验证
核心假说：0x8dxd 在同一二元市场同时买入 Yes 和 No 两侧，通过价差获利。
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("eda/01_etl/output/0x8dxd_full")
OUT_DIR = Path("eda/03_analysis/output/0x8dxd_hedge")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 1. 加载数据 ────────────────────────────────────────────────────────────────

df = pd.read_csv(DATA_DIR / "trades_cleaned.csv")
df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
df["size"] = pd.to_numeric(df["size"], errors="coerce")
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
df = df.dropna(subset=["conditionId", "side", "size", "price"])
df = df.sort_values("timestamp").reset_index(drop=True)

# outcomeIndex: 0=Yes/Up侧, 1=No/Down侧
df["is_yes"] = (df["outcomeIndex"].fillna(0).astype(int) == 0)
df["cashflow"] = np.where(df["side"] == "BUY", -df["size"] * df["price"], df["size"] * df["price"])

print(f"总交易: {len(df)} 条, 市场数(conditionId): {df['conditionId'].nunique()}")
print(f"时间范围: {df['dt'].min()} ~ {df['dt'].max()}")
print(f"BUY: {(df['side']=='BUY').sum()}, SELL: {(df['side']=='SELL').sum()}")
print()

# ── 2. 按 conditionId 聚合：检测双边下注 ──────────────────────────────────────

def analyze_market(g):
    """按 conditionId 聚合，检测是否双边下注。"""
    buy = g[g["side"] == "BUY"]
    sell = g[g["side"] == "SELL"]

    buy_yes = buy[buy["is_yes"]]
    buy_no = buy[~buy["is_yes"]]
    sell_yes = sell[sell["is_yes"]]
    sell_no = sell[~sell["is_yes"]]

    # 各侧的成本/收入
    buy_yes_cost = (buy_yes["size"] * buy_yes["price"]).sum()
    buy_no_cost = (buy_no["size"] * buy_no["price"]).sum()
    sell_yes_rev = (sell_yes["size"] * sell_yes["price"]).sum()
    sell_no_rev = (sell_no["size"] * sell_no["price"]).sum()

    # 各侧净持仓
    yes_pos = buy_yes["size"].sum() - sell_yes["size"].sum()
    no_pos = buy_no["size"].sum() - sell_no["size"].sum()

    # 加权平均买入价
    buy_yes_avg_p = (buy_yes["size"] * buy_yes["price"]).sum() / buy_yes["size"].sum() if len(buy_yes) > 0 else np.nan
    buy_no_avg_p = (buy_no["size"] * buy_no["price"]).sum() / buy_no["size"].sum() if len(buy_no) > 0 else np.nan

    # 是否双边: 两侧都有 BUY
    both_sides = len(buy_yes) > 0 and len(buy_no) > 0

    # 如果双边下注，计算综合成本（买 Yes + 买 No 的总成本 vs 总 size）
    # 二元市场: Yes_price + No_price 应该 ≈ 1.0（扣除手续费）
    # 如果 avg_yes_price + avg_no_price < 1.0 → 套利空间
    combined_price = (buy_yes_avg_p if not np.isnan(buy_yes_avg_p) else 0) + \
                     (buy_no_avg_p if not np.isnan(buy_no_avg_p) else 0)

    # 时间跨度
    t_min = g["timestamp"].min()
    t_max = g["timestamp"].max()
    duration_sec = t_max - t_min

    return pd.Series({
        "question": g["question"].iloc[0],
        "market_category": g["market_category"].iloc[0] if "market_category" in g.columns else "",
        "n_trades": len(g),
        "n_buy": len(buy),
        "n_sell": len(sell),
        "buy_yes_count": len(buy_yes),
        "buy_no_count": len(buy_no),
        "sell_yes_count": len(sell_yes),
        "sell_no_count": len(sell_no),
        "buy_yes_cost": round(buy_yes_cost, 2),
        "buy_no_cost": round(buy_no_cost, 2),
        "buy_yes_avg_price": round(buy_yes_avg_p, 4) if not np.isnan(buy_yes_avg_p) else None,
        "buy_no_avg_price": round(buy_no_avg_p, 4) if not np.isnan(buy_no_avg_p) else None,
        "combined_buy_price": round(combined_price, 4),
        "yes_net_position": round(yes_pos, 2),
        "no_net_position": round(no_pos, 2),
        "total_cashflow": round(g["cashflow"].sum(), 2),
        "both_sides_buy": both_sides,
        "duration_sec": duration_sec,
        "first_trade": t_min,
        "last_trade": t_max,
    })

mkt = df.groupby("conditionId").apply(analyze_market).reset_index()
print("=" * 80)
print("按市场(conditionId)聚合完成")
print(f"总市场数: {len(mkt)}")
print(f"双边下注市场数: {mkt['both_sides_buy'].sum()} / {len(mkt)} = {mkt['both_sides_buy'].mean()*100:.1f}%")
print()

# ── 3. 双边下注市场的详细分析 ──────────────────────────────────────────────────

hedge = mkt[mkt["both_sides_buy"]].copy()
print("=" * 80)
print(f"双边下注市场详细分析 (共 {len(hedge)} 个)")
print()

# combined_buy_price 分布: < 1.0 说明有套利空间
print("综合买入价 (Yes_avg + No_avg) 分布:")
print(f"  < 0.90: {(hedge['combined_buy_price'] < 0.90).sum()} 个")
print(f"  0.90-0.95: {((hedge['combined_buy_price'] >= 0.90) & (hedge['combined_buy_price'] < 0.95)).sum()} 个")
print(f"  0.95-1.00: {((hedge['combined_buy_price'] >= 0.95) & (hedge['combined_buy_price'] < 1.00)).sum()} 个")
print(f"  >= 1.00: {(hedge['combined_buy_price'] >= 1.00).sum()} 个")
print(f"  均值: {hedge['combined_buy_price'].mean():.4f}")
print(f"  中位数: {hedge['combined_buy_price'].median():.4f}")
print()

# 套利利润 = 如果 combined < 1.0，每单位名义的理论利润
hedge["theoretical_arb_per_unit"] = 1.0 - hedge["combined_buy_price"]
hedge["min_side_cost"] = hedge[["buy_yes_cost", "buy_no_cost"]].min(axis=1)
hedge["max_side_cost"] = hedge[["buy_yes_cost", "buy_no_cost"]].max(axis=1)
hedge["cost_ratio"] = hedge["min_side_cost"] / hedge["max_side_cost"].replace(0, np.nan)

print("各侧投入比例 (min_cost / max_cost):")
print(f"  均值: {hedge['cost_ratio'].mean():.4f}")
print(f"  中位数: {hedge['cost_ratio'].median():.4f}")
print(f"  说明: 越接近1.0说明两侧下注金额越均衡")
print()

# ── 4. 单边下注市场分析 ──────────────────────────────────────────────────────

single = mkt[~mkt["both_sides_buy"]].copy()
print("=" * 80)
print(f"单边下注市场 (共 {len(single)} 个)")
if len(single) > 0:
    only_yes = single[(single["buy_yes_count"] > 0) & (single["buy_no_count"] == 0)]
    only_no = single[(single["buy_no_count"] > 0) & (single["buy_yes_count"] == 0)]
    print(f"  仅买 Yes: {len(only_yes)} 个")
    print(f"  仅买 No: {len(only_no)} 个")
    print(f"  仅 SELL (无 BUY): {len(single) - len(only_yes) - len(only_no)} 个")
print()

# ── 5. 时序分析：同一市场内两侧下单的时间差 ──────────────────────────────────

print("=" * 80)
print("双边下注时间序列分析")
print()

timing_records = []
for cid in hedge["conditionId"]:
    trades = df[df["conditionId"] == cid].sort_values("timestamp")
    buy_trades = trades[trades["side"] == "BUY"]

    yes_buys = buy_trades[buy_trades["is_yes"]].sort_values("timestamp")
    no_buys = buy_trades[~buy_trades["is_yes"]].sort_values("timestamp")

    if len(yes_buys) == 0 or len(no_buys) == 0:
        continue

    first_yes_t = yes_buys["timestamp"].iloc[0]
    first_no_t = no_buys["timestamp"].iloc[0]

    # 两侧首笔交易的时间差
    time_gap = abs(first_yes_t - first_no_t)
    which_first = "Yes" if first_yes_t <= first_no_t else "No"

    # 交替模式分析：按时间排序所有 BUY，看 Yes/No 的交替频率
    all_buys = buy_trades.sort_values("timestamp")
    sides_seq = all_buys["is_yes"].astype(int).tolist()  # 1=Yes, 0=No
    switches = sum(1 for i in range(1, len(sides_seq)) if sides_seq[i] != sides_seq[i-1])

    timing_records.append({
        "conditionId": cid,
        "question": trades["question"].iloc[0],
        "first_side": which_first,
        "time_gap_sec": time_gap,
        "n_buys": len(all_buys),
        "n_yes_buys": len(yes_buys),
        "n_no_buys": len(no_buys),
        "side_switches": switches,
        "switch_rate": switches / (len(all_buys) - 1) if len(all_buys) > 1 else 0,
    })

timing_df = pd.DataFrame(timing_records)
if len(timing_df) > 0:
    print(f"首笔 Yes vs No 的时间差分布:")
    print(f"  0秒 (同一秒下单): {(timing_df['time_gap_sec'] == 0).sum()} 个")
    print(f"  1-10秒: {((timing_df['time_gap_sec'] > 0) & (timing_df['time_gap_sec'] <= 10)).sum()} 个")
    print(f"  11-60秒: {((timing_df['time_gap_sec'] > 10) & (timing_df['time_gap_sec'] <= 60)).sum()} 个")
    print(f"  1-5分钟: {((timing_df['time_gap_sec'] > 60) & (timing_df['time_gap_sec'] <= 300)).sum()} 个")
    print(f"  >5分钟: {(timing_df['time_gap_sec'] > 300).sum()} 个")
    print(f"  均值: {timing_df['time_gap_sec'].mean():.1f}秒, 中位数: {timing_df['time_gap_sec'].median():.1f}秒")
    print()

    print(f"先下哪侧:")
    print(f"  先 Yes: {(timing_df['first_side']=='Yes').sum()}")
    print(f"  先 No: {(timing_df['first_side']=='No').sum()}")
    print()

    print(f"交替下注频率 (switch_rate: 越高=越频繁在 Yes/No 间切换):")
    print(f"  均值: {timing_df['switch_rate'].mean():.4f}")
    print(f"  中位数: {timing_df['switch_rate'].median():.4f}")
    print()

# ── 6. Positions 数据验证：双边持仓确认 ──────────────────────────────────────

print("=" * 80)
print("Positions 数据验证")
print()

positions = []
with open(DATA_DIR / "positions_raw.jsonl") as f:
    for line in f:
        if line.strip():
            positions.append(json.loads(line))

pos_df = pd.DataFrame(positions)
print(f"总持仓: {len(pos_df)} 条, 涉及市场: {pos_df['conditionId'].nunique()} 个")

# 找出同一 conditionId 有两条记录的（双边持仓）
pos_both = pos_df.groupby("conditionId").filter(lambda g: len(g) >= 2)
pos_both_cids = pos_both["conditionId"].unique()
print(f"双边持仓市场: {len(pos_both_cids)} 个")
print()

for cid in pos_both_cids:
    rows = pos_df[pos_df["conditionId"] == cid]
    title = rows["title"].iloc[0]
    print(f"  [{title}]")
    for _, r in rows.iterrows():
        print(f"    {r['outcome']}: size={r['size']:.2f}, avgPrice={r['avgPrice']:.4f}, "
              f"curPrice={r['curPrice']:.4f}, cashPnl=${r['cashPnl']:.2f}")

    # 计算综合成本
    total_cost = sum(r["size"] * r["avgPrice"] for _, r in rows.iterrows())
    total_value = sum(r["currentValue"] for _, r in rows.iterrows())
    total_size_min = min(r["size"] for _, r in rows.iterrows())
    combined_avg_price = sum(r["avgPrice"] for _, r in rows.iterrows())
    # 对冲利润 = min(yes_size, no_size) * (1 - combined_avg_price)
    arb_profit = total_size_min * (1.0 - combined_avg_price) if combined_avg_price < 1.0 else 0
    print(f"    综合: cost=${total_cost:.2f}, value=${total_value:.2f}, "
          f"combined_avg_price={combined_avg_price:.4f}, "
          f"理论套利利润=${arb_profit:.2f}")
    print()

# ── 7. 价格行为分析：在什么价格水平下单 ──────────────────────────────────────

print("=" * 80)
print("买入价格分布")
print()

buys = df[df["side"] == "BUY"]
print(f"Yes侧买入价:")
yes_prices = buys[buys["is_yes"]]["price"]
print(f"  均值={yes_prices.mean():.4f}, 中位数={yes_prices.median():.4f}, "
      f"std={yes_prices.std():.4f}, min={yes_prices.min():.4f}, max={yes_prices.max():.4f}")

print(f"No侧买入价:")
no_prices = buys[~buys["is_yes"]]["price"]
print(f"  均值={no_prices.mean():.4f}, 中位数={no_prices.median():.4f}, "
      f"std={no_prices.std():.4f}, min={no_prices.min():.4f}, max={no_prices.max():.4f}")

print(f"\nYes+No 买入价之和 (按市场):")
paired = hedge[hedge["buy_yes_avg_price"].notna() & hedge["buy_no_avg_price"].notna()]
print(f"  均值={paired['combined_buy_price'].mean():.4f}")
print(f"  中位数={paired['combined_buy_price'].median():.4f}")
print(f"  <1.0 (有套利空间): {(paired['combined_buy_price'] < 1.0).sum()} / {len(paired)}")
print(f"  >=1.0 (无套利空间): {(paired['combined_buy_price'] >= 1.0).sum()} / {len(paired)}")
print()

# ── 8. 按 size 分析：下注规模 ──────────────────────────────────────────────────

print("=" * 80)
print("下注规模分析")
print()
buy_sizes = buys["size"] * buys["price"]  # USDC 成本
print(f"单笔买入 USDC 成本:")
print(f"  均值=${buy_sizes.mean():.2f}, 中位数=${buy_sizes.median():.2f}")
print(f"  25%=${buy_sizes.quantile(0.25):.2f}, 75%=${buy_sizes.quantile(0.75):.2f}")
print(f"  min=${buy_sizes.min():.2f}, max=${buy_sizes.max():.2f}")
print()

# ── 9. 导出详细结果 ──────────────────────────────────────────────────────────

mkt.to_csv(OUT_DIR / "markets_hedge_analysis.csv", index=False)
hedge.to_csv(OUT_DIR / "hedge_markets_detail.csv", index=False)
if len(timing_df) > 0:
    timing_df.to_csv(OUT_DIR / "hedge_timing_analysis.csv", index=False)

# ── 10. 综合结论 ──────────────────────────────────────────────────────────────

print("=" * 80)
print("综合结论")
print("=" * 80)
print()

hedge_pct = mkt["both_sides_buy"].mean() * 100
avg_combined = paired["combined_buy_price"].mean() if len(paired) > 0 else 0
arb_space = (paired["combined_buy_price"] < 1.0).sum() if len(paired) > 0 else 0
total_arb_pct = arb_space / len(paired) * 100 if len(paired) > 0 else 0

print(f"1. 双边下注比例: {hedge_pct:.1f}% 的市场同时买入 Yes 和 No")
print(f"2. 综合买入价均值: {avg_combined:.4f} (< 1.0 说明有正期望)")
print(f"3. 有套利空间的市场: {arb_space}/{len(paired)} ({total_arb_pct:.1f}%)")
if len(timing_df) > 0:
    print(f"4. 两侧首笔交易中位时间差: {timing_df['time_gap_sec'].median():.0f}秒")
    print(f"5. 交替下注率: {timing_df['switch_rate'].mean():.2%} (高=频繁切换Yes/No)")
print()
