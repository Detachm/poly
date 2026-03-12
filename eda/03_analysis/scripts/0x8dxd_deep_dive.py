#!/usr/bin/env python3
"""
0x8dxd 深度分析：
1. 同一市场内的建仓时间序列——是一次性还是逐步建仓？
2. 价格变化追踪——是在价格变动后才对冲另一侧？
3. SELL 行为——卖出是平仓获利还是调仓？
4. 分析是"提前双边挂单"还是"动态对冲"
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("eda/01_etl/output/0x8dxd_full")

df = pd.read_csv(DATA_DIR / "trades_cleaned.csv")
df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
df["size"] = pd.to_numeric(df["size"], errors="coerce")
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df = df.dropna(subset=["conditionId", "side", "size", "price"])
df = df.sort_values("timestamp").reset_index(drop=True)
df["is_yes"] = (df["outcomeIndex"].fillna(0).astype(int) == 0)
df["usdc_cost"] = df["size"] * df["price"]

# ── 1. 选取交易量最大的几个市场做详细时序展开 ─────────────────────────────────

print("=" * 80)
print("TOP 10 市场详细时序 (按交易笔数排)")
print("=" * 80)

top_markets = df.groupby("conditionId").size().nlargest(10).index

for cid in top_markets:
    trades = df[df["conditionId"] == cid].sort_values("timestamp")
    title = trades["question"].iloc[0]
    print(f"\n{'─' * 80}")
    print(f"市场: {title}")
    print(f"conditionId: {cid}")
    print(f"总交易: {len(trades)}, 时间跨度: {trades['timestamp'].max() - trades['timestamp'].min()}秒")

    # 按 (side, is_yes) 统计
    for side in ["BUY", "SELL"]:
        for is_yes, label in [(True, "Yes/Up"), (False, "No/Down")]:
            subset = trades[(trades["side"] == side) & (trades["is_yes"] == is_yes)]
            if len(subset) > 0:
                cost = subset["usdc_cost"].sum()
                avg_p = (subset["size"] * subset["price"]).sum() / subset["size"].sum()
                total_size = subset["size"].sum()
                print(f"  {side} {label}: {len(subset)}笔, "
                      f"size={total_size:.1f}, USDC=${cost:.1f}, avgPrice={avg_p:.4f}, "
                      f"price_range=[{subset['price'].min():.4f}, {subset['price'].max():.4f}]")

    # 时序展开：按时间前15笔
    print(f"\n  时序前20笔:")
    for i, (_, t) in enumerate(trades.head(20).iterrows()):
        side_label = "Yes/Up" if t["is_yes"] else "No/Down"
        print(f"    [{i+1:3d}] ts={t['timestamp']} {t['side']:4s} {side_label:8s} "
              f"size={t['size']:8.2f} @ {t['price']:.4f} (USDC ${t['usdc_cost']:.2f})")

    # 累计建仓变化
    print(f"\n  累计持仓演变 (每10笔采样):")
    cum_yes = 0.0
    cum_no = 0.0
    cum_yes_cost = 0.0
    cum_no_cost = 0.0
    for i, (_, t) in enumerate(trades.iterrows()):
        sign = 1 if t["side"] == "BUY" else -1
        if t["is_yes"]:
            cum_yes += sign * t["size"]
            cum_yes_cost += sign * t["usdc_cost"]
        else:
            cum_no += sign * t["size"]
            cum_no_cost += sign * t["usdc_cost"]
        if (i + 1) % max(1, len(trades) // 10) == 0 or i == len(trades) - 1:
            print(f"    [{i+1:3d}/{len(trades)}] Yes: pos={cum_yes:8.1f} cost=${cum_yes_cost:8.1f} | "
                  f"No: pos={cum_no:8.1f} cost=${cum_no_cost:8.1f} | "
                  f"净敞口: Yes-No={cum_yes - cum_no:+.1f}")

print()

# ── 2. 分析建仓模式：一次性 vs 分批 ─────────────────────────────────────────

print("=" * 80)
print("建仓模式分析")
print("=" * 80)

records = []
for cid, g in df.groupby("conditionId"):
    buys = g[g["side"] == "BUY"]
    if len(buys) < 2:
        continue

    yes_buys = buys[buys["is_yes"]]
    no_buys = buys[~buys["is_yes"]]

    # 建仓集中度：最大单笔占总建仓的比例
    if len(yes_buys) > 0:
        max_buy_pct = yes_buys["usdc_cost"].max() / yes_buys["usdc_cost"].sum()
    else:
        max_buy_pct = 0

    # 价格变化幅度：第一笔和最后一笔价格差
    price_drift_yes = (yes_buys["price"].iloc[-1] - yes_buys["price"].iloc[0]) if len(yes_buys) >= 2 else 0
    price_drift_no = (no_buys["price"].iloc[-1] - no_buys["price"].iloc[0]) if len(no_buys) >= 2 else 0

    records.append({
        "conditionId": cid,
        "n_buys": len(buys),
        "n_yes_buys": len(yes_buys),
        "n_no_buys": len(no_buys),
        "max_buy_pct_of_total": max_buy_pct,
        "yes_price_drift": price_drift_yes,
        "no_price_drift": price_drift_no,
    })

build_df = pd.DataFrame(records)
if len(build_df) > 0:
    print(f"\n最大单笔占总建仓比例:")
    print(f"  均值: {build_df['max_buy_pct_of_total'].mean():.2%}")
    print(f"  中位数: {build_df['max_buy_pct_of_total'].median():.2%}")
    print(f"  >50% (偏一次性): {(build_df['max_buy_pct_of_total'] > 0.5).sum()} / {len(build_df)}")
    print(f"  <20% (明显分批): {(build_df['max_buy_pct_of_total'] < 0.2).sum()} / {len(build_df)}")

    print(f"\nYes侧价格漂移 (末-首):")
    print(f"  均值: {build_df['yes_price_drift'].mean():+.4f}")
    print(f"  说明: 正值=越买越贵(追涨), 负值=越买越便宜(捡便宜)")

    print(f"\nNo侧价格漂移 (末-首):")
    print(f"  均值: {build_df['no_price_drift'].mean():+.4f}")

# ── 3. SELL 行为深度分析 ─────────────────────────────────────────────────────

print()
print("=" * 80)
print("SELL 行为分析")
print("=" * 80)

sells = df[df["side"] == "SELL"]
print(f"\n总 SELL: {len(sells)} 笔 (占总交易 {len(sells)/len(df)*100:.1f}%)")

# 卖出的是哪一侧？
sell_yes = sells[sells["is_yes"]]
sell_no = sells[~sells["is_yes"]]
print(f"  卖 Yes: {len(sell_yes)} 笔, USDC=${sell_yes['usdc_cost'].sum():.1f}")
print(f"  卖 No: {len(sell_no)} 笔, USDC=${sell_no['usdc_cost'].sum():.1f}")

# 卖出价格 vs 同市场买入价格
print(f"\n卖出价格:")
print(f"  Yes侧卖出价: 均值={sell_yes['price'].mean():.4f}, 中位数={sell_yes['price'].median():.4f}" if len(sell_yes) > 0 else "  Yes侧无卖出")
print(f"  No侧卖出价: 均值={sell_no['price'].mean():.4f}, 中位数={sell_no['price'].median():.4f}" if len(sell_no) > 0 else "  No侧无卖出")

# 哪些市场有 SELL？它们是只卖一侧还是两侧都卖？
sell_markets = sells["conditionId"].unique()
print(f"\n有卖出行为的市场: {len(sell_markets)} / {df['conditionId'].nunique()}")

# 在有SELL的市场中，看SELL是否配合BUY形成调仓
for cid in sell_markets[:5]:
    g = df[df["conditionId"] == cid].sort_values("timestamp")
    title = g["question"].iloc[0]
    sell_g = g[g["side"] == "SELL"]
    buy_g = g[g["side"] == "BUY"]
    print(f"\n  [{title}]")
    print(f"    BUY: {len(buy_g)}笔, SELL: {len(sell_g)}笔")
    # SELL 在时间序列中的位置（前期/中期/后期）
    if len(g) > 1:
        sell_positions = []
        for _, s in sell_g.iterrows():
            rank = (g["timestamp"] <= s["timestamp"]).sum() / len(g)
            sell_positions.append(rank)
        print(f"    SELL 位于时间序列的: {[f'{p:.0%}' for p in sell_positions[:5]]}")

# ── 4. 关键问题：是预先套利还是动态对冲？ ─────────────────────────────────────

print()
print("=" * 80)
print("核心判断：预先套利 vs 动态对冲")
print("=" * 80)

# 方法：看双边市场中，第一侧建仓时的价格 vs 第二侧建仓时的价格
# 如果是预先套利：两侧几乎同时，价格互补 (p_yes + p_no ≈ 常数 < 1.0)
# 如果是动态对冲：先建一侧，等价格变动后再建另一侧

pair_records = []
for cid, g in df.groupby("conditionId"):
    buys = g[g["side"] == "BUY"].sort_values("timestamp")
    yes_buys = buys[buys["is_yes"]]
    no_buys = buys[~buys["is_yes"]]

    if len(yes_buys) == 0 or len(no_buys) == 0:
        continue

    # 第一轮建仓的价格
    first_yes_p = yes_buys.iloc[0]["price"]
    first_no_p = no_buys.iloc[0]["price"]
    first_yes_t = yes_buys.iloc[0]["timestamp"]
    first_no_t = no_buys.iloc[0]["timestamp"]

    # 第一侧的加权平均价格（前50%建仓）
    half = max(1, len(yes_buys) // 2)
    early_yes_p = (yes_buys.head(half)["size"] * yes_buys.head(half)["price"]).sum() / yes_buys.head(half)["size"].sum()
    half_n = max(1, len(no_buys) // 2)
    early_no_p = (no_buys.head(half_n)["size"] * no_buys.head(half_n)["price"]).sum() / no_buys.head(half_n)["size"].sum()

    pair_records.append({
        "conditionId": cid,
        "first_yes_price": first_yes_p,
        "first_no_price": first_no_p,
        "first_combined": first_yes_p + first_no_p,
        "early_combined": early_yes_p + early_no_p,
        "time_gap": abs(first_yes_t - first_no_t),
        "first_side": "Yes" if first_yes_t <= first_no_t else "No",
    })

pair_df = pd.DataFrame(pair_records)
if len(pair_df) > 0:
    print(f"\n首笔 Yes + 首笔 No 的价格之和:")
    print(f"  均值: {pair_df['first_combined'].mean():.4f}")
    print(f"  中位数: {pair_df['first_combined'].median():.4f}")
    print(f"  < 1.0: {(pair_df['first_combined'] < 1.0).sum()} / {len(pair_df)}")
    print(f"  >= 1.0: {(pair_df['first_combined'] >= 1.0).sum()} / {len(pair_df)}")

    print(f"\n前半建仓加权均价之和:")
    print(f"  均值: {pair_df['early_combined'].mean():.4f}")
    print(f"  中位数: {pair_df['early_combined'].median():.4f}")

    # 分段分析：按 time_gap 分
    fast_pairs = pair_df[pair_df["time_gap"] <= 10]  # 10秒内
    slow_pairs = pair_df[pair_df["time_gap"] > 60]   # 1分钟以上
    print(f"\n快速配对 (<=10秒) 的 first_combined:")
    if len(fast_pairs) > 0:
        print(f"  均值: {fast_pairs['first_combined'].mean():.4f}, <1.0: {(fast_pairs['first_combined']<1.0).sum()}/{len(fast_pairs)}")
    print(f"慢速配对 (>60秒) 的 first_combined:")
    if len(slow_pairs) > 0:
        print(f"  均值: {slow_pairs['first_combined'].mean():.4f}, <1.0: {(slow_pairs['first_combined']<1.0).sum()}/{len(slow_pairs)}")

# ── 5. 市场生命周期分析：他在市场的哪个阶段入场？ ───────────────────────────

print()
print("=" * 80)
print("入场时间与市场窗口分析")
print("=" * 80)

# 从 slug 提取市场时间窗口信息
import re

def extract_market_window(slug):
    """从 slug 中提取时间窗口类型"""
    if not isinstance(slug, str):
        return "unknown"
    if re.search(r"updown-1m-", slug):
        return "1min"
    elif re.search(r"updown-5m-", slug):
        return "5min"
    elif re.search(r"updown-15m-", slug):
        return "15min"
    elif re.search(r"up-or-down.*\d+am|\d+pm", slug):
        return "1hour"
    elif re.search(r"above-\d+k", slug):
        return "above_threshold"
    else:
        return "other"

df["window_type"] = df["slug"].apply(extract_market_window)
print(f"\n按市场窗口类型:")
window_stats = df.groupby("window_type").agg(
    trades=("conditionId", "size"),
    markets=("conditionId", "nunique"),
    avg_price=("price", "mean"),
    avg_size_usdc=("usdc_cost", "mean"),
).sort_values("trades", ascending=False)
print(window_stats.to_string())

# ── 6. 综合判断 ──────────────────────────────────────────────────────────────

print()
print("=" * 80)
print("综合判断")
print("=" * 80)
print()
print("核心发现:")
print(f"  1. {pair_df['first_combined'].mean():.4f} 的首笔综合价格均值 → ", end="")
if pair_df['first_combined'].mean() < 1.0:
    print("存在系统性套利空间")
else:
    print("首笔配对时综合价格接近或超过1.0，非纯套利")

print(f"  2. {pair_df['time_gap'].median():.0f}秒的中位配对延迟 → ", end="")
if pair_df['time_gap'].median() <= 10:
    print("近似同步下单，疑似自动化套利")
else:
    print("有明显延迟，可能是观察-反应式对冲")

arb_pct = (pair_df['first_combined'] < 1.0).mean()
print(f"  3. {arb_pct:.1%} 的首笔配对有套利空间 → ", end="")
if arb_pct > 0.7:
    print("主要是套利驱动")
elif arb_pct > 0.4:
    print("混合策略：部分套利 + 部分投机/对冲")
else:
    print("主要不是套利驱动")

print()
