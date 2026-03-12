#!/usr/bin/env python3
"""
0x8dxd PnL 三层分解分析
========================
将每个市场的 PnL 拆解为：
  A. 结构性套利：hedged × (1 - p_yes - p_no)
  B. 方向性 alpha：directional × (outcome - p_direction)
  C. 残差（SELL 行为 + 执行差异）

数据源：
  - trades_cleaned.csv    全部成交（BUY/SELL）
  - positions_raw.jsonl   当前/近期持仓快照（含 curPrice 判断结算）
  - activity_cleaned.csv  REDEEM 事件（判断已结算+胜方）
"""

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path("eda/01_etl/output/0x8dxd_full")
OUT_DIR = Path("eda/03_analysis/output/0x8dxd_pnl_decomp")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ── 1. 加载数据 ──────────────────────────────────────────────────

def load_trades():
    """按 conditionId × outcome 汇总 BUY/SELL"""
    markets = defaultdict(lambda: {
        "question": "", "slug": "", "category": "",
        "buy_yes_cost": 0, "buy_yes_size": 0, "buy_yes_n": 0,
        "buy_no_cost": 0, "buy_no_size": 0, "buy_no_n": 0,
        "sell_yes_revenue": 0, "sell_yes_size": 0, "sell_yes_n": 0,
        "sell_no_revenue": 0, "sell_no_size": 0, "sell_no_n": 0,
        "first_ts": 1e18, "last_ts": 0,
    })
    with open(DATA_DIR / "trades_cleaned.csv") as f:
        for r in csv.DictReader(f):
            cid = r["conditionId"]
            m = markets[cid]
            m["question"] = r["question"]
            m["slug"] = r["slug"]
            m["category"] = r.get("market_category", "")
            ts = int(r["timestamp"])
            m["first_ts"] = min(m["first_ts"], ts)
            m["last_ts"] = max(m["last_ts"], ts)
            side = r["side"]  # BUY / SELL
            outcome = r["outcome"]  # Up/Down or Yes/No → 统一为 yes/no
            # Polymarket: outcomeIndex 0 = Yes/Up, 1 = No/Down
            oi = "yes" if r.get("outcomeIndex", "0") == "0" else "no"
            # 如果 outcome 是 Up/Yes → yes, Down/No → no
            if outcome.lower() in ("up", "yes"):
                oi = "yes"
            elif outcome.lower() in ("down", "no"):
                oi = "no"
            size = float(r["size"])
            price = float(r["price"])
            cost = size * price
            if side == "BUY":
                m[f"buy_{oi}_cost"] += cost
                m[f"buy_{oi}_size"] += size
                m[f"buy_{oi}_n"] += 1
            elif side == "SELL":
                m[f"sell_{oi}_revenue"] += cost
                m[f"sell_{oi}_size"] += size
                m[f"sell_{oi}_n"] += 1
    return dict(markets)


def load_positions():
    """按 conditionId 汇总持仓（用 curPrice 判断结算方向）"""
    pos_by_cid = defaultdict(dict)
    with open(DATA_DIR / "positions_raw.jsonl") as f:
        for line in f:
            p = json.loads(line)
            cid = p["conditionId"]
            outcome = p["outcome"].lower()
            if outcome in ("up", "yes"):
                key = "yes"
            else:
                key = "no"
            pos_by_cid[cid][key] = {
                "size": p["size"],
                "avgPrice": p["avgPrice"],
                "curPrice": p["curPrice"],
                "cashPnl": p["cashPnl"],
                "initialValue": p["initialValue"],
                "currentValue": p["currentValue"],
            }
    return dict(pos_by_cid)


def load_redeems():
    """从 activity 取 REDEEM 事件，按 conditionId 汇总兑付金额"""
    redeems = defaultdict(lambda: {"total_usdc": 0, "count": 0})
    with open(DATA_DIR / "activity_cleaned.csv") as f:
        for r in csv.DictReader(f):
            if r["type"] == "REDEEM":
                cid = r["conditionId"]
                redeems[cid]["total_usdc"] += float(r["usdcSize"])
                redeems[cid]["count"] += 1
    return dict(redeems)


# ── 2. 判断市场结算状态与胜方 ────────────────────────────────────

def determine_outcome(cid, trades_m, positions, redeems):
    """
    返回 (settled: bool, winner: str|None)
    winner = "yes" / "no" / None
    """
    # 方法 1：positions 有 curPrice 信息
    if cid in positions:
        pos = positions[cid]
        for side in ("yes", "no"):
            if side in pos and pos[side]["curPrice"] > 0.95:
                return True, side
            if side in pos and pos[side]["curPrice"] < 0.05:
                other = "no" if side == "yes" else "yes"
                if other in pos:
                    return True, other
                # 只有一边有数据且 curPrice<0.05 → 这边输了
                return True, other
        # curPrice 在中间 → 未结算
        return False, None

    # 方法 2：有 REDEEM → 已结算，用 redeem size 匹配买入 size 判断胜方
    if cid in redeems and redeems[cid]["total_usdc"] > 0:
        redeem_size = redeems[cid]["total_usdc"]
        yes_net = trades_m["buy_yes_size"] - trades_m["sell_yes_size"]
        no_net = trades_m["buy_no_size"] - trades_m["sell_no_size"]
        # redeem 金额接近哪边的净持仓 → 那边是胜方
        diff_yes = abs(redeem_size - yes_net)
        diff_no = abs(redeem_size - no_net)
        if yes_net > 0 and (no_net <= 0 or diff_yes < diff_no):
            return True, "yes"
        elif no_net > 0:
            return True, "no"
        return True, None  # 无法确定

    # 方法 3：有 REDEEM 但 total_usdc=0 → 全部亏损
    if cid in redeems and redeems[cid]["total_usdc"] == 0:
        return True, None  # 两边都输了（不应该发生在二元市场）

    return False, None


# ── 3. PnL 分解 ─────────────────────────────────────────────────

def decompose_pnl(trades_m, settled, winner):
    """
    三层分解：
      A = 结构性套利 = hedged_size × (1 - avg_p_yes - avg_p_no)
      B = 方向性 alpha = directional_size × (payout - avg_p_direction)
      C = 残差 = actual_pnl - A - B

    对于未结算市场：仅返回 A（确定性套利空间），B 标记为 pending
    """
    buy_yes_size = trades_m["buy_yes_size"]
    buy_no_size = trades_m["buy_no_size"]
    buy_yes_cost = trades_m["buy_yes_cost"]
    buy_no_cost = trades_m["buy_no_cost"]
    sell_yes_revenue = trades_m["sell_yes_revenue"]
    sell_no_revenue = trades_m["sell_no_revenue"]
    sell_yes_size = trades_m["sell_yes_size"]
    sell_no_size = trades_m["sell_no_size"]

    # 净买入持仓
    net_yes = buy_yes_size - sell_yes_size
    net_no = buy_no_size - sell_no_size

    # 平均买入价
    avg_p_yes = buy_yes_cost / buy_yes_size if buy_yes_size > 0 else 0
    avg_p_no = buy_no_cost / buy_no_size if buy_no_size > 0 else 0

    # 总投入成本
    total_cost = buy_yes_cost + buy_no_cost
    # 卖出收回
    total_sell_revenue = sell_yes_revenue + sell_no_revenue

    both_sides = buy_yes_size > 0 and buy_no_size > 0
    combined_price = avg_p_yes + avg_p_no if both_sides else None

    # ── A: 结构性套利 ──
    # 对冲量 = 两边净持仓的重叠部分
    if net_yes > 0 and net_no > 0:
        hedged = min(net_yes, net_no)
        arb_per_unit = 1.0 - avg_p_yes - avg_p_no if both_sides else 0
        pnl_arb = hedged * arb_per_unit
    else:
        hedged = 0
        arb_per_unit = (1.0 - avg_p_yes - avg_p_no) if both_sides else 0
        pnl_arb = 0

    # ── B: 方向性 alpha ──
    if net_yes > net_no:
        directional_side = "yes"
        directional_size = net_yes - max(net_no, 0)
        directional_price = avg_p_yes
    elif net_no > net_yes:
        directional_side = "no"
        directional_size = net_no - max(net_yes, 0)
        directional_price = avg_p_no
    else:
        directional_side = "none"
        directional_size = 0
        directional_price = 0

    if settled and winner is not None:
        # 结算赔付：胜方每 share 得 $1，败方 $0
        if directional_side == winner:
            payout = 1.0
        elif directional_side == "none":
            payout = 0
        else:
            payout = 0.0
        pnl_direction = directional_size * (payout - directional_price)

        # 实际总 PnL
        winner_net = net_yes if winner == "yes" else net_no
        actual_pnl = winner_net - total_cost + total_sell_revenue

        pnl_residual = actual_pnl - pnl_arb - pnl_direction
    else:
        pnl_direction = None  # 未结算无法算
        actual_pnl = None
        pnl_residual = None

    return {
        "both_sides": both_sides,
        "buy_yes_cost": round(buy_yes_cost, 2),
        "buy_no_cost": round(buy_no_cost, 2),
        "buy_yes_size": round(buy_yes_size, 2),
        "buy_no_size": round(buy_no_size, 2),
        "net_yes": round(net_yes, 2),
        "net_no": round(net_no, 2),
        "avg_p_yes": round(avg_p_yes, 4),
        "avg_p_no": round(avg_p_no, 4),
        "combined_price": round(combined_price, 4) if combined_price else None,
        "hedged_size": round(hedged, 2),
        "arb_per_unit": round(arb_per_unit, 4) if both_sides else None,
        "directional_side": directional_side,
        "directional_size": round(directional_size, 2),
        "directional_price": round(directional_price, 4),
        "sell_revenue": round(total_sell_revenue, 2),
        "pnl_arb": round(pnl_arb, 2),
        "pnl_direction": round(pnl_direction, 2) if pnl_direction is not None else None,
        "pnl_residual": round(pnl_residual, 2) if pnl_residual is not None else None,
        "pnl_total": round(actual_pnl, 2) if actual_pnl is not None else None,
    }


# ── 4. 报告生成 ───────────────────────────────────────────────────

def generate_report(results, label="全部市场"):
    """生成分析报告"""
    settled = [r for r in results if r["settled"]]
    unsettled = [r for r in results if not r["settled"]]

    # 汇总
    total_arb = sum(r["pnl_arb"] for r in settled)
    total_dir = sum(r["pnl_direction"] for r in settled if r["pnl_direction"] is not None)
    total_res = sum(r["pnl_residual"] for r in settled if r["pnl_residual"] is not None)
    total_pnl = sum(r["pnl_total"] for r in settled if r["pnl_total"] is not None)
    total_cost = sum(r["buy_yes_cost"] + r["buy_no_cost"] for r in settled)

    # 未结算市场的套利空间
    unsettled_arb = sum(r["pnl_arb"] for r in unsettled)
    unsettled_cost = sum(r["buy_yes_cost"] + r["buy_no_cost"] for r in unsettled)

    both_sides_count = sum(1 for r in results if r["both_sides"])
    single_side_count = len(results) - both_sides_count

    # 方向性胜率（已结算 + 有方向敞口）
    dir_markets = [r for r in settled if r["directional_size"] > 1]
    dir_win = [r for r in dir_markets if r["pnl_direction"] is not None and r["pnl_direction"] > 0]
    dir_lose = [r for r in dir_markets if r["pnl_direction"] is not None and r["pnl_direction"] <= 0]

    lines = []
    lines.append(f"\n{'='*70}")
    lines.append(f"  0x8dxd PnL 三层分解报告 — {label}")
    lines.append(f"{'='*70}\n")

    lines.append(f"  市场总数:         {len(results)}")
    lines.append(f"  已结算:           {len(settled)}")
    lines.append(f"  未结算:           {len(unsettled)}")
    lines.append(f"  双边下注:         {both_sides_count} ({both_sides_count/len(results)*100:.1f}%)")
    lines.append(f"  单边下注:         {single_side_count} ({single_side_count/len(results)*100:.1f}%)")
    lines.append("")

    lines.append(f"{'─'*70}")
    lines.append(f"  已结算市场 PnL 分解 (n={len(settled)})")
    lines.append(f"{'─'*70}")
    lines.append(f"  总投入成本:             ${total_cost:>12,.2f}")
    lines.append(f"  ────────────────────────────────────")
    lines.append(f"  A. 结构性套利 PnL:      ${total_arb:>12,.2f}  ({total_arb/total_pnl*100:>6.1f}% of total)" if total_pnl else f"  A. 结构性套利 PnL:      ${total_arb:>12,.2f}")
    lines.append(f"  B. 方向性 alpha PnL:    ${total_dir:>12,.2f}  ({total_dir/total_pnl*100:>6.1f}% of total)" if total_pnl else f"  B. 方向性 alpha PnL:    ${total_dir:>12,.2f}")
    lines.append(f"  C. 残差 (SELL/执行):    ${total_res:>12,.2f}  ({total_res/total_pnl*100:>6.1f}% of total)" if total_pnl else f"  C. 残差 (SELL/执行):    ${total_res:>12,.2f}")
    lines.append(f"  ────────────────────────────────────")
    lines.append(f"  总 PnL:                 ${total_pnl:>12,.2f}")
    if total_cost > 0:
        lines.append(f"  ROI:                    {total_pnl/total_cost*100:>11.2f}%")
    lines.append("")

    lines.append(f"{'─'*70}")
    lines.append(f"  方向性 alpha 细分 (有方向敞口的已结算市场, n={len(dir_markets)})")
    lines.append(f"{'─'*70}")
    lines.append(f"  方向判断正确:     {len(dir_win)} 个市场")
    lines.append(f"  方向判断错误:     {len(dir_lose)} 个市场")
    if dir_markets:
        lines.append(f"  方向胜率:         {len(dir_win)/len(dir_markets)*100:.1f}%")
    if dir_win:
        avg_win = sum(r["pnl_direction"] for r in dir_win) / len(dir_win)
        lines.append(f"  正确时平均收益:   ${avg_win:,.2f}")
    if dir_lose:
        avg_lose = sum(r["pnl_direction"] for r in dir_lose) / len(dir_lose)
        lines.append(f"  错误时平均亏损:   ${avg_lose:,.2f}")
    lines.append("")

    # 套利空间分布
    arb_markets = [r for r in results if r["both_sides"] and r["arb_per_unit"] is not None]
    if arb_markets:
        arb_vals = sorted([r["arb_per_unit"] for r in arb_markets])
        positive_arb = [v for v in arb_vals if v > 0]
        negative_arb = [v for v in arb_vals if v <= 0]
        lines.append(f"{'─'*70}")
        lines.append(f"  套利空间分布 (双边市场 n={len(arb_markets)})")
        lines.append(f"{'─'*70}")
        lines.append(f"  正套利空间 (combined < 1): {len(positive_arb)} 个 ({len(positive_arb)/len(arb_markets)*100:.1f}%)")
        lines.append(f"  负套利空间 (combined > 1): {len(negative_arb)} 个 ({len(negative_arb)/len(arb_markets)*100:.1f}%)")
        if positive_arb:
            lines.append(f"  正套利中位数:    {sorted(positive_arb)[len(positive_arb)//2]:.4f} (即每$1对冲赚 ${sorted(positive_arb)[len(positive_arb)//2]:.4f})")
        lines.append(f"  全部中位数:      {arb_vals[len(arb_vals)//2]:.4f}")
        lines.append(f"  全部均值:        {sum(arb_vals)/len(arb_vals):.4f}")
        lines.append(f"  最小值:          {arb_vals[0]:.4f}")
        lines.append(f"  最大值:          {arb_vals[-1]:.4f}")
        lines.append("")

    # 未结算市场锁定的套利
    if unsettled:
        lines.append(f"{'─'*70}")
        lines.append(f"  未结算市场 (n={len(unsettled)})")
        lines.append(f"{'─'*70}")
        lines.append(f"  投入成本:               ${unsettled_cost:>12,.2f}")
        lines.append(f"  已锁定套利空间:         ${unsettled_arb:>12,.2f}")
        lines.append("")

    # Top 10 最赚/最亏市场
    settled_with_pnl = sorted([r for r in settled if r["pnl_total"] is not None],
                               key=lambda x: x["pnl_total"], reverse=True)
    if settled_with_pnl:
        lines.append(f"{'─'*70}")
        lines.append(f"  Top 10 最赚市场")
        lines.append(f"{'─'*70}")
        lines.append(f"  {'市场':<42s} {'总PnL':>8s} {'套利':>8s} {'方向':>8s} {'残差':>8s}")
        for r in settled_with_pnl[:10]:
            q = r["question"][:40]
            lines.append(f"  {q:<42s} {r['pnl_total']:>8.0f} {r['pnl_arb']:>8.0f} {r['pnl_direction'] or 0:>8.0f} {r['pnl_residual'] or 0:>8.0f}")
        lines.append("")
        lines.append(f"{'─'*70}")
        lines.append(f"  Top 10 最亏市场")
        lines.append(f"{'─'*70}")
        lines.append(f"  {'市场':<42s} {'总PnL':>8s} {'套利':>8s} {'方向':>8s} {'残差':>8s}")
        for r in settled_with_pnl[-10:]:
            q = r["question"][:40]
            lines.append(f"  {q:<42s} {r['pnl_total']:>8.0f} {r['pnl_arb']:>8.0f} {r['pnl_direction'] or 0:>8.0f} {r['pnl_residual'] or 0:>8.0f}")
        lines.append("")

    return "\n".join(lines)


# ── 5. 主流程 ────────────────────────────────────────────────────

def main():
    print("加载数据...")
    trades = load_trades()
    positions = load_positions()
    redeems = load_redeems()

    print(f"  trades: {len(trades)} 个市场")
    print(f"  positions: {len(positions)} 个市场")
    print(f"  redeems: {len(redeems)} 个市场")

    results = []
    for cid, m in trades.items():
        settled, winner = determine_outcome(cid, m, positions, redeems)
        decomp = decompose_pnl(m, settled, winner)
        results.append({
            "conditionId": cid,
            "question": m["question"],
            "slug": m["slug"],
            "category": m["category"],
            "settled": settled,
            "winner": winner,
            **decomp,
        })

    # 输出 CSV
    csv_path = OUT_DIR / "pnl_decomposition.csv"
    fields = list(results[0].keys())
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"\n详细结果已保存: {csv_path}")

    # 全部市场报告
    report_all = generate_report(results, "全部市场")
    print(report_all)

    # Crypto-only 报告
    crypto = [r for r in results if r["category"] == "crypto"]
    if crypto:
        report_crypto = generate_report(crypto, "仅 Crypto 市场")
        print(report_crypto)

    # 非 Crypto 报告（如有）
    non_crypto = [r for r in results if r["category"] != "crypto"]
    if non_crypto:
        report_other = generate_report(non_crypto, "非 Crypto 市场")
        print(report_other)

    # 保存报告
    report_path = OUT_DIR / "pnl_decomposition_report.txt"
    with open(report_path, "w") as f:
        f.write(report_all)
        if crypto:
            f.write(report_crypto)
        if non_crypto:
            f.write(report_other)
    print(f"\n报告已保存: {report_path}")


if __name__ == "__main__":
    main()
