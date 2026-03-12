#!/usr/bin/env python3
"""
BTC 5min 方向预测因子测试
==========================
从 Binance book_ticker + agg_trades 构建 5 类因子，
评估每个因子对"5min 后涨跌"的预测力（IC / 准确率）。

因子列表：
  1. OBI  — Order Book Imbalance (bid_qty - ask_qty) / (bid_qty + ask_qty)
  2. TFI  — Trade Flow Imbalance (buy_vol - sell_vol) / total_vol
  3. MOM  — Mid Price Momentum (多尺度: 10s/30s/1m/5m)
  4. VSUR — Volume Surge Ratio (recent_vol / baseline_vol)
  5. SPRD — Spread 变化率 (spread_now / spread_ma)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

DATA_DIR = Path("/vault/core/data/poly/lake/silver/binance")
OUT_DIR = Path("eda/03_analysis/output/0x8dxd_factors")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 只测 BTCUSDT
SYMBOL_BT = "BTCUSDT"
STREAM_AT = "btcusdt@aggTrade"

# 使用所有可用日期
ALL_DATES = sorted([d.name.split("=")[1] for d in DATA_DIR.iterdir() if d.is_dir()])

# ── 1. 数据加载 ─────────────────────────────────────────────────

def load_book_ticker_day(dt_str):
    """加载一天的 book_ticker，返回 BTCUSDT 的 1s 重采样"""
    dt_path = DATA_DIR / f"dt={dt_str}"
    dfs = []
    for hour_dir in sorted(dt_path.iterdir()):
        for sub in hour_dir.iterdir():
            if "book_ticker" not in sub.name:
                continue
            try:
                t = pq.read_table(sub, columns=["local_receipt_ts_ms", "s", "b", "B", "a", "A"])
                df = t.to_pandas()
                df = df[df["s"] == SYMBOL_BT]
                if len(df) > 0:
                    dfs.append(df)
            except Exception:
                pass
    if not dfs:
        return None
    df = pd.concat(dfs, ignore_index=True)
    df["ts_ms"] = df["local_receipt_ts_ms"].astype(np.int64)
    df["bid"] = df["b"].astype(float)
    df["bid_qty"] = df["B"].astype(float)
    df["ask"] = df["a"].astype(float)
    df["ask_qty"] = df["A"].astype(float)
    df["mid"] = (df["bid"] + df["ask"]) / 2
    df["spread"] = df["ask"] - df["bid"]
    df["obi"] = (df["bid_qty"] - df["ask_qty"]) / (df["bid_qty"] + df["ask_qty"])

    # 1s 重采样（取每秒最后一条）
    df["ts_s"] = (df["ts_ms"] // 1000) * 1000
    df = df.sort_values("ts_ms")
    resampled = df.groupby("ts_s").agg(
        mid=("mid", "last"),
        spread=("spread", "last"),
        obi=("obi", "mean"),         # 秒内平均 OBI
        bid_qty=("bid_qty", "last"),
        ask_qty=("ask_qty", "last"),
        n_ticks=("mid", "count"),
    ).reset_index()
    return resampled


def load_agg_trades_day(dt_str):
    """加载一天的 agg_trades，返回 BTCUSDT 的 1s 聚合"""
    dt_path = DATA_DIR / f"dt={dt_str}"
    dfs = []
    for hour_dir in sorted(dt_path.iterdir()):
        for sub in hour_dir.iterdir():
            if "agg_trades" not in sub.name:
                continue
            try:
                t = pq.read_table(sub, columns=["local_receipt_ts_ms", "stream", "p", "q", "m"])
                df = t.to_pandas()
                df = df[df["stream"] == STREAM_AT]
                if len(df) > 0:
                    dfs.append(df)
            except Exception:
                pass
    if not dfs:
        return None
    df = pd.concat(dfs, ignore_index=True)
    df["ts_ms"] = df["local_receipt_ts_ms"].astype(np.int64)
    df["price"] = df["p"].astype(float)
    df["qty"] = df["q"].astype(float)
    df["usd_vol"] = df["price"] * df["qty"]
    # m=True → buyer is maker → sell aggressor; m=False → buy aggressor
    df["buy_vol"] = df["usd_vol"].where(~df["m"], 0)
    df["sell_vol"] = df["usd_vol"].where(df["m"], 0)

    df["ts_s"] = (df["ts_ms"] // 1000) * 1000
    df = df.sort_values("ts_ms")
    agg = df.groupby("ts_s").agg(
        total_vol=("usd_vol", "sum"),
        buy_vol=("buy_vol", "sum"),
        sell_vol=("sell_vol", "sum"),
        n_trades=("qty", "count"),
        vwap=("price", lambda x: np.average(x, weights=df.loc[x.index, "qty"])),
    ).reset_index()
    return agg


# ── 2. 因子计算 ─────────────────────────────────────────────────

def compute_factors(bt_1s, at_1s):
    """在 1s 分辨率上合并 book_ticker 和 agg_trades，计算因子"""
    # 合并
    merged = bt_1s.merge(at_1s, on="ts_s", how="left")
    merged = merged.sort_values("ts_s").reset_index(drop=True)

    # 填充 agg_trades 缺失秒（无交易 → 0）
    for col in ["total_vol", "buy_vol", "sell_vol", "n_trades"]:
        merged[col] = merged[col].fillna(0)
    merged["vwap"] = merged["vwap"].ffill()

    # ── Factor 1: OBI（已在 bt_1s 中计算）──
    # obi 列已存在，取多尺度滚动均值
    merged["obi_10s"] = merged["obi"].rolling(10, min_periods=1).mean()
    merged["obi_30s"] = merged["obi"].rolling(30, min_periods=1).mean()
    merged["obi_60s"] = merged["obi"].rolling(60, min_periods=1).mean()

    # ── Factor 2: Trade Flow Imbalance ──
    for w in [10, 30, 60, 300]:
        buy_sum = merged["buy_vol"].rolling(w, min_periods=1).sum()
        sell_sum = merged["sell_vol"].rolling(w, min_periods=1).sum()
        total = buy_sum + sell_sum
        merged[f"tfi_{w}s"] = (buy_sum - sell_sum) / total.replace(0, np.nan)

    # ── Factor 3: Mid Price Momentum ──
    for w in [10, 30, 60, 300]:
        merged[f"mom_{w}s"] = merged["mid"].pct_change(w)

    # ── Factor 4: Volume Surge Ratio ──
    vol_5m = merged["total_vol"].rolling(300, min_periods=60).mean()
    for w in [10, 30, 60]:
        vol_short = merged["total_vol"].rolling(w, min_periods=1).mean()
        merged[f"vsur_{w}s"] = vol_short / vol_5m.replace(0, np.nan)

    # ── Factor 5: Spread 变化率 ──
    spread_ma = merged["spread"].rolling(300, min_periods=60).mean()
    merged["sprd_ratio"] = merged["spread"] / spread_ma.replace(0, np.nan)
    merged["sprd_chg_10s"] = merged["spread"].pct_change(10)
    merged["sprd_chg_30s"] = merged["spread"].pct_change(30)

    # ── Micro Price ──
    total_qty = merged["bid_qty"] + merged["ask_qty"]
    merged["micro_price"] = merged["mid"] + merged["spread"] * (
        merged["bid_qty"] / total_qty.replace(0, np.nan) - 0.5
    )

    return merged


# ── 3. 构建 5min 窗口样本 ───────────────────────────────────────

def build_5min_samples(merged):
    """
    每 300s 一个窗口，特征取窗口开始时刻的因子值，
    目标取窗口末 mid 相对窗口初 mid 的涨跌。
    """
    # 以 300s (5min) 为窗口对齐
    merged["window"] = (merged["ts_s"] // 300_000) * 300_000

    samples = []
    factor_cols = [c for c in merged.columns if any(
        c.startswith(p) for p in ["obi_", "tfi_", "mom_", "vsur_", "sprd_", "micro"]
    )] + ["obi", "spread", "mid"]

    for win_start, grp in merged.groupby("window"):
        if len(grp) < 60:  # 至少 60s 数据
            continue
        # 窗口开始时的因子（取前 5s 平均以减少噪声）
        start_rows = grp.head(5)
        end_rows = grp.tail(5)

        mid_start = start_rows["mid"].iloc[0]
        mid_end = end_rows["mid"].iloc[-1]
        if mid_start == 0:
            continue

        ret_5m = (mid_end - mid_start) / mid_start
        target = 1 if ret_5m > 0 else 0  # Up=1, Down=0

        row = {"window_ts": win_start, "ret_5m": ret_5m, "target": target}
        for col in factor_cols:
            vals = start_rows[col].dropna()
            row[col] = vals.mean() if len(vals) > 0 else np.nan
        samples.append(row)

    return pd.DataFrame(samples)


# ── 4. 评估因子预测力 ───────────────────────────────────────────

def evaluate_factors(df):
    """计算每个因子的 IC、方向准确率、分桶收益"""
    factor_cols = [c for c in df.columns if any(
        c.startswith(p) for p in ["obi_", "tfi_", "mom_", "vsur_", "sprd_"]
    )]

    results = []
    for col in sorted(factor_cols):
        valid = df.dropna(subset=[col, "ret_5m"])
        if len(valid) < 50:
            continue

        # IC (Pearson on ranks — avoids scipy dependency)
        rank_factor = valid[col].rank()
        rank_ret = valid["ret_5m"].rank()
        ic = rank_factor.corr(rank_ret)

        # 方向准确率
        signal_up = valid[col] > 0
        actual_up = valid["target"] == 1
        # 对于动量/TFI: 正值 → 预测涨；对于 OBI: 正值(bid>ask) → 预测涨
        agree = (signal_up == actual_up).mean()
        # 取 agree 和 1-agree 的较大值（因子可能反向）
        accuracy = max(agree, 1 - agree)
        direction = "same" if agree >= 0.5 else "inverse"

        # 分位数收益
        try:
            q5 = pd.qcut(valid[col], 5, labels=False, duplicates="drop")
            quintile_rets = valid.groupby(q5)["ret_5m"].mean()
            q1_ret = quintile_rets.iloc[0] if len(quintile_rets) >= 5 else np.nan
            q5_ret = quintile_rets.iloc[-1] if len(quintile_rets) >= 5 else np.nan
            spread_q5_q1 = q5_ret - q1_ret if not np.isnan(q5_ret) else np.nan
        except Exception:
            spread_q5_q1 = np.nan

        results.append({
            "factor": col,
            "n_samples": len(valid),
            "IC": round(ic, 4),
            "abs_IC": round(abs(ic), 4),
            "accuracy": round(accuracy * 100, 2),
            "direction": direction,
            "Q5-Q1_spread": round(spread_q5_q1 * 10000, 2) if not np.isnan(spread_q5_q1) else None,
        })

    return pd.DataFrame(results).sort_values("abs_IC", ascending=False)


def _standardize(X):
    mu = np.nanmean(X, axis=0)
    sd = np.nanstd(X, axis=0)
    sd[sd == 0] = 1
    return (X - mu) / sd, mu, sd


def _logistic_fit(X, y, lr=0.05, n_iter=500, reg=0.01):
    """极简 logistic regression (numpy only)"""
    n, d = X.shape
    w = np.zeros(d)
    b = 0.0
    for _ in range(n_iter):
        z = X @ w + b
        z = np.clip(z, -30, 30)
        pred = 1 / (1 + np.exp(-z))
        err = pred - y
        w -= lr * (X.T @ err / n + reg * w)
        b -= lr * err.mean()
    return w, b


def _logistic_predict(X, w, b):
    z = np.clip(X @ w + b, -30, 30)
    return (1 / (1 + np.exp(-z)) > 0.5).astype(int)


def evaluate_combined(df):
    """用纯 numpy logistic regression 测试因子组合"""
    feature_sets = {
        "OBI_only": ["obi_10s", "obi_30s", "obi_60s"],
        "TFI_only": ["tfi_10s", "tfi_30s", "tfi_60s", "tfi_300s"],
        "MOM_only": ["mom_10s", "mom_30s", "mom_60s", "mom_300s"],
        "VSUR_only": ["vsur_10s", "vsur_30s", "vsur_60s"],
        "SPRD_only": ["sprd_ratio", "sprd_chg_10s", "sprd_chg_30s"],
        "ALL_factors": [c for c in df.columns if any(
            c.startswith(p) for p in ["obi_", "tfi_", "mom_", "vsur_", "sprd_"]
        )],
    }

    results = []
    for name, features in feature_sets.items():
        valid_features = [f for f in features if f in df.columns]
        sub = df.dropna(subset=valid_features + ["target"]).reset_index(drop=True)
        if len(sub) < 100:
            continue

        X = sub[valid_features].values.astype(float)
        y = sub["target"].values.astype(float)

        # 时间序列 5-fold CV
        n = len(X)
        fold_size = n // 6  # 前 1/6 至少做训练
        accs = []
        for i in range(5):
            split = fold_size * (i + 1)
            end = min(split + fold_size, n)
            X_train, y_train = X[:split], y[:split]
            X_test, y_test = X[split:end], y[split:end]
            if len(X_test) < 20:
                continue

            X_train_s, mu, sd = _standardize(X_train)
            X_test_s = (X_test - mu) / sd
            # 替换 nan
            X_train_s = np.nan_to_num(X_train_s, 0)
            X_test_s = np.nan_to_num(X_test_s, 0)

            w, b = _logistic_fit(X_train_s, y_train)
            preds = _logistic_predict(X_test_s, w, b)
            acc = (preds == y_test).mean()
            accs.append(acc)

        if not accs:
            continue

        results.append({
            "feature_set": name,
            "n_features": len(valid_features),
            "n_samples": len(sub),
            "cv_accuracy_mean": round(np.mean(accs) * 100, 2),
            "cv_accuracy_std": round(np.std(accs) * 100, 2),
            "cv_accuracy_min": round(np.min(accs) * 100, 2),
            "cv_accuracy_max": round(np.max(accs) * 100, 2),
        })

    return pd.DataFrame(results).sort_values("cv_accuracy_mean", ascending=False)


# ── 5. 主流程 ────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  BTC 5min 方向预测因子测试")
    print("=" * 70)

    all_samples = []
    for dt_str in ALL_DATES:
        print(f"\n  处理 {dt_str}...", end=" ", flush=True)
        bt = load_book_ticker_day(dt_str)
        if bt is None:
            print("无 book_ticker 数据")
            continue
        at = load_agg_trades_day(dt_str)
        if at is None:
            print("无 agg_trades 数据")
            continue

        merged = compute_factors(bt, at)
        samples = build_5min_samples(merged)
        print(f"book_ticker {len(bt)} ticks, agg_trades {len(at)} ticks → {len(samples)} 个 5min 窗口")
        all_samples.append(samples)

    if not all_samples:
        print("无数据！")
        return

    df = pd.concat(all_samples, ignore_index=True)
    df = df.sort_values("window_ts").reset_index(drop=True)
    print(f"\n  总样本: {len(df)} 个 5min 窗口")
    print(f"  Up/Down 分布: Up={df['target'].sum()} ({df['target'].mean()*100:.1f}%), Down={len(df)-df['target'].sum()}")
    print(f"  平均 5min 收益: {df['ret_5m'].mean()*10000:.2f} bps")

    # ── 单因子评估 ──
    print(f"\n{'='*70}")
    print(f"  单因子预测力排名 (IC / 准确率)")
    print(f"{'='*70}")
    factor_results = evaluate_factors(df)
    print(factor_results.to_string(index=False))

    # ── 因子组合评估 ──
    print(f"\n{'='*70}")
    print(f"  因子组合预测力 (Logistic Regression, 时间序列 5-fold CV)")
    print(f"{'='*70}")
    combined_results = evaluate_combined(df)
    print(combined_results.to_string(index=False))

    # 保存
    df.to_csv(OUT_DIR / "factor_samples.csv", index=False)
    factor_results.to_csv(OUT_DIR / "factor_ic_ranking.csv", index=False)
    combined_results.to_csv(OUT_DIR / "factor_combination_accuracy.csv", index=False)
    print(f"\n  结果已保存到: {OUT_DIR}/")


if __name__ == "__main__":
    main()
