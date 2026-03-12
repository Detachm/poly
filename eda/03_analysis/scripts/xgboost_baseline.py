#!/usr/bin/env python3
"""
XGBoost 5min 方向预测 Baseline
================================
用 Binance 历史 klines (5m OHLCV + taker_buy_vol) 训练方向分类器。

数据源: data.binance.vision 下载的 5m klines CSV
特征: 多周期收益、波动率、成交量、K线形态、主动买入比例(TFI)
目标: 下一个 5min bar 涨 (1) / 跌 (0)

用法:
    python eda/03_analysis/scripts/xgboost_baseline.py
    python eda/03_analysis/scripts/xgboost_baseline.py --symbol ETHUSDT --years 1
"""

import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import accuracy_score, classification_report, log_loss
from sklearn.model_selection import TimeSeriesSplit

HIST_DIR = Path("/vault/core/data/poly/historical/binance")
OUT_DIR = Path("eda/03_analysis/output/xgboost_baseline")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Binance klines CSV columns (no header)
KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "n_trades",
    "taker_buy_vol", "taker_buy_quote_vol", "ignore",
]


# ── 1. 数据加载 ─────────────────────────────────────────────────

def load_klines(symbol: str) -> pd.DataFrame:
    """加载全部 klines CSV，合并排序。"""
    sym_dir = HIST_DIR / symbol.lower() / "klines_5m"
    files = sorted(glob.glob(str(sym_dir / f"{symbol.upper()}-5m-*.csv")))
    if not files:
        raise FileNotFoundError(f"未找到 klines 文件: {sym_dir}")

    dfs = []
    for f in files:
        df = pd.read_csv(f, header=None, names=KLINE_COLS)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)

    # 转换类型
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                 "taker_buy_vol", "taker_buy_quote_vol"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["n_trades"] = pd.to_numeric(df["n_trades"], errors="coerce").astype("Int64")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")

    # 统一为毫秒：2023 以前是 13 位 ms，2025+ 有些文件是 16 位 us
    mask = df["open_time"] > 1e14
    if mask.any():
        df.loc[mask, "open_time"] = df.loc[mask, "open_time"] // 1000

    df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    print(f"  加载 {symbol}: {len(df)} 条 ({df['datetime'].min()} ~ {df['datetime'].max()})")
    return df


# ── 2. 特征工程 ─────────────────────────────────────────────────

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """从 OHLCV + taker_buy_vol 构建预测特征。"""
    f = pd.DataFrame(index=df.index)

    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    vol = df["volume"].values
    taker_buy = df["taker_buy_vol"].values
    n_trades = df["n_trades"].values.astype(float)

    # --- 收益率 (多周期) ---
    for lag in [1, 2, 3, 6, 12, 24, 48, 96]:
        f[f"ret_{lag}"] = pd.Series(close).pct_change(lag).values

    # --- 波动率 (收益率滚动标准差) ---
    ret1 = pd.Series(close).pct_change()
    for win in [6, 12, 24, 48]:
        f[f"vol_{win}"] = ret1.rolling(win).std().values

    # --- TFI (Trade Flow Imbalance) from taker_buy_vol ---
    # taker_buy_ratio = taker_buy_vol / total_vol; TFI = 2*ratio - 1
    with np.errstate(divide="ignore", invalid="ignore"):
        taker_ratio = np.where(vol > 0, taker_buy / vol, 0.5)
    f["tfi_1"] = 2 * taker_ratio - 1  # 当前 bar 的 TFI

    # 多周期 TFI 均值
    tfi_s = pd.Series(f["tfi_1"].values)
    for win in [3, 6, 12, 24]:
        f[f"tfi_{win}"] = tfi_s.rolling(win).mean().values

    # --- 成交量特征 ---
    vol_s = pd.Series(vol)
    for win in [6, 12, 24, 48]:
        ma = vol_s.rolling(win).mean()
        f[f"vol_ratio_{win}"] = (vol_s / ma).values

    # 成交量变化率
    for lag in [1, 3, 6]:
        f[f"vol_chg_{lag}"] = vol_s.pct_change(lag).values

    # 成交笔数
    n_s = pd.Series(n_trades)
    for win in [6, 12]:
        ma = n_s.rolling(win).mean()
        f[f"ntrades_ratio_{win}"] = (n_s / ma).values

    # --- K线形态特征 ---
    body = close - open_
    candle_range = high - low
    with np.errstate(divide="ignore", invalid="ignore"):
        f["body_ratio"] = np.where(candle_range > 0, body / candle_range, 0)
        f["upper_shadow"] = np.where(candle_range > 0, (high - np.maximum(open_, close)) / candle_range, 0)
        f["lower_shadow"] = np.where(candle_range > 0, (np.minimum(open_, close) - low) / candle_range, 0)
    f["hl_ratio"] = np.where(close > 0, candle_range / close, 0)  # 振幅

    # --- 价格位置 (在近期高低点中的位置) ---
    close_s = pd.Series(close)
    for win in [12, 48]:
        roll_high = close_s.rolling(win).max()
        roll_low = close_s.rolling(win).min()
        rng = roll_high - roll_low
        f[f"price_pos_{win}"] = np.where(rng > 0, (close_s - roll_low) / rng, 0.5).astype(float)

    # --- 时间特征 (cyclical) ---
    if "datetime" in df.columns:
        hour = df["datetime"].dt.hour.values
        dow = df["datetime"].dt.dayofweek.values
        f["hour_sin"] = np.sin(2 * np.pi * hour / 24)
        f["hour_cos"] = np.cos(2 * np.pi * hour / 24)
        f["dow_sin"] = np.sin(2 * np.pi * dow / 7)
        f["dow_cos"] = np.cos(2 * np.pi * dow / 7)

    return f


# ── 3. 训练与评估 ─────────────────────────────────────────────

def train_eval(symbol: str, n_splits: int = 5):
    """时间序列 CV 训练 XGBoost + LightGBM baseline。"""
    print(f"\n{'='*60}")
    print(f"XGBoost Baseline: {symbol} 5min 方向预测")
    print(f"{'='*60}")

    df = load_klines(symbol)
    features = build_features(df)

    # 目标：下一个 bar 涨 (1) / 跌 (0)
    target = (df["close"].shift(-1) > df["close"]).astype(int)

    # 替换 inf 并去除 NaN
    data = features.copy()
    data = data.replace([np.inf, -np.inf], np.nan)
    data["target"] = target.values
    data = data.dropna()

    X = data.drop(columns=["target"])
    y = data["target"]

    feature_names = list(X.columns)
    print(f"  特征数: {len(feature_names)}")
    print(f"  样本数: {len(X)}")
    print(f"  Up/Down: {y.sum()} ({y.mean()*100:.1f}%) / {(1-y).sum()} ({(1-y).mean()*100:.1f}%)")

    # 时间序列 CV
    tscv = TimeSeriesSplit(n_splits=n_splits)

    results = {"xgb": [], "lgb": []}
    fold_details = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        # XGBoost
        xgb_model = xgb.XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=50,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            eval_metric="logloss",
            verbosity=0,
        )
        xgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        xgb_pred = xgb_model.predict(X_test)
        xgb_prob = xgb_model.predict_proba(X_test)[:, 1]
        xgb_acc = accuracy_score(y_test, xgb_pred)
        xgb_ll = log_loss(y_test, xgb_prob)

        # LightGBM
        lgb_model = lgb.LGBMClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_samples=50,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            verbose=-1,
        )
        lgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
        lgb_pred = lgb_model.predict(X_test)
        lgb_prob = lgb_model.predict_proba(X_test)[:, 1]
        lgb_acc = accuracy_score(y_test, lgb_pred)
        lgb_ll = log_loss(y_test, lgb_prob)

        results["xgb"].append(xgb_acc)
        results["lgb"].append(lgb_acc)

        fold_details.append({
            "fold": fold + 1,
            "train_size": len(X_train),
            "test_size": len(X_test),
            "xgb_acc": xgb_acc,
            "xgb_logloss": xgb_ll,
            "lgb_acc": lgb_acc,
            "lgb_logloss": lgb_ll,
        })

        print(f"  Fold {fold+1}: XGB {xgb_acc:.4f} / LGB {lgb_acc:.4f}  "
              f"(train {len(X_train):,} / test {len(X_test):,})")

    # 汇总
    xgb_mean = np.mean(results["xgb"])
    xgb_std = np.std(results["xgb"])
    lgb_mean = np.mean(results["lgb"])
    lgb_std = np.std(results["lgb"])

    print(f"\n  XGBoost CV: {xgb_mean:.4f} +/- {xgb_std:.4f}")
    print(f"  LightGBM CV: {lgb_mean:.4f} +/- {lgb_std:.4f}")

    # 最后一个 fold 的特征重要性
    xgb_imp = pd.DataFrame({
        "feature": feature_names,
        "xgb_importance": xgb_model.feature_importances_,
        "lgb_importance": lgb_model.feature_importances_,
    }).sort_values("xgb_importance", ascending=False)

    # 最后一个 fold 的分类报告
    print(f"\n  最后 fold XGBoost 分类报告:")
    print(classification_report(y_test, xgb_pred, target_names=["Down", "Up"], digits=4))

    # 保存结果
    fold_df = pd.DataFrame(fold_details)
    fold_df.to_csv(OUT_DIR / f"{symbol.lower()}_cv_results.csv", index=False)
    xgb_imp.to_csv(OUT_DIR / f"{symbol.lower()}_feature_importance.csv", index=False)

    # 生成报告
    report = f"""# XGBoost Baseline: {symbol} 5min 方向预测

日期: {pd.Timestamp.now().strftime('%Y-%m-%d')}

## 数据
- 样本数: {len(X):,}
- 特征数: {len(feature_names)}
- Up/Down: {y.sum():,} ({y.mean()*100:.1f}%) / {int((1-y).sum()):,} ({(1-y).mean()*100:.1f}%)
- 时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}

## CV 结果 ({n_splits}-fold TimeSeriesSplit)

| Fold | Train | Test | XGBoost | LightGBM |
|------|-------|------|---------|----------|
"""
    for d in fold_details:
        report += f"| {d['fold']} | {d['train_size']:,} | {d['test_size']:,} | {d['xgb_acc']:.4f} | {d['lgb_acc']:.4f} |\n"
    report += f"| **Mean** | | | **{xgb_mean:.4f} +/- {xgb_std:.4f}** | **{lgb_mean:.4f} +/- {lgb_std:.4f}** |\n"

    report += f"\n## Top 15 特征重要性 (XGBoost)\n\n"
    report += "| 排名 | 特征 | XGB Importance | LGB Importance |\n"
    report += "|------|------|---------------|----------------|\n"
    for i, row in xgb_imp.head(15).iterrows():
        report += f"| {xgb_imp.index.get_loc(i)+1} | {row['feature']} | {row['xgb_importance']:.4f} | {row['lgb_importance']:.0f} |\n"

    report += f"\n## 与目标差距\n\n"
    report += f"- Baseline 准确率: **{max(xgb_mean, lgb_mean):.1%}**\n"
    report += f"- 目标准确率: **63%**\n"
    report += f"- 差距: **{0.63 - max(xgb_mean, lgb_mean):.1%}**\n"

    with open(OUT_DIR / f"{symbol.lower()}_report.md", "w") as f:
        f.write(report)

    print(f"\n  报告已保存: {OUT_DIR / f'{symbol.lower()}_report.md'}")

    return results


def main():
    parser = argparse.ArgumentParser(description="XGBoost 5min 方向预测 baseline")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT"], help="交易对")
    parser.add_argument("--splits", type=int, default=5, help="CV folds")
    args = parser.parse_args()

    for symbol in args.symbols:
        train_eval(symbol, args.splits)


if __name__ == "__main__":
    main()
