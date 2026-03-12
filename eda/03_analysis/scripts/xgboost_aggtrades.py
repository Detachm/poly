#!/usr/bin/env python3
"""
XGBoost 5min 方向预测 — aggTrades 版本
=========================================
从 Binance 历史 aggTrades 提取 tick 级特征（TFI、OFI、momentum 等），
结合 klines 的 OHLCV 特征，训练 XGBoost 方向分类器。

处理流程:
1. 每月 aggTrades CSV (~50M 行) → 1s 重采样
2. 计算滚动因子 (TFI/MOM 等，多尺度)
3. 在 5min 边界采样特征
4. 合并 klines OHLCV 特征
5. XGBoost/LightGBM 训练

用法:
    python eda/03_analysis/scripts/xgboost_aggtrades.py
    python eda/03_analysis/scripts/xgboost_aggtrades.py --start 2025-01 --end 2026-03
"""

import argparse
import glob
import os
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from sklearn.metrics import accuracy_score, classification_report, log_loss
from sklearn.model_selection import TimeSeriesSplit

HIST_DIR = Path("/vault/core/data/poly/historical/binance")
OUT_DIR = Path("eda/03_analysis/output/xgboost_baseline")
CACHE_DIR = OUT_DIR / "feature_cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

AGGTRADE_COLS = [
    "agg_trade_id", "price", "qty", "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker", "is_best_match",
]

KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "n_trades",
    "taker_buy_vol", "taker_buy_quote_vol", "ignore",
]


# ── 1. aggTrades → 1s bars → 5min features ──────────────────────

def process_aggtrades_month(filepath: str, cache_key: str) -> pd.DataFrame:
    """处理一个月的 aggTrades，返回 5min 级别特征。带缓存。"""
    cache_path = CACHE_DIR / f"{cache_key}.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    print(f"    处理 {os.path.basename(filepath)}...")

    # 分块读取，减少内存
    chunks = []
    for chunk in pd.read_csv(
        filepath, header=None, names=AGGTRADE_COLS,
        dtype={"price": float, "qty": float, "is_buyer_maker": str},
        usecols=["price", "qty", "transact_time", "is_buyer_maker"],
        chunksize=5_000_000,
    ):
        # 统一时间戳为毫秒
        chunk["transact_time"] = pd.to_numeric(chunk["transact_time"], errors="coerce")
        mask = chunk["transact_time"] > 1e14
        if mask.any():
            chunk.loc[mask, "transact_time"] = chunk.loc[mask, "transact_time"] // 1000

        # is_buyer_maker: "True" = 卖方主动 (sell), "False" = 买方主动 (buy)
        chunk["is_sell"] = chunk["is_buyer_maker"].str.lower() == "true"
        chunk["value"] = chunk["price"] * chunk["qty"]
        chunk["buy_val"] = np.where(~chunk["is_sell"], chunk["value"], 0)
        chunk["sell_val"] = np.where(chunk["is_sell"], chunk["value"], 0)

        # 1s 重采样
        chunk["ts_s"] = (chunk["transact_time"] // 1000).astype("int64")  # 秒级 unix
        sec_agg = chunk.groupby("ts_s").agg(
            vwap=("price", lambda x: np.average(x, weights=chunk.loc[x.index, "qty"])),
            last_price=("price", "last"),
            total_val=("value", "sum"),
            buy_val=("buy_val", "sum"),
            sell_val=("sell_val", "sum"),
            n_trades=("price", "count"),
            total_qty=("qty", "sum"),
        ).reset_index()

        chunks.append(sec_agg)

    if not chunks:
        return pd.DataFrame()

    # 合并所有 chunk 的 1s bars
    sec_df = pd.concat(chunks, ignore_index=True)
    # 同一秒可能跨 chunk，需要再聚合
    sec_df = sec_df.groupby("ts_s").agg(
        last_price=("last_price", "last"),
        total_val=("total_val", "sum"),
        buy_val=("buy_val", "sum"),
        sell_val=("sell_val", "sum"),
        n_trades=("n_trades", "sum"),
        total_qty=("total_qty", "sum"),
    ).reset_index().sort_values("ts_s")

    # 填充缺失秒（用前值）
    full_range = np.arange(sec_df["ts_s"].min(), sec_df["ts_s"].max() + 1)
    sec_df = sec_df.set_index("ts_s").reindex(full_range).ffill()
    sec_df.index.name = "ts_s"
    # 缺失秒的成交量设为 0
    for col in ["total_val", "buy_val", "sell_val", "n_trades", "total_qty"]:
        sec_df[col] = sec_df[col].fillna(0)
    sec_df = sec_df.reset_index()

    # 计算 1s 级别滚动因子
    features_5min = _compute_5min_features(sec_df)

    # 缓存
    features_5min.to_parquet(cache_path, index=False)
    print(f"    → {len(features_5min)} 个 5min 窗口")
    return features_5min


def _compute_5min_features(sec_df: pd.DataFrame) -> pd.DataFrame:
    """从 1s bars 计算 5min 边界的特征。"""
    price = sec_df["last_price"].values
    buy = sec_df["buy_val"].values
    sell = sec_df["sell_val"].values
    total = sec_df["total_val"].values
    n_trades = sec_df["n_trades"].values
    ts = sec_df["ts_s"].values

    n = len(sec_df)

    # 预计算滚动和 (用 cumsum 加速)
    cum_buy = np.cumsum(buy)
    cum_sell = np.cumsum(sell)
    cum_total = np.cumsum(total)
    cum_ntrades = np.cumsum(n_trades)

    def rolling_sum(cum, window):
        result = np.empty(n)
        result[:window] = cum[:window]
        result[window:] = cum[window:] - cum[:-window]
        return result

    # TFI at multiple scales (using cumsum for speed)
    tfi = {}
    for w in [5, 10, 30, 60, 300]:
        b = rolling_sum(cum_buy, w)
        s = rolling_sum(cum_sell, w)
        t = b + s
        tfi[f"tfi_{w}s"] = np.where(t > 0, (b - s) / t, 0)

    # Momentum at multiple scales
    mom = {}
    for w in [5, 10, 30, 60, 300]:
        shifted = np.roll(price, w)
        shifted[:w] = price[:w]
        mom[f"mom_{w}s"] = np.where(shifted > 0, (price - shifted) / shifted, 0)

    # Volume surge
    vsur = {}
    for short_w in [10, 30, 60]:
        short = rolling_sum(cum_total, short_w) / short_w
        long_ = rolling_sum(cum_total, 300) / 300
        vsur[f"vsur_{short_w}s"] = np.where(long_ > 0, short / long_, 1)

    # Trade intensity
    for w in [10, 30, 60]:
        short_n = rolling_sum(cum_ntrades, w) / w
        long_n = rolling_sum(cum_ntrades, 300) / 300
        vsur[f"ntrades_ratio_{w}s"] = np.where(long_n > 0, short_n / long_n, 1)

    # 在 5min 边界采样 (每 300s 取一个点)
    # 使用窗口开始时刻的前 5s 平均值作为特征
    window_start_mask = (ts % 300 == 0)
    # 实际取窗口开始前的最后一个时间点的特征值
    indices = np.where(window_start_mask)[0]

    if len(indices) == 0:
        return pd.DataFrame()

    rows = []
    for idx in indices:
        if idx < 300:  # 需要至少 300s 的历史
            continue

        # 取窗口开始时刻的特征（前 5s 均值减噪）
        start = max(0, idx - 5)
        end = idx

        row = {"ts_s": int(ts[idx])}

        # TFI 特征
        for k, v in tfi.items():
            row[k] = np.mean(v[start:end]) if end > start else v[idx]

        # Momentum 特征
        for k, v in mom.items():
            row[k] = np.mean(v[start:end]) if end > start else v[idx]

        # Volume surge 特征
        for k, v in vsur.items():
            row[k] = np.mean(v[start:end]) if end > start else v[idx]

        # 目标：5min 后价格涨跌
        future_idx = idx + 300
        if future_idx < n:
            row["future_price"] = price[future_idx]
            row["current_price"] = price[idx]
            row["target"] = int(price[future_idx] > price[idx])
        else:
            row["future_price"] = np.nan
            row["current_price"] = price[idx]
            row["target"] = np.nan

        rows.append(row)

    return pd.DataFrame(rows)


# ── 2. klines OHLCV 特征 ────────────────────────────────────────

def load_klines_features(symbol: str) -> pd.DataFrame:
    """加载 klines 并计算 OHLCV 特征，按 ts_s 索引。"""
    sym_dir = HIST_DIR / symbol.lower() / "klines_5m"
    files = sorted(glob.glob(str(sym_dir / f"{symbol.upper()}-5m-*.csv")))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_csv(f, header=None, names=KLINE_COLS)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    for col in ["open", "high", "low", "close", "volume", "taker_buy_vol"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["n_trades"] = pd.to_numeric(df["n_trades"], errors="coerce")

    # 统一时间戳为秒
    mask = df["open_time"] > 1e14
    if mask.any():
        df.loc[mask, "open_time"] = df.loc[mask, "open_time"] // 1000
    df["ts_s"] = (df["open_time"] // 1000).astype("int64")
    df = df.sort_values("ts_s").drop_duplicates("ts_s").reset_index(drop=True)

    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    vol = df["volume"].values
    tbv = df["taker_buy_vol"].values

    feat = pd.DataFrame({"ts_s": df["ts_s"].values})

    # IMPORTANT: 所有 klines 特征用 shift(1) 取前一个 bar 的数据，
    # 因为当前 bar 的 OHLCV 包含了窗口内的信息（即目标信息）。
    # close_prev = 上一个 bar 的 close price（= 当前窗口开始时的价格）
    cs = pd.Series(close).shift(1)
    os_ = pd.Series(open_).shift(1)
    hs = pd.Series(high).shift(1)
    ls = pd.Series(low).shift(1)
    vs = pd.Series(vol).shift(1)
    tbvs = pd.Series(tbv).shift(1)

    # 多周期收益 (基于前一个 bar 的 close)
    for lag in [1, 2, 3, 6, 12, 24, 48]:
        feat[f"kl_ret_{lag}"] = cs.pct_change(lag).values

    # 波动率
    ret1 = cs.pct_change()
    for win in [6, 12, 24]:
        feat[f"kl_vol_{win}"] = ret1.rolling(win).std().values

    # K线形态 (前一个 bar 的形态)
    body = cs - os_
    rng = hs - ls
    with np.errstate(divide="ignore", invalid="ignore"):
        feat["kl_body_ratio"] = np.where(rng > 0, body / rng, 0).astype(float)
        feat["kl_upper_shadow"] = np.where(rng > 0, (hs - pd.concat([os_, cs], axis=1).max(axis=1)) / rng, 0).astype(float)
        feat["kl_lower_shadow"] = np.where(rng > 0, (pd.concat([os_, cs], axis=1).min(axis=1) - ls) / rng, 0).astype(float)
        feat["kl_hl_ratio"] = np.where(cs > 0, rng / cs, 0).astype(float)

    # TFI from klines (前一个 bar)
    with np.errstate(divide="ignore", invalid="ignore"):
        taker_ratio = np.where(vs > 0, tbvs / vs, 0.5)
    feat["kl_tfi"] = (2 * taker_ratio - 1).astype(float)

    # 成交量变化 (前一个 bar)
    for win in [6, 12, 24]:
        feat[f"kl_vol_ratio_{win}"] = (vs / vs.rolling(win).mean()).values

    # 价格位置
    for win in [12, 48]:
        rh = cs.rolling(win).max()
        rl = cs.rolling(win).min()
        r = rh - rl
        feat[f"kl_price_pos_{win}"] = np.where(r > 0, (cs - rl) / r, 0.5).astype(float)

    # 时间特征 (这些不需要 shift，时间本身不泄露)
    dt = pd.to_datetime(df["ts_s"], unit="s", utc=True)
    hour = dt.dt.hour.values
    dow = dt.dt.dayofweek.values
    feat["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    feat["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    feat["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    feat["dow_cos"] = np.cos(2 * np.pi * dow / 7)

    return feat


# ── 2b. Futures 合约特征 ─────────────────────────────────────────

def load_futures_features(symbol: str) -> pd.DataFrame:
    """加载 futures klines + funding rate，计算合约因子，按 ts_s 索引。"""
    sym_dir = HIST_DIR / symbol.lower()

    # --- Futures klines (5m) ---
    fkl_dir = sym_dir / "futures_klines_5m"
    fkl_files = sorted(glob.glob(str(fkl_dir / f"{symbol.upper()}-5m-*.csv")))
    if not fkl_files:
        print("  [futures] 无 klines 文件")
        return pd.DataFrame()

    fkl_dfs = []
    for f in fkl_files:
        # futures klines 有 header
        try:
            df = pd.read_csv(f, nrows=1)
            has_header = "open_time" in df.columns
        except Exception:
            has_header = False
        if has_header:
            df = pd.read_csv(f)
        else:
            df = pd.read_csv(f, header=None, names=KLINE_COLS)
        fkl_dfs.append(df)

    fkl = pd.concat(fkl_dfs, ignore_index=True)
    for col in ["open", "high", "low", "close", "volume", "taker_buy_vol", "taker_buy_volume"]:
        if col in fkl.columns:
            fkl[col] = pd.to_numeric(fkl[col], errors="coerce")
    # 统一 taker_buy 列名
    if "taker_buy_volume" in fkl.columns and "taker_buy_vol" not in fkl.columns:
        fkl["taker_buy_vol"] = fkl["taker_buy_volume"]

    ot_col = "open_time" if "open_time" in fkl.columns else fkl.columns[0]
    fkl["open_time"] = pd.to_numeric(fkl[ot_col], errors="coerce").astype("int64")
    mask = fkl["open_time"] > 1e14
    if mask.any():
        fkl.loc[mask, "open_time"] = fkl.loc[mask, "open_time"] // 1000
    fkl["ts_s"] = (fkl["open_time"] // 1000).astype("int64")
    fkl = fkl.sort_values("ts_s").drop_duplicates("ts_s").reset_index(drop=True)

    feat = pd.DataFrame({"ts_s": fkl["ts_s"].values})

    # 合约 TFI (shift(1) 避免泄露)
    f_vol = pd.Series(fkl["volume"].values).shift(1)
    f_tbv = pd.Series(fkl["taker_buy_vol"].values).shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        f_ratio = np.where(f_vol > 0, f_tbv / f_vol, 0.5)
    feat["ft_tfi"] = (2 * f_ratio - 1).astype(float)
    tfi_s = pd.Series(feat["ft_tfi"].values)
    for w in [3, 6, 12]:
        feat[f"ft_tfi_{w}"] = tfi_s.rolling(w).mean().values

    # 合约 vs 现货 basis (需要现货 klines)
    spot_dir = sym_dir / "klines_5m"
    spot_files = sorted(glob.glob(str(spot_dir / f"{symbol.upper()}-5m-*.csv")))
    if spot_files:
        s_dfs = []
        for f in spot_files:
            s_dfs.append(pd.read_csv(f, header=None, names=KLINE_COLS))
        spot = pd.concat(s_dfs, ignore_index=True)
        spot["close"] = pd.to_numeric(spot["close"], errors="coerce")
        spot["open_time"] = pd.to_numeric(spot[spot.columns[0]], errors="coerce").astype("int64")
        m = spot["open_time"] > 1e14
        if m.any():
            spot.loc[m, "open_time"] = spot.loc[m, "open_time"] // 1000
        spot["ts_s"] = (spot["open_time"] // 1000).astype("int64")
        spot = spot.sort_values("ts_s").drop_duplicates("ts_s")[["ts_s", "close"]].rename(columns={"close": "spot_close"})

        fkl_basis = fkl[["ts_s", "close"]].rename(columns={"close": "fut_close"}).merge(spot, on="ts_s", how="inner")
        with np.errstate(divide="ignore", invalid="ignore"):
            basis = np.where(fkl_basis["spot_close"] > 0,
                           (fkl_basis["fut_close"] - fkl_basis["spot_close"]) / fkl_basis["spot_close"],
                           0)
        basis_df = pd.DataFrame({"ts_s": fkl_basis["ts_s"].values, "ft_basis": basis.astype(float)})
        # shift(1) 避免用当前 bar 的 close
        basis_df["ft_basis"] = basis_df["ft_basis"].shift(1)
        bs = pd.Series(basis_df["ft_basis"].values)
        basis_df["ft_basis_ma6"] = bs.rolling(6).mean().values
        basis_df["ft_basis_chg"] = bs.diff().values
        feat = feat.merge(basis_df, on="ts_s", how="left")

    # 合约成交量 / 现货成交量比
    f_close = pd.Series(fkl["close"].values).shift(1)
    f_ret = f_close.pct_change()
    for lag in [1, 3, 6]:
        feat[f"ft_ret_{lag}"] = f_close.pct_change(lag).values

    # 合约成交量特征
    fv = pd.Series(fkl["volume"].values).shift(1)
    for w in [6, 12]:
        feat[f"ft_vol_ratio_{w}"] = (fv / fv.rolling(w).mean()).values

    # --- Funding Rate ---
    fr_dir = sym_dir / "futures_fundingRate"
    fr_files = sorted(glob.glob(str(fr_dir / f"{symbol.upper()}-fundingRate-*.csv")))
    if fr_files:
        fr_dfs = []
        for f in fr_files:
            try:
                df = pd.read_csv(f)
                fr_dfs.append(df)
            except Exception:
                pass
        if fr_dfs:
            fr = pd.concat(fr_dfs, ignore_index=True)
            fr["calc_time"] = pd.to_numeric(fr["calc_time"], errors="coerce")
            fr["ts_s"] = (fr["calc_time"] // 1000).astype("int64")
            fr["last_funding_rate"] = pd.to_numeric(fr["last_funding_rate"], errors="coerce")
            fr = fr.sort_values("ts_s").drop_duplicates("ts_s")

            # Funding rate 每 8h 更新，forward fill 到 5min 级别
            fr_feat = fr[["ts_s", "last_funding_rate"]].rename(columns={"last_funding_rate": "funding_rate"})

            # merge_asof: 每个 5min 窗口取最近一次 funding rate
            feat = feat.sort_values("ts_s")
            fr_feat = fr_feat.sort_values("ts_s")
            feat = pd.merge_asof(feat, fr_feat, on="ts_s", direction="backward")

            # funding rate 衍生特征
            if "funding_rate" in feat.columns:
                feat["fr_abs"] = feat["funding_rate"].abs()
                feat["fr_sign"] = np.sign(feat["funding_rate"])

    print(f"  [futures] 特征: {len(feat)} 条, {len([c for c in feat.columns if c != 'ts_s'])} 个因子")
    return feat


# ── 3. 训练 ──────────────────────────────────────────────────────

def train_eval(symbol: str, start_month: str, end_month: str, n_splits: int = 5,
               min_return: float = 0.0, run_optuna: bool = False,
               sweep_returns: bool = False):
    print(f"\n{'='*60}")
    print(f"XGBoost aggTrades: {symbol} 5min 方向预测")
    print(f"{'='*60}")

    # 收集 aggTrades 文件
    sym_dir = HIST_DIR / symbol.lower() / "aggTrades"
    monthly_files = sorted(glob.glob(str(sym_dir / f"{symbol}-aggTrades-????-??.csv")))
    daily_files = sorted(glob.glob(str(sym_dir / f"{symbol}-aggTrades-????-??-??.csv")))
    all_files = monthly_files + daily_files

    if start_month:
        all_files = [f for f in all_files if os.path.basename(f).split("aggTrades-")[1][:7] >= start_month]
    if end_month:
        all_files = [f for f in all_files if os.path.basename(f).split("aggTrades-")[1][:7] <= end_month]

    print(f"  找到 {len(all_files)} 个 aggTrades 文件")

    # 处理每个文件
    tick_features = []
    for filepath in all_files:
        basename = os.path.basename(filepath).replace(".csv", "")
        cache_key = f"{symbol.lower()}_{basename}"
        feat = process_aggtrades_month(filepath, cache_key)
        if len(feat) > 0:
            tick_features.append(feat)

    if not tick_features:
        print("  无可用数据")
        return

    tick_df = pd.concat(tick_features, ignore_index=True)
    tick_df = tick_df.sort_values("ts_s").drop_duplicates("ts_s").reset_index(drop=True)
    print(f"  aggTrades 特征: {len(tick_df)} 个 5min 窗口")

    # 加载 klines 特征
    kl_feat = load_klines_features(symbol)
    print(f"  klines 特征: {len(kl_feat)} 条")

    # 加载 futures 特征
    ft_feat = load_futures_features(symbol)

    # 合并
    merged = tick_df
    if len(kl_feat) > 0:
        merged = merged.merge(kl_feat, on="ts_s", how="inner")
    if len(ft_feat) > 0:
        merged = merged.merge(ft_feat, on="ts_s", how="inner")
    print(f"  合并后: {len(merged)} 条")

    # 保存未过滤的数据（用于 sweep）
    merged_full = merged.copy() if sweep_returns else None

    # (b) 标签噪声过滤
    if min_return > 0 and "future_price" in merged.columns and "current_price" in merged.columns:
        abs_ret = (merged["future_price"] - merged["current_price"]).abs() / merged["current_price"]
        noise_mask = abs_ret >= min_return
        n_before = len(merged)
        merged = merged[noise_mask].reset_index(drop=True)
        print(f"  标签噪声过滤: 去除 |ret| < {min_return:.4%} 的 {n_before - len(merged)} 个样本 "
              f"({(n_before - len(merged))/n_before:.1%})")

    # 准备 X, y
    target = merged["target"]
    drop_cols = ["ts_s", "target", "future_price", "current_price"]
    X = merged.drop(columns=[c for c in drop_cols if c in merged.columns])
    X = X.replace([np.inf, -np.inf], np.nan)

    valid_mask = target.notna() & X.notna().all(axis=1)
    X = X[valid_mask].reset_index(drop=True)
    y = target[valid_mask].astype(int).reset_index(drop=True)

    feature_names = list(X.columns)
    print(f"  特征数: {len(feature_names)}")
    print(f"  样本数: {len(X)}")
    print(f"  Up/Down: {y.sum()} ({y.mean()*100:.1f}%) / {(1-y).sum()} ({(1-y).mean()*100:.1f}%)")

    # CV
    tscv = TimeSeriesSplit(n_splits=n_splits)
    results = {"xgb": [], "lgb": []}
    fold_details = []
    all_y_test = []
    all_xgb_prob = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        xgb_model = xgb.XGBClassifier(
            n_estimators=500,
            max_depth=5,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_weight=100,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            eval_metric="logloss",
            verbosity=0,
            tree_method="hist",
            early_stopping_rounds=50,
        )
        xgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        xgb_pred = xgb_model.predict(X_test)
        xgb_prob = xgb_model.predict_proba(X_test)[:, 1]
        xgb_acc = accuracy_score(y_test, xgb_pred)
        all_y_test.append(y_test.values)
        all_xgb_prob.append(xgb_prob)

        lgb_model = lgb.LGBMClassifier(
            n_estimators=500,
            max_depth=5,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_samples=100,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=42,
            verbose=-1,
        )
        lgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)],
                      callbacks=[lgb.early_stopping(50), lgb.log_evaluation(-1)])
        lgb_pred = lgb_model.predict(X_test)
        lgb_prob = lgb_model.predict_proba(X_test)[:, 1]
        lgb_acc = accuracy_score(y_test, lgb_pred)

        results["xgb"].append(xgb_acc)
        results["lgb"].append(lgb_acc)
        fold_details.append({
            "fold": fold + 1,
            "train_size": len(X_train),
            "test_size": len(X_test),
            "xgb_acc": xgb_acc,
            "lgb_acc": lgb_acc,
        })
        print(f"  Fold {fold+1}: XGB {xgb_acc:.4f} / LGB {lgb_acc:.4f}  "
              f"(train {len(X_train):,} / test {len(X_test):,})")

    xgb_mean = np.mean(results["xgb"])
    xgb_std = np.std(results["xgb"])
    lgb_mean = np.mean(results["lgb"])
    lgb_std = np.std(results["lgb"])

    print(f"\n  XGBoost CV: {xgb_mean:.4f} +/- {xgb_std:.4f}")
    print(f"  LightGBM CV: {lgb_mean:.4f} +/- {lgb_std:.4f}")

    # (c) 概率阈值分析
    all_y_arr = np.concatenate(all_y_test)
    all_xp_arr = np.concatenate(all_xgb_prob)
    print(f"\n  概率阈值分析 (XGBoost, 所有 fold 汇总, N={len(all_y_arr):,}):")
    print(f"  {'阈值':>6} | {'准确率':>7} | {'覆盖率':>6} | {'样本数':>8}")
    print(f"  {'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*8}")
    threshold_results = []
    for thr in [0.50, 0.505, 0.51, 0.52, 0.53, 0.54, 0.55, 0.57, 0.60]:
        mask = (all_xp_arr >= thr) | (all_xp_arr <= 1 - thr)
        if mask.sum() == 0:
            continue
        pred = (all_xp_arr[mask] >= 0.5).astype(int)
        acc = accuracy_score(all_y_arr[mask], pred)
        coverage = mask.mean()
        threshold_results.append({"threshold": thr, "accuracy": acc,
                                  "coverage": coverage, "n_samples": int(mask.sum())})
        print(f"  {thr:.3f}  | {acc:.4f}  | {coverage:6.1%} | {mask.sum():>8,}")

    # 特征重要性
    xgb_imp = pd.DataFrame({
        "feature": feature_names,
        "xgb_importance": xgb_model.feature_importances_,
        "lgb_importance": lgb_model.feature_importances_,
    }).sort_values("xgb_importance", ascending=False)

    print(f"\n  最后 fold XGBoost 分类报告:")
    print(classification_report(y_test, xgb_pred, target_names=["Down", "Up"], digits=4))

    # 保存
    fold_df = pd.DataFrame(fold_details)
    fold_df.to_csv(OUT_DIR / f"{symbol.lower()}_aggtrades_cv.csv", index=False)
    xgb_imp.to_csv(OUT_DIR / f"{symbol.lower()}_aggtrades_importance.csv", index=False)

    best = max(xgb_mean, lgb_mean)
    report = f"""# XGBoost + aggTrades: {symbol} 5min 方向预测

日期: {pd.Timestamp.now().strftime('%Y-%m-%d')}

## 数据
- 样本数: {len(X):,}
- 特征数: {len(feature_names)} (tick 级 {sum(1 for f in feature_names if not f.startswith('kl_') and f not in ('hour_sin','hour_cos','dow_sin','dow_cos'))} + klines {sum(1 for f in feature_names if f.startswith('kl_'))} + 时间 4)
- Up/Down: {y.sum():,} ({y.mean()*100:.1f}%) / {int((1-y).sum()):,} ({(1-y).mean()*100:.1f}%)

## CV 结果 ({n_splits}-fold TimeSeriesSplit)

| Fold | Train | Test | XGBoost | LightGBM |
|------|-------|------|---------|----------|
"""
    for d in fold_details:
        report += f"| {d['fold']} | {d['train_size']:,} | {d['test_size']:,} | {d['xgb_acc']:.4f} | {d['lgb_acc']:.4f} |\n"
    report += f"| **Mean** | | | **{xgb_mean:.4f} +/- {xgb_std:.4f}** | **{lgb_mean:.4f} +/- {lgb_std:.4f}** |\n"

    report += f"\n## Top 20 特征重要性\n\n"
    report += "| 排名 | 特征 | XGB Importance | LGB Importance |\n"
    report += "|------|------|---------------|----------------|\n"
    for rank, (_, row) in enumerate(xgb_imp.head(20).iterrows(), 1):
        report += f"| {rank} | {row['feature']} | {row['xgb_importance']:.4f} | {row['lgb_importance']:.0f} |\n"

    report += f"\n## 与目标差距\n\n"
    report += f"- Baseline 准确率: **{best:.1%}**\n"
    report += f"- 目标准确率: **63%**\n"
    report += f"- 差距: **{0.63 - best:.1%}**\n"

    report += f"\n## 对比\n\n"
    report += f"| 方法 | 准确率 |\n"
    report += f"|------|--------|\n"
    report += f"| Logistic Regression (TFI全家桶, 19天) | 53.3% |\n"
    report += f"| XGBoost (klines OHLCV, 3年) | 51.8% |\n"
    report += f"| **XGBoost (aggTrades+klines, 3年)** | **{best:.1%}** |\n"

    # (c) 阈值分析写入报告
    if threshold_results:
        report += f"\n## 概率阈值分析\n\n"
        report += "| 阈值 | 准确率 | 覆盖率 | 样本数 |\n"
        report += "|------|--------|--------|--------|\n"
        for tr in threshold_results:
            report += f"| {tr['threshold']:.3f} | {tr['accuracy']:.4f} | {tr['coverage']:.1%} | {tr['n_samples']:,} |\n"

    with open(OUT_DIR / f"{symbol.lower()}_aggtrades_report.md", "w") as f:
        f.write(report)
    print(f"\n  报告: {OUT_DIR / f'{symbol.lower()}_aggtrades_report.md'}")

    # (b) min_return 阈值扫描
    if sweep_returns and merged_full is not None:
        print(f"\n{'='*60}")
        print(f"(b) 标签噪声过滤扫描")
        print(f"{'='*60}")
        sweep_thresholds = [0, 0.0001, 0.0002, 0.0005, 0.001]
        print(f"  {'min_return':>12} | {'过滤比例':>8} | {'样本数':>8} | {'XGB_acc':>8} | {'LGB_acc':>8}")
        print(f"  {'-'*12}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}")

        for mr in sweep_thresholds:
            m = merged_full.copy()
            if mr > 0 and "future_price" in m.columns and "current_price" in m.columns:
                abs_ret = (m["future_price"] - m["current_price"]).abs() / m["current_price"]
                m = m[abs_ret >= mr].reset_index(drop=True)
            filtered_pct = 1 - len(m) / len(merged_full)

            t = m["target"]
            dc = ["ts_s", "target", "future_price", "current_price"]
            Xs = m.drop(columns=[c for c in dc if c in m.columns]).replace([np.inf, -np.inf], np.nan)
            vm = t.notna() & Xs.notna().all(axis=1)
            Xs, ys = Xs[vm].reset_index(drop=True), t[vm].astype(int).reset_index(drop=True)

            if len(Xs) < 1000:
                print(f"  {mr:>12.4%} | {filtered_pct:>7.1%} | {len(Xs):>8,} | 样本太少")
                continue

            # 快速 3-fold CV
            tscv3 = TimeSeriesSplit(n_splits=3)
            xgb_scores, lgb_scores = [], []
            for ti, vi in tscv3.split(Xs):
                xm = xgb.XGBClassifier(
                    n_estimators=500, max_depth=5, learning_rate=0.03,
                    subsample=0.8, colsample_bytree=0.7, min_child_weight=100,
                    reg_alpha=0.1, reg_lambda=1.0, random_state=42,
                    eval_metric="logloss", verbosity=0, tree_method="hist",
                    early_stopping_rounds=50,
                )
                xm.fit(Xs.iloc[ti], ys.iloc[ti], eval_set=[(Xs.iloc[vi], ys.iloc[vi])], verbose=False)
                xgb_scores.append(accuracy_score(ys.iloc[vi], xm.predict(Xs.iloc[vi])))

                lm = lgb.LGBMClassifier(
                    n_estimators=500, max_depth=5, learning_rate=0.03,
                    subsample=0.8, colsample_bytree=0.7, min_child_samples=100,
                    reg_alpha=0.1, reg_lambda=1.0, random_state=42, verbose=-1,
                )
                lm.fit(Xs.iloc[ti], ys.iloc[ti], eval_set=[(Xs.iloc[vi], ys.iloc[vi])],
                       callbacks=[lgb.early_stopping(50), lgb.log_evaluation(-1)])
                lgb_scores.append(accuracy_score(ys.iloc[vi], lm.predict(Xs.iloc[vi])))

            print(f"  {mr:>12.4%} | {filtered_pct:>7.1%} | {len(Xs):>8,} | "
                  f"{np.mean(xgb_scores):.4f}  | {np.mean(lgb_scores):.4f}")

    # (e) Optuna 超参搜索
    if run_optuna:
        print(f"\n{'='*60}")
        print(f"(e) Optuna 超参搜索 (30 trials, 3-fold CV)")
        print(f"{'='*60}")
        best_params, best_val = hyperparam_search(X, y, n_trials=30, n_splits=3)


def hyperparam_search(X, y, n_trials: int = 30, n_splits: int = 3):
    """Optuna 超参搜索。"""
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    def objective(trial):
        params = {
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "n_estimators": trial.suggest_int("n_estimators", 100, 800, step=100),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 10, 500),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10, log=True),
        }
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = []
        for train_idx, test_idx in tscv.split(X):
            model = xgb.XGBClassifier(
                **params, random_state=42, eval_metric="logloss",
                verbosity=0, tree_method="hist", early_stopping_rounds=50,
            )
            model.fit(X.iloc[train_idx], y.iloc[train_idx],
                      eval_set=[(X.iloc[test_idx], y.iloc[test_idx])], verbose=False)
            pred = model.predict(X.iloc[test_idx])
            scores.append(accuracy_score(y.iloc[test_idx], pred))
        return np.mean(scores)

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    print(f"\n  Optuna 最优 CV 准确率: {study.best_value:.4f}")
    print(f"  最优超参:")
    for k, v in study.best_params.items():
        if isinstance(v, float):
            print(f"    {k}: {v:.6f}")
        else:
            print(f"    {k}: {v}")

    # 用最优超参跑 5-fold 验证
    print(f"\n  用最优超参跑 5-fold 验证...")
    tscv5 = TimeSeriesSplit(n_splits=5)
    final_scores = []
    for train_idx, test_idx in tscv5.split(X):
        model = xgb.XGBClassifier(
            **study.best_params, random_state=42, eval_metric="logloss",
            verbosity=0, tree_method="hist", early_stopping_rounds=50,
        )
        model.fit(X.iloc[train_idx], y.iloc[train_idx],
                  eval_set=[(X.iloc[test_idx], y.iloc[test_idx])], verbose=False)
        pred = model.predict(X.iloc[test_idx])
        s = accuracy_score(y.iloc[test_idx], pred)
        final_scores.append(s)
        print(f"    Fold {len(final_scores)}: {s:.4f}")
    print(f"  5-fold 最终结果: {np.mean(final_scores):.4f} +/- {np.std(final_scores):.4f}")

    return study.best_params, study.best_value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", default="", help="起始月份 YYYY-MM")
    parser.add_argument("--end", default="", help="结束月份 YYYY-MM")
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--min-return", type=float, default=0.0,
                        help="最小 |return| 过滤阈值")
    parser.add_argument("--optuna", action="store_true", help="运行超参搜索")
    parser.add_argument("--sweep", action="store_true", help="扫描 min_return 阈值")
    parser.add_argument("--all", action="store_true", help="运行所有实验 (sweep + optuna)")
    args = parser.parse_args()

    run_optuna = args.optuna or getattr(args, 'all', False)
    sweep = args.sweep or getattr(args, 'all', False)

    train_eval(args.symbol, args.start, args.end, args.splits,
               min_return=getattr(args, 'min_return', 0.0),
               run_optuna=run_optuna, sweep_returns=sweep)


if __name__ == "__main__":
    main()
