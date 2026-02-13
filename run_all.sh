#!/bin/bash
# 一键跑通流程：创建目录 → 可选 discovery 一次 → 后台启动各录制器（或提示用 supervisord）
# 用法: ./run_all.sh [--skip-discovery]

set -e
cd "$(dirname "$0")"
DATA_ROOT="${POLY_DATA_DIR:-/vault/core/data/poly}"
RAW_ROOT="$DATA_ROOT/raw"
mkdir -p "$RAW_ROOT/polymarket" "$RAW_ROOT/binance" "$RAW_ROOT/chainlink" "$RAW_ROOT/tracker_0x8dxd" logs

echo "========== 0. 依赖 =========="
pip install -q -r requirements.txt 2>/dev/null || true

echo "========== 1. Discovery（合并新 Crypto 市场到 targets.json）=========="
if [[ "$1" != "--skip-discovery" ]]; then
  echo "运行 discovery 一次（需可访问 Gamma API，国内请设 HTTP_PROXY/HTTPS_PROXY）..."
  if python ingest/discovery_loop.py --once 2>&1; then
    echo "Discovery 完成"
  else
    echo "Discovery 失败或超时，将使用现有 targets.json（若存在）"
  fi
else
  echo "跳过 discovery（--skip-discovery）"
fi

if [[ ! -f targets.json ]] || [[ $(python -c "import json; print(len(json.load(open('targets.json'))))" 2>/dev/null) -eq 0 ]]; then
  echo "警告: targets.json 不存在或为空，Polymarket 录制将无市场可录。请先运行: python ingest/discovery.py"
fi

echo ""
echo "========== 2. 启动多源录制（后台）=========="
echo "Polymarket (recorder) ..."
python -u ingest/recorder.py --targets targets.json --output-dir "$RAW_ROOT/polymarket" &
PID_PM=$!
echo "Binance ..."
python -u ingest/binance_recorder.py --output-dir "$RAW_ROOT/binance" &
PID_BN=$!
echo "Chainlink ..."
python -u ingest/chainlink_recorder.py --output-dir "$RAW_ROOT/chainlink" &
PID_CL=$!
echo "0x8dxd Tracker ..."
python -u ingest/tracker_0x8dxd.py --output-dir "$RAW_ROOT/tracker_0x8dxd" &
PID_TR=$!
echo "Discovery 循环（每 60 秒）..."
python -u ingest/discovery_loop.py --targets targets.json --interval 60 &
PID_DL=$!
echo "Compact 循环（每 2 分钟）..."
python -u ingest/compact_raw_to_bronze.py --data-dir "$DATA_ROOT" --interval-sec 120 &
PID_CP=$!
echo "Silver 构建循环（每 5 分钟）..."
(while true; do
  python -u ingest/build_silver.py --data-dir "$DATA_ROOT" --source polymarket || true
  python -u ingest/build_silver.py --data-dir "$DATA_ROOT" --source binance || true
  python -u ingest/build_silver.py --data-dir "$DATA_ROOT" --source chainlink || true
  python -u ingest/build_silver.py --data-dir "$DATA_ROOT" --source tracker_0x8dxd || true
  sleep 300
done) &
PID_SV=$!
echo "Metrics/Alert 循环（每 1 分钟）..."
(while true; do
  python -u ingest/metrics_collector.py --data-dir "$DATA_ROOT" --targets targets.json --output "$DATA_ROOT/checkpoint/metrics.json" || true
  python -u ingest/alert_check.py --metrics "$DATA_ROOT/checkpoint/metrics.json" || true
  sleep 60
done) &
PID_MT=$!
echo ""
echo "已启动: Polymarket=$PID_PM Binance=$PID_BN Chainlink=$PID_CL Tracker=$PID_TR DiscoveryLoop=$PID_DL Compact=$PID_CP Silver=$PID_SV Metrics=$PID_MT"
echo "数据目录: $RAW_ROOT/{polymarket,binance,chainlink,tracker_0x8dxd}"
echo "停止: kill $PID_PM $PID_BN $PID_CL $PID_TR $PID_DL $PID_CP $PID_SV $PID_MT"
echo "手动运行 ETL（交易次数 > N 的所有市场）:"
echo "  python -u eda/01_etl/scripts/select_top2_markets.py --data-dir \"$DATA_ROOT\" --hours 24 --min-buys 10 --output eda/01_etl/output/top2.json --run-etl"
echo "手动运行可视化:"
echo "  python -u eda/02_visual/scripts/batch_plot_retry.py --top2 eda/01_etl/output/top2.json"
echo "或使用 supervisord: mkdir -p logs && supervisord -c supervisord.conf"
