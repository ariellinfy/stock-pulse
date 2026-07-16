#!/bin/bash
set -e  # 任何一行指令失敗,立刻停止整個 script,不要繼續往下跑

# 全台股歷史資料完整回補流程
# 用法: ./scripts/run_full_backfill.sh <start-date> <end-date>
# 範例: ./scripts/run_full_backfill.sh 2024-07-01 2026-07-10

START_DATE=$1
END_DATE=$2

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
    echo "❌ 用法: ./scripts/run_full_backfill.sh <start-date> <end-date>"
    exit 1
fi

echo "=== [1/4] 回補 TWSE 歷史行情 (${START_DATE} ~ ${END_DATE}) ==="
docker compose run --rm backfill python -m scripts.backfill_twse --start-date "$START_DATE" --end-date "$END_DATE"

echo "=== [2/4] 回補 TPEx 歷史行情(透過 Yahoo Finance) ==="
docker compose run --rm backfill python -m scripts.backfill_yahoo_tpex --start-date "$START_DATE" --end-date "$END_DATE"

echo "=== [3/4] 回補 Fear & Greed 歷史指數 ==="
docker compose run --rm backfill python -m scripts.backfill_fear_greed --start-date "$START_DATE"

echo "=== [4/4] 執行 Spark 清洗與合併 ==="
docker run --rm \
  -v ~/stock-pulse/secrets:/app/secrets:ro \
  -e GCP_SA_KEY_PATH=/app/secrets/gcp-sa-key.json \
  -e GCP_BUCKET_NAME=stock-pulse-data-lake \
  stock-pulse-spark \
  /opt/spark/bin/spark-submit /app/spark/jobs/backfill_clean_stock.py

echo "✅ 全台股歷史資料完整回補流程執行完畢"