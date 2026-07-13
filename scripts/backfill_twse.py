"""
歷史資料回補 CLI

目前階段: 先驗證 TWSE 回補邏輯(小規模測試),TPEx(Yahoo)/Fear&Greed
之後再加進來。

執行方式:
    python scripts/backfill_twse.py
"""

import sys
import json
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import BUCKET_NAME, get_gcs_client, write_raw_json, raw_blob_exists
from scrapers.twse_client import fetch_daily_quotes

SOURCE_NAME = "twse_daily"


def generate_date_range(start: date, end: date) -> list[date]:
    """產生 [start, end] 之間的所有日期(含週末,交易日判斷交給爬蟲本身,
    因為 TWSE 對非交易日會回傳 stat != 'OK',我們不需要自己先猜哪天有開盤)。"""
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def mark_no_trading_day(client, bucket_name: str, source_name: str, target_date: date):
    """
    標記某天為「已確認無交易資料」,避免斷點續跑機制每次都重新請求非交易日,
    降低對外部 API 的請求頻率。
    """
    dt_str = target_date.strftime("%Y-%m-%d")
    blob_path = f"raw/{source_name}/dt={dt_str}/_no_data_marker.json"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string('{"status": "confirmed_no_trading_day"}', content_type="application/json")


def is_marked_no_trading_day(client, bucket_name: str, source_name: str, target_date: date) -> bool:
    dt_str = target_date.strftime("%Y-%m-%d")
    blob_path = f"raw/{source_name}/dt={dt_str}/_no_data_marker.json"
    return client.bucket(bucket_name).blob(blob_path).exists()


def backfill_twse(start: date, end: date):
    client = get_gcs_client()
    date_list = generate_date_range(start, end)

    print(f"預計處理 {len(date_list)} 天,範圍: {start} ~ {end}\n")

    skipped, succeeded, no_trading, failed = 0, 0, 0, 0

    for target_date in date_list:
        # 檢查一: 這天是否已經成功寫入過資料
        if raw_blob_exists(client, BUCKET_NAME, SOURCE_NAME, target_date):
            print(f"⏭️  {target_date} 已存在,跳過")
            skipped += 1
            continue

        # 檢查二: 這天是否已經被確認過是非交易日(這一行就是原本漏接的部分)
        if is_marked_no_trading_day(client, BUCKET_NAME, SOURCE_NAME, target_date):
            print(f"⏭️  {target_date} 已確認為非交易日,跳過")
            no_trading += 1
            continue

        result = fetch_daily_quotes(target_date)

        if result == "NO_TRADING_DAY":
            mark_no_trading_day(client, BUCKET_NAME, SOURCE_NAME, target_date)
            no_trading += 1
            continue

        if result is None:
            print(f"⚠️ {target_date} 本次無法確認狀態,之後可重試")
            failed += 1
            continue

        content = json.dumps(result, ensure_ascii=False)
        write_raw_json(client, BUCKET_NAME, SOURCE_NAME, target_date, content)
        succeeded += 1

    print(f"\n=== 回補完成 ===")
    print(f"成功: {succeeded} / 跳過(已存在): {skipped} / 確認非交易日: {no_trading} / 待重試: {failed}")

    

if __name__ == "__main__":
    # 先用小範圍測試(5 天),確認流程正確再擴大
    # backfill_twse(
    #     start=date(2026, 7, 1),
    #     end=date(2026, 7, 8),
    # )

    backfill_twse(
        start=date(2025, 12, 1),   # 往回抓約 2 年,確保涵蓋 500+ 個交易日,含 MA200 暖機空間
        end=date(2026, 7, 10),     # 到目前為止(不含已經在小規模測試時抓過的 7/11,那天等每日排程處理)
    )