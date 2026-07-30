"""
TWSE 歷史回補(透過 官方 API)

分區方式: 按日期(dt=yyyy-MM-dd)。

執行方式:
    python -m scripts.backfill.backfill_twse
"""

import sys
import json
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import (
    BUCKET_NAME,
    RAW_TWSE_DAILY,
    get_gcs_client,
    write_raw_json,
    raw_blob_exists,
)
from scrapers.twse_client import FetchStatus, fetch_daily_quotes_for_backfill

SOURCE_NAME = RAW_TWSE_DAILY


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
    blob.upload_from_string(
        '{"status": "confirmed_no_trading_day"}', content_type="application/json"
    )


def is_marked_no_trading_day(
    client, bucket_name: str, source_name: str, target_date: date
) -> bool:
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

        result = fetch_daily_quotes_for_backfill(target_date)

        if result.status == FetchStatus.NO_TRADING_DAY:
            mark_no_trading_day(client, BUCKET_NAME, SOURCE_NAME, target_date)
            no_trading += 1
            continue

        if result.status == FetchStatus.UNKNOWN_FAILURE:
            print(f"⚠️ {target_date} 本次無法確認狀態,之後可重試")
            failed += 1
            continue

        assert result.data is not None  # status == SUCCESS,data 保證有值

        content = json.dumps(result.data, ensure_ascii=False)
        write_raw_json(client, BUCKET_NAME, SOURCE_NAME, target_date, content)
        succeeded += 1

    print("\n=== 回補完成 ===")
    print(
        f"成功: {succeeded} / 跳過(已存在): {skipped} / 確認非交易日: {no_trading} / 待重試: {failed}"
    )


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="TWSE 歷史資料回補")
    parser.add_argument("--start-date", required=True, help="起始日期,格式 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="結束日期,格式 YYYY-MM-DD")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    backfill_twse(start=start, end=end)
