"""
歷史資料回補 CLI

目前階段: 先驗證 TWSE 回補邏輯(小規模測試),TPEx(Yahoo)/Fear&Greed
之後再加進來。

執行方式:
    python scripts/backfill_historical.py
"""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, write_raw_json, raw_blob_exists
from scrapers.twse_client import fetch_daily_quotes

BUCKET_NAME = "stock-pulse-data-lake"


def generate_date_range(start: date, end: date) -> list[date]:
    """產生 [start, end] 之間的所有日期(含週末,交易日判斷交給爬蟲本身,
    因為 TWSE 對非交易日會回傳 stat != 'OK',我們不需要自己先猜哪天有開盤)。"""
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def backfill_twse(start: date, end: date):
    client = get_gcs_client()
    date_list = generate_date_range(start, end)

    print(f"預計處理 {len(date_list)} 天,範圍: {start} ~ {end}\n")

    skipped, succeeded, failed = 0, 0, 0

    for target_date in date_list:
        # 斷點續跑核心邏輯:已存在就跳過
        if raw_blob_exists(client, BUCKET_NAME, "twse_daily", target_date):
            print(f"⏭️  {target_date} 已存在,跳過")
            skipped += 1
            continue

        result = fetch_daily_quotes(target_date)
        if result is None:
            print(f"⚠️ {target_date} 無資料(可能是非交易日)")
            failed += 1
            continue

        import json
        content = json.dumps(result, ensure_ascii=False)
        write_raw_json(client, BUCKET_NAME, "twse_daily", target_date, content)
        succeeded += 1

    print(f"\n=== 回補完成 ===")
    print(f"成功: {succeeded} / 跳過(已存在): {skipped} / 無資料: {failed}")


if __name__ == "__main__":
    # 先用小範圍測試(5 天),確認流程正確再擴大
    # backfill_twse(
    #     start=date(2026, 7, 1),
    #     end=date(2026, 7, 8),
    # )

    backfill_twse(
        start=date(2024, 7, 1),   # 往回抓約 2 年,確保涵蓋 500+ 個交易日,含 MA200 暖機空間
        end=date(2026, 7, 10),     # 到目前為止(不含已經在小規模測試時抓過的 7/11,那天等每日排程處理)
    )