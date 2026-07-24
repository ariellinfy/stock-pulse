"""
Fear & Greed 歷史回補

特性: 一次 API 呼叫即可拿到整段區間,不需要迴圈、不需要斷點續跑機制。

執行方式:
    python -m scripts.backfill_fear_greed
"""

import sys
import json
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import BUCKET_NAME, get_gcs_client, write_raw_partitioned, raw_blob_exists_partitioned
from scrapers.fear_greed_client import fetch_fear_greed_full_history

SOURCE_NAME = "fear_greed_history"
PARTITION_VALUE = "full"  # 固定值,因為這份資料本質上不分區,整段歷史就是一筆


def backfill_fear_greed(start_date: date):
    client = get_gcs_client()

    # 即使不太需要,仍比照其他回補腳本加上存在性檢查,維持一致的操作習慣
    if raw_blob_exists_partitioned(client, BUCKET_NAME, SOURCE_NAME, "range", PARTITION_VALUE):
        print("⏭️  Fear & Greed 歷史資料已存在,跳過(如需強制更新,請先手動刪除該檔案)")
        return

    result = fetch_fear_greed_full_history(start_date)
    if result is None:
        print("❌ 無法取得 Fear & Greed 歷史資料")
        return

    content = json.dumps(result, ensure_ascii=False)
    write_raw_partitioned(client, BUCKET_NAME, SOURCE_NAME, "range", PARTITION_VALUE, content)


# if __name__ == "__main__":
#     # 近 2 年,對應我們回補股票資料的區間
#     backfill_fear_greed(start_date=date(2024, 7, 1))

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="Fear & Greed 歷史資料回補")
    parser.add_argument("--start-date", required=True, help="起始日期,格式 YYYY-MM-DD")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    backfill_fear_greed(start_date=start)
