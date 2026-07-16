"""
TPEx 歷史回補(透過 Yahoo Finance)

背景: TPEx 官方 API 無法指定歷史日期,故上櫃股票的歷史回補改用 Yahoo Finance,
      詳見 README 已知限制。

分區方式: 按股票代號(stock_id=xxxx),不是按日期——因為一次 API 呼叫
          就拿到該股票整個回補區間的資料。

執行方式:
    python -m scripts.backfill_yahoo_tpex
"""

import sys
import json
import time
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import BUCKET_NAME, get_gcs_client, write_raw_partitioned, raw_blob_exists_partitioned
from scrapers.yahoo_client import fetch_yahoo_history

SOURCE_NAME = "yahoo_tpex_history"


def load_tpex_stock_list(client, bucket_name: str) -> list[str]:
    """
    從我們已經存好的產業分類清單(TPEx)讀出全部股票代號。
    這是「要回補哪些股票」的權威來源,不是憑空列清單。
    """
    # 目前產業分類清單是按「抓取當天」分區的,先用最簡單的方式:
    # 直接讀最近一次存的那份(還記得我們之前討論過,這份清單本質是「快照」,
    # 下游只在乎最新一份)
    import re
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"raw/industry_list_tpex/"))

    if not blobs:
        raise RuntimeError("找不到 industry_list_tpex 的任何資料,請先執行 industry_client.py")

    # 取最新的一個分區(路徑格式: raw/industry_list_tpex/dt=YYYY-MM-DD/data.json)
    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]
    print(f"讀取產業分類清單: {latest_blob.name}")

    content = latest_blob.download_as_text()
    records = json.loads(content)

    stock_ids = [r["公司代號"] for r in records]
    print(f"共 {len(stock_ids)} 檔 TPEx 股票")
    return stock_ids


def backfill_yahoo_tpex(start_date: date, end_date: date, delay_seconds: float = 1.5):
    client = get_gcs_client()
    stock_ids = load_tpex_stock_list(client, BUCKET_NAME)

    skipped, succeeded, failed = 0, 0, []

    for i, stock_id in enumerate(stock_ids, start=1):
        # 斷點續跑:整支股票的資料存不存在
        if raw_blob_exists_partitioned(client, BUCKET_NAME, SOURCE_NAME, "stock_id", stock_id):
            print(f"[{i}/{len(stock_ids)}] ⏭️  {stock_id} 已存在,跳過")
            skipped += 1
            continue

        print(f"[{i}/{len(stock_ids)}] 抓取 {stock_id}...")
        result = fetch_yahoo_history(stock_id, "TPEx", start_date, end_date)

        if result:
            content = json.dumps(result, ensure_ascii=False)
            write_raw_partitioned(client, BUCKET_NAME, SOURCE_NAME, "stock_id", stock_id, content)
            succeeded += 1
        else:
            failed.append(stock_id)

        time.sleep(delay_seconds)

    print(f"\n=== TPEx Yahoo 回補完成 ===")
    print(f"成功: {succeeded} / 跳過(已存在): {skipped} / 失敗: {len(failed)}")
    if failed:
        print(f"失敗清單: {failed}")
        Path("local_output").mkdir(exist_ok=True)
        with open("local_output/yahoo_tpex_failed.json", "w", encoding="utf-8") as f:
            json.dump(failed, f, ensure_ascii=False, indent=2)


# if __name__ == "__main__":
#     # 先只測試「讀取股票清單」這個環節,不觸發任何 Yahoo API 呼叫
#     # client = get_gcs_client()
#     # stock_ids = load_tpex_stock_list(client, BUCKET_NAME)

#     # print(f"\n前 5 個代號: {stock_ids[:5]}")
#     # print(f"總數: {len(stock_ids)}")
#     # print(f"是否有重複代號: {len(stock_ids) != len(set(stock_ids))}")
#     # print(f"是否有非字串型態: {any(not isinstance(s, str) for s in stock_ids)}")
    
#     # 先用小規模測試:只抓近 10 天,而且先手動抽 3 檔測試,不要一開始就跑全部 891 檔
#     # (下面這行先註解掉全市場清單讀取,直接手動指定測試名單)
#     test_run = False

#     if test_run:
#         from datetime import timedelta
#         client = get_gcs_client()
#         test_stocks = ["1240", "6488"]  # 手動挑 2 檔,不透過完整清單
#         start = date.today() - timedelta(days=10)
#         end = date.today()

#         for stock_id in test_stocks:
#             result = fetch_yahoo_history(stock_id, "TPEx", start, end)
#             if result:
#                 content = json.dumps(result, ensure_ascii=False)
#                 write_raw_partitioned(client, BUCKET_NAME, SOURCE_NAME, "stock_id", stock_id, content)
#             time.sleep(1.5)
#     else:
#         backfill_yahoo_tpex(
#             start_date=date(2024, 7, 1),
#             end_date=date(2026, 7, 16),
#         )

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="TPEx 歷史資料回補(透過 Yahoo Finance)")
    parser.add_argument("--start-date", required=True, help="起始日期,格式 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="結束日期,格式 YYYY-MM-DD")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    backfill_yahoo_tpex(start_date=start, end_date=end)