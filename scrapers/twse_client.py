"""
TWSE 每日全市場收盤行情爬蟲

資料源: MI_INDEX (支援指定日期,可用於歷史回補與每日例行抓取)
表格: tables[8] 「每日收盤行情(全部...)」

設計原則:
  - 只負責「忠實取得原始資料」,不做任何欄位清洗或型態轉換
  - HTML tag、千分位逗號等清理邏輯留給階段三 Spark 處理
"""

import sys
import json
from datetime import date
from pathlib import Path

import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, write_raw_json

TWSE_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
DAILY_QUOTES_TABLE_INDEX = 8  # 「每日收盤行情(全部...)」在 tables 陣列中的位置

# 目前已知、驗證過的欄位定義。之後若 TWSE 調整欄位,這裡是唯一需要更新的地方,
# 下游 Spark schema (階段 3.2) 也應該以這份定義為準。
EXPECTED_FIELDS = [
    "證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額",
    "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差",
    "最後揭示買價", "最後揭示買量", "最後揭示賣價", "最後揭示賣量", "本益比",
]

def validate_fields(actual_fields: list[str]) -> bool:
    """
    比對實際抓到的欄位跟預期定義是否一致。
    回傳 False 但不拋例外——讓呼叫端決定要繼續存檔(附警告)還是中止。
    """
    if actual_fields != EXPECTED_FIELDS:
        print("⚠️ 警告:TWSE 回傳的欄位結構與預期不同!")
        print(f"   預期: {EXPECTED_FIELDS}")
        print(f"   實際: {actual_fields}")

        missing = set(EXPECTED_FIELDS) - set(actual_fields)
        extra = set(actual_fields) - set(EXPECTED_FIELDS)
        if missing:
            print(f"   缺少欄位: {missing}")
        if extra:
            print(f"   新增欄位: {extra}")
        return False
    return True


def fetch_daily_quotes(target_date: date) -> dict | None:
    """
    抓取指定日期的全市場個股收盤行情原始資料(未清洗)。

    回傳: 每一列是一支股票的原始資料(list of str),對應 fields 定義的 16 個欄位。
          非交易日或抓取失敗時回傳 None(不拋例外,讓呼叫端決定如何處理)。

    回傳格式(自帶欄位說明,不再是純陣列):
        {
            "fields": [...],   # 當次抓取時,TWSE 實際回傳的欄位順序與名稱
            "data": [[...], ...],
            "fields_match_expected": true/false  # 供下游快速判斷是否需要特別檢查
        }
    """
    date_str = target_date.strftime("%Y%m%d")
    params = {
        "response": "json",
        "date": date_str,
        "type": "ALLBUT0999", # 全部股票（不含權證 - 權證檔數通常是普通股的 10 倍以上，容易導致 API 回傳過久或記憶體耗盡。）
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(TWSE_URL, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("stat") != "OK":
        print(f"⚠️ {date_str} 非交易日或無資料,stat={payload.get('stat')}")
        return None

    tables = payload.get("tables", [])
    if len(tables) <= DAILY_QUOTES_TABLE_INDEX:
        print(f"⚠️ {date_str} tables 結構異常,只有 {len(tables)} 張表")
        return None

     
    target_table = tables[DAILY_QUOTES_TABLE_INDEX]
    actual_fields = target_table.get("fields", [])
    rows = target_table.get("data", [])

    fields_ok = validate_fields(actual_fields)
    print(f"✅ {date_str} 取得 {len(rows)} 筆個股資料(欄位結構{'正常' if fields_ok else '⚠️ 已變動,見上方警告'})")

    return {
        "fields": actual_fields,
        "data": rows,
        "fields_match_expected": fields_ok,
    }


if __name__ == "__main__":
    target_date = date(2026, 7, 8)
    result  = fetch_daily_quotes(target_date)

    if result :
        print("\n=== 前 2 筆原始資料(未清洗)===")
        for row in result["data"][:2]:
            print(row)

        # 存到本地檔案先驗證,還不上傳 GCS
        # import json
        # Path("local_output").mkdir(exist_ok=True)
        # with open("local_output/twse_daily_2026-07-08.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=2)
        # print("\n✅ 已存到 local_output/twse_daily_2026-07-08.json")

        # 寫入 GCS Raw Layer(冪等覆蓋)
        BUCKET_NAME = "stock-pulse-data-lake"  # 改成你的 bucket 名稱
        client = get_gcs_client()

        content = json.dumps(result, ensure_ascii=False)
        write_raw_json(
            client=client,
            bucket_name=BUCKET_NAME,
            source_name="twse_daily",
            target_date=target_date,
            content=content,
        )
    else:
        print("⚠️ 無資料可寫入,略過此次上傳")