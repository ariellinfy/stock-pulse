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
import time
import requests
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME

TWSE_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
DAILY_QUOTES_TABLE_INDEX = 8  # 「每日收盤行情(全部...)」在 tables 陣列中的位置

# 基於歷史觀察,正常交易日應有 1300+ 檔(含 ETF/特別股等),
# 若筆數明顯偏低(低於此比例),視為擷取不完整,不寫入,讓斷點續跑機制之後重新嘗試
MIN_ROWS_RATIO_OF_RECENT_AVERAGE = 0.85

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


def fetch_daily_quotes(target_date: date, max_retries: int = 3, no_data_confirm_attempts: int = 2) -> dict | None:
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

    回傳三種狀態,呼叫端要分別處理:
      - dict: 成功取得資料
      - "NO_TRADING_DAY": 連續 no_data_confirm_attempts 次都確認 TWSE 明確回應無資料,
                           可放心視為非交易日,標記起來避免未來重複請求
      - None: 請求本身失敗(網路錯誤/403等),無法判斷當天狀態,不應標記,需要之後重試
    """
    date_str = target_date.strftime("%Y%m%d")
    params = {"response": "json", "date": date_str, "type": "ALLBUT0999"}
    headers = {"User-Agent": "Mozilla/5.0"}

    no_data_confirmations = 0

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(TWSE_URL, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 403:
                wait_time = 10 * attempt
                print(f"⚠️ {date_str} 收到 403,等待 {wait_time} 秒後重試({attempt}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ {date_str} 請求失敗(非 403): {e}")
                return None  # 不確定狀態,不標記,交由之後重試
        except requests.exceptions.RequestException as e:
            print(f"❌ {date_str} 網路錯誤: {e}")
            return None  # 同樣不確定狀態,不標記

        payload = resp.json()

        if payload.get("stat") != "OK":
            no_data_confirmations += 1
            print(f"ℹ️ {date_str} 第 {no_data_confirmations} 次確認無交易資料(stat={payload.get('stat')})")

            if no_data_confirmations >= no_data_confirm_attempts:
                print(f"✅ {date_str} 已連續 {no_data_confirm_attempts} 次確認,判定為非交易日")
                return "NO_TRADING_DAY"

            time.sleep(2)  # 兩次確認之間稍微間隔,避免瞬間重複打到同一個暫時性問題
            continue

        # stat == OK,成功取得資料,走原本既有的邏輯(欄位驗證、回傳 dict)
        tables = payload.get("tables", [])
        if len(tables) <= DAILY_QUOTES_TABLE_INDEX:
            print(f"⚠️ {date_str} tables 結構異常,只有 {len(tables)} 張表")
            return None

        target_table = tables[DAILY_QUOTES_TABLE_INDEX]
        actual_fields = target_table.get("fields", [])
        rows = target_table.get("data", [])

        min_expected = int(1300 * 0.85)
        if len(rows) < min_expected:
            print(f"⚠️ {date_str} 只取得 {len(rows)} 筆,低於門檻 {min_expected},判定為擷取不完整")
            return None

        fields_ok = validate_fields(actual_fields)
        print(f"✅ {date_str} 取得 {len(rows)} 筆個股資料")

        return {
            "fields": actual_fields,
            "data": rows,
            "fields_match_expected": fields_ok,
        }

    print(f"❌ {date_str} 重試 {max_retries} 次後仍無法取得明確結果,略過")
    return None


if __name__ == "__main__":
    target_date = date(2025, 12, 17)
    result  = fetch_daily_quotes(target_date)

    if result :
        print("\n=== 前 2 筆原始資料(未清洗)===")
        for row in result["data"][:2]:
            print(row)

        # 存到本地檔案先驗證,還不上傳 GCS
        # Path("local_output").mkdir(exist_ok=True)
        # with open("local_output/twse_daily_2025-12-17.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=2)
        # print("\n✅ 已存到 local_output/twse_daily_2025-12-17.json")

        # 寫入 GCS Raw Layer(冪等覆蓋)
        # client = get_gcs_client()

        # content = json.dumps(result, ensure_ascii=False)
        # write_raw_json(
        #     client=client,
        #     bucket_name=BUCKET_NAME,
        #     source_name="twse_daily",
        #     target_date=target_date,
        #     content=content,
        # )
    else:
        print("⚠️ 無資料可寫入,略過此次上傳")