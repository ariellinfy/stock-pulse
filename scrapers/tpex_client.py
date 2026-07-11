"""
TPEx 每日全市場收盤行情爬蟲(上櫃)

資料源: daily_close_quotes/stk_quote_result.php
限制: 只回傳「當日」資料,無法指定過去日期(已驗證確認)
      → 因此本爬蟲僅用於每日例行排程(階段 4.4),
        歷史回補(階段 2.3)改用 Yahoo Finance,詳見 README 已知限制。

設計原則: 只負責忠實取得原始資料,不做欄位清洗或型態轉換(留給階段三 Spark)。
"""

import sys
import json
import requests
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME

TPEX_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"

# 已驗證過的真實欄位定義(2026-07-09 測試結果)
EXPECTED_FIELDS = [
    "代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價",
    "成交股數", "成交金額(元)", "成交筆數", "最後買價", "最後買量(張數)",
    "最後賣價", "最後賣量(張數)", "發行股數", "次日 參考價", "次日 漲停價", "次日 跌停價",
]


def to_roc_date(target_date: date) -> str:
    """西元轉民國,例如 date(2026,7,9) -> '115/07/09'。"""
    roc_year = target_date.year - 1911
    return f"{roc_year}/{target_date.month:02d}/{target_date.day:02d}"

def roc_to_gregorian(roc_date_str: str) -> str:
    """
    將 TPEx 回傳的民國日期字串轉成西元 ISO 格式。
    例如: '115/07/09' -> '2026-07-09'
    """
    roc_year, month, day = roc_date_str.split("/")
    gregorian_year = int(roc_year) + 1911
    return f"{gregorian_year}-{month}-{day}"

def validate_fields(actual_fields: list[str]) -> bool:
    if actual_fields != EXPECTED_FIELDS:
        print("⚠️ 警告:TPEx 回傳的欄位結構與預期不同!")
        print(f"   預期: {EXPECTED_FIELDS}")
        print(f"   實際: {actual_fields}")
        return False
    return True


def fetch_daily_quotes(target_date: date) -> dict | None:
    """
    抓取「當日」上櫃全市場收盤行情(不論傳入的 target_date 是什麼,
    此 API 固定回傳呼叫當下的最新交易日資料 —— 這是已知限制,呼叫端
    應只在每日例行排程中使用本函式,不要用來做歷史回補)。
    """
    roc_date_str = to_roc_date(target_date)
    params = {"l": "zh-tw", "d": roc_date_str, "s": "0,asc,0"}
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(TPEX_URL, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    tables = payload.get("tables", [])
    if not tables:
        print(f"⚠️ 無資料,可能是非交易日。回傳 date={payload.get('date')}")
        return None

    table = tables[0]
    actual_fields = table.get("fields", [])
    rows = table.get("data", [])

    actual_roc_date = table.get("date")  # 民國格式,用來確認實際拿到的是哪一天，例如 '115/07/09'
    actual_trade_date = roc_to_gregorian(actual_roc_date) if actual_roc_date else None
    
    fields_ok = validate_fields(actual_fields)
    print(f"✅ TPEx 實際回傳日期: {actual_trade_date},取得 {len(rows)} 筆資料"
          f"(欄位結構{'正常' if fields_ok else '⚠️ 已變動,見上方警告'})")

    return {
        "fields": actual_fields,
        "data": rows,
        "fields_match_expected": fields_ok,
        "actual_trade_date": actual_trade_date,       # 西元,給下游用
        "actual_trade_date_roc": actual_roc_date,      # 民國,保留原始證據
    }


if __name__ == "__main__":
    target_date = date(2026, 7, 9)
    result = fetch_daily_quotes(target_date)

    if result:
        print("\n=== 前 2 筆原始資料(未清洗)===")
        for row in result["data"][:2]:
            print(row)

        client = get_gcs_client()

        content = json.dumps(result, ensure_ascii=False)
        write_raw_json(
            client=client,
            bucket_name=BUCKET_NAME,
            source_name="tpex_daily",
            target_date=target_date,
            content=content,
        )
    else:
        print("⚠️ 無資料可寫入,略過此次上傳")