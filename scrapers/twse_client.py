"""
TWSE OpenAPI — 每日全市場快照
API: https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL

用途:
  - 每個交易日收盤後（約 15:30 後）抓當日全市場收盤行情
  - 回傳「最新一個交易日」的所有上市個股，無法指定日期
  - 每日由 Airflow DAG 觸發，累積後形成歷史資料

執行方式（本地測試）:
  python scrapers/twse_client.py
"""

import sys
import time
import requests
import pandas as pd
from pathlib import Path

# 讓 scrapers/ 以外也能 import utils
sys.path.append(str(Path(__file__).resolve().parent))
from utils import get_logger, save_json, RAW_STOCK_DIR, today_str

logger = get_logger("twse_client")

TWSE_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

# 只關注的股票代號（空白 = 全部）
# 若要縮小規模可填入: ["2330", "2317", "2454", "2881", "0050"]
WATCH_LIST: list[str] = []


def fetch_daily_quotes(retries: int = 3) -> list[dict]:
    """
    抓取最新交易日全市場收盤行情。
    遇到非交易日或 API 暫時無資料時回傳空 list（不拋錯，讓 Airflow 不觸發失敗）。
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"呼叫 TWSE API（第 {attempt} 次）...")
            resp = requests.get(TWSE_URL, timeout=30)
            resp.raise_for_status()
            data: list[dict] = resp.json()

            if not data:
                logger.warning("TWSE API 回傳空資料，可能是非交易日或盤中時段。")
                return []

            logger.info(f"成功取得 {len(data)} 筆資料")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"第 {attempt} 次請求失敗: {e}")
            if attempt < retries:
                time.sleep(5)

    logger.error("已達最大重試次數，放棄本次抓取。")
    return []


def filter_watch_list(data: list[dict]) -> list[dict]:
    """若有設定 WATCH_LIST，只保留指定股票"""
    if not WATCH_LIST:
        return data
    return [row for row in data if row.get("Code") in WATCH_LIST]


def validate_and_tag(data: list[dict]) -> list[dict]:
    """
    加上 pipeline metadata：
    - fetched_date: 本腳本執行的日期（≠ 交易日，但可用來追蹤資料新鮮度）
    - source: 來源標記
    """
    today = today_str()
    for row in data:
        row["_fetched_date"] = today
        row["_source"] = "twse_openapi"
    return data


def run() -> Path | None:
    raw_data = fetch_daily_quotes()
    if not raw_data:
        return None

    filtered = filter_watch_list(raw_data)
    tagged   = validate_and_tag(filtered)

    # 印出前 3 筆讓使用者確認欄位
    logger.info("=== 範例資料（前 3 筆）===")
    for row in tagged[:3]:
        print(row)

    # 確認欄位
    df = pd.DataFrame(tagged)
    logger.info(f"欄位清單: {list(df.columns)}")
    logger.info(f"資料筆數: {len(df)}")

    # 儲存
    filename = f"twse_{today_str()}.json"
    filepath = save_json(tagged, RAW_STOCK_DIR, filename, gcs_source="twse")
    logger.info(f"已儲存至 {filepath}")
    return filepath


if __name__ == "__main__":
    run()