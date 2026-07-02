"""
CNN Fear & Greed Index 抓取腳本（非官方 API）
端點: https://production.dataviz.cnn.io/index/fearandgreed/graphdata

用途:
  - 每日抓取市場情緒指數（0=極度恐慌, 100=極度貪婪）
  - 同時抓取各子指標（動能、避險需求、波動率等）
  - 作為獨立情緒維度，在 dbt 建模時與新聞情緒分數 join

注意:
  - CNN 非官方端點，可能變動。若失敗先嘗試: https://fear-and-greed-index.p.rapidapi.com
  - 本腳本每日執行一次即可（Airflow 設 @daily）

執行方式（本地測試）:
  python scrapers/fear_greed_client.py
"""

import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from utils import get_logger, save_json, RAW_DIR, today_str

logger = get_logger("fear_greed_client")

CNN_FG_URL = (
    "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://edition.cnn.com/markets/fear-and-greed",
}


def fetch_fear_greed(retries: int = 3) -> dict | None:
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"呼叫 CNN Fear & Greed API（第 {attempt} 次）...")
            resp = requests.get(CNN_FG_URL, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            logger.info("成功取得資料")
            return data
        except Exception as e:
            logger.error(f"第 {attempt} 次失敗: {e}")
            if attempt < retries:
                time.sleep(5)
    return None


def parse_fear_greed(raw: dict) -> dict:
    """
    把 CNN 回傳的巢狀結構攤平成單層 dict，方便存入 BigQuery。

    CNN API 回傳結構（主要欄位）:
    {
      "fear_and_greed": {
        "score": 45.5,
        "rating": "Fear",
        "timestamp": "2026-07-01T..."
      },
      "fear_and_greed_historical": { ... },  # 歷史走勢（我們只取現值）
      ...子指標...
    }
    """
    fg = raw.get("fear_and_greed", {})

    record = {
        # 核心指標
        "score":           fg.get("score"),
        "rating":          fg.get("rating"),          # Extreme Fear / Fear / Neutral / Greed / Extreme Greed
        "previous_close":  fg.get("previous_close"),  # 前日收盤分數
        "previous_1_week": fg.get("previous_1_week"),
        "previous_1_month":fg.get("previous_1_month"),
        "previous_1_year": fg.get("previous_1_year"),

        # 子指標（CNN 通常提供 5-7 個）
        "junk_bond_demand":      _get_sub(raw, "junk_bond_demand"),
        "market_momentum_sp500": _get_sub(raw, "market_momentum_sp500"),
        "market_momentum_sp125": _get_sub(raw, "market_momentum_sp125"),
        "stock_price_strength":  _get_sub(raw, "stock_price_strength"),
        "stock_price_breadth":   _get_sub(raw, "stock_price_breadth"),
        "put_call_options":      _get_sub(raw, "put_call_options"),
        "market_volatility_vix": _get_sub(raw, "market_volatility_vix"),
        "safe_haven_demand":     _get_sub(raw, "safe_haven_demand"),

        # Metadata
        "date":            today_str(),
        "fetched_at":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source":         "cnn_fear_greed",
    }
    return record


def _get_sub(raw: dict, key: str) -> float | None:
    """從子指標 dict 取 score，欄位不存在時回傳 None"""
    sub = raw.get(key, {})
    if isinstance(sub, dict):
        return sub.get("score")
    return None


def run() -> Path | None:
    raw = fetch_fear_greed()
    if raw is None:
        return None

    record = parse_fear_greed(raw)

    # 印出關鍵欄位
    logger.info("=== Fear & Greed 指數 ===")
    print(f"  今日分數  : {record['score']}")
    print(f"  情緒評級  : {record['rating']}")
    print(f"  前日分數  : {record['previous_close']}")
    print(f"  一週前    : {record['previous_1_week']}")
    print(f"  一個月前  : {record['previous_1_month']}")
    print(f"  一年前    : {record['previous_1_year']}")
    print(f"  VIX 子指標: {record['market_volatility_vix']}")

    filepath = save_json(record, RAW_DIR, gcs_source="cnn_fear_greed")
    logger.info(f"已儲存至 {filepath}")
    return filepath


if __name__ == "__main__":
    run()