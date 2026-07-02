"""
Yahoo Finance — 一次性歷史資料回填腳本
套件: yfinance (https://github.com/ranaroussi/yfinance)

用途:
  - 專案啟動時執行一次，把過去 N 個月的歷史 K 線資料補齊
  - 提供 Spark 計算技術指標（MA/RSI/MACD）所需的連續歷史
  - 日常更新改由 TWSE 每日快照累積，此腳本不重複執行

執行方式（本地測試）:
  python scrapers/yahoo_client.py

注意:
  - Yahoo Finance 台股代號格式為 "2330.TW"（上市）或 "6531.TWO"（上櫃）
  - yfinance 為非官方套件，偶有不穩定，已加入 retry 機制
  - 回填完成後請勿重複大量執行，避免被限速
"""

import sys
import time
import json
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).resolve().parent))
from utils import get_logger, save_json, RAW_STOCK_DIR, today_str

logger = get_logger("yahoo_client")

# ── 設定區 ────────────────────────────────────────────────

# 回填區間（預設過去 1 年，Spark 算 MA60/RSI 至少需要 60+ 交易日）
BACKFILL_MONTHS = 12

# 目標股票清單（Yahoo Finance 格式）
# 上市加 .TW，上櫃加 .TWO
# 可依需求擴充，建議先用少量清單測試
STOCK_LIST = [
    "2330.TW",   # 台積電
    "2317.TW",   # 鴻海
    "2454.TW",   # 聯發科
    "2881.TW",   # 富邦金
    "2882.TW",   # 國泰金
    "0050.TW",   # 元大台灣50 ETF
    "0056.TW",   # 元大高股息 ETF
    "2308.TW",   # 台達電
    "2412.TW",   # 中華電
    "3711.TW",   # 日月光投控
]

# 每次 API 呼叫之間的等待秒數（避免被 rate limit）
SLEEP_BETWEEN_REQUESTS = 1.0


# ── 核心函式 ─────────────────────────────────────────────

def calc_date_range(months: int) -> tuple[str, str]:
    end   = datetime.now()
    start = end - timedelta(days=months * 31)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_single_stock(
    symbol: str,
    start: str,
    end: str,
    retries: int = 3,
) -> list[dict] | None:
    """
    抓取單支股票的日K歷史資料。
    回傳格式: list of dict，每筆為一個交易日。
    """
    for attempt in range(1, retries + 1):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start, end=end, auto_adjust=True)

            if df.empty:
                logger.warning(f"[{symbol}] 無資料（可能代號錯誤或區間無交易日）")
                return None

            df = df.reset_index()
            df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
            df["symbol"] = symbol
            df["_source"] = "yahoo_finance"
            df["_fetched_date"] = today_str()

            # 只保留需要的欄位，統一命名（後續 Spark 清洗會進一步處理）
            df = df.rename(columns={
                "Date":   "date",
                "Open":   "open",
                "High":   "high",
                "Low":    "low",
                "Close":  "close",
                "Volume": "volume",
            })

            keep_cols = ["date", "open", "high", "low", "close", "volume",
                         "symbol", "_source", "_fetched_date"]
            df = df[[c for c in keep_cols if c in df.columns]]

            return df.to_dict(orient="records")

        except Exception as e:
            logger.error(f"[{symbol}] 第 {attempt} 次失敗: {e}")
            if attempt < retries:
                time.sleep(3)

    return None


def run_backfill(
    stock_list: list[str] = STOCK_LIST,
    months: int = BACKFILL_MONTHS,
) -> dict[str, int]:
    """
    對所有股票執行歷史回填，每支存成獨立 JSON 檔。
    回傳 {symbol: 筆數} 的統計摘要。
    """
    start, end = calc_date_range(months)
    logger.info(f"回填區間: {start} ～ {end}，共 {len(stock_list)} 支股票")

    summary: dict[str, int] = {}
    failed: list[str] = []

    for i, symbol in enumerate(stock_list, 1):
        logger.info(f"[{i}/{len(stock_list)}] 抓取 {symbol}...")

        records = fetch_single_stock(symbol, start, end)

        if records:
            # 每支股票存一個獨立 JSON（symbol 中的 . 換成 _）
            safe_name = symbol.replace(".", "_")
            filename  = f"yfinance_backfill_{safe_name}_{start}_to_{end}.json"
            filepath  = save_json(records, RAW_STOCK_DIR, filename, gcs_source="yahoo")
            summary[symbol] = len(records)
            logger.info(f"  ✓ {symbol}: {len(records)} 筆 → {filepath.name}")
        else:
            failed.append(symbol)
            summary[symbol] = 0
            logger.warning(f"  ✗ {symbol}: 抓取失敗，已跳過")

        # 避免被 rate limit
        if i < len(stock_list):
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 輸出摘要
    logger.info("=== 回填完成摘要 ===")
    total_records = sum(summary.values())
    logger.info(f"成功: {len(stock_list) - len(failed)} 支，失敗: {len(failed)} 支")
    logger.info(f"總計 {total_records} 筆交易日資料")
    if failed:
        logger.warning(f"失敗清單: {failed}")

    return summary


if __name__ == "__main__":
    logger.info("=== Yahoo Finance 歷史資料回填開始 ===")
    summary = run_backfill()

    # 印出每支股票的資料筆數
    print("\n=== 各股票回填筆數 ===")
    for symbol, count in summary.items():
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {symbol}: {count} 筆")