"""
Yahoo Finance 備援/歷史回補爬蟲

用途:
  1. 每日備援校驗(階段 3.3):單日查詢,跟 TWSE/TPEx 官方資料比對
  2. TPEx 歷史回補(階段 2.3):日期區間查詢,因 TPEx 官方無開放歷史 API

設計原則: 一個通用核心函式,兩種用途都呼叫它,只是傳入的日期區間不同。
"""

import sys
import time
import json
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf

sys.path.append(str(Path(__file__).resolve().parent.parent))


def build_yahoo_ticker(stock_id: str, market: str) -> str:
    """
    根據我們系統內部的 (代號, market) 組出 Yahoo 需要的 ticker。
    market 必須是 'TWSE' 或 'TPEx'(對應 normalize_stock_id 的回傳值)。
    """
    suffix = {"TWSE": ".TW", "TPEx": ".TWO"}.get(market)
    if suffix is None:
        raise ValueError(f"不支援的 market: {market}")
    return f"{stock_id}{suffix}"


def save_failed_list(failed: list[str], filepath: str = "local_output/yahoo_failed_stocks.json"):
    """把失敗清單存成本地檔案,方便之後重跑補抓。"""
    Path(filepath).parent.mkdir(exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(failed, f, ensure_ascii=False, indent=2)
    print(f"✅ 失敗清單已存至 {filepath}")

    
def fetch_batch(
    stock_list: list[dict],  # 每個 dict 需要有 'stock_id' 跟 'market'
    start_date: date,
    end_date: date,
    delay_seconds: float = 1.5,
) -> dict:
    """
    批次抓取多支股票的 Yahoo 歷史資料,單支失敗不中斷整批。

    回傳:
        {
            "success": {stock_id: [records...], ...},
            "failed": [stock_id, ...],  # 失敗清單,供之後補抓使用
        }
    """
    success: dict[str, list[dict]] = {}
    failed: list[str] = []
    total = len(stock_list)

    for i, item in enumerate(stock_list, start=1):
        stock_id = item["stock_id"]
        market = item["market"]

        print(f"[{i}/{total}] 抓取 {stock_id} ({market})...")
        result = fetch_yahoo_history(stock_id, market, start_date, end_date)

        if result:
            success[stock_id] = result
        else:
            failed.append(stock_id)

        # 每支股票之間都要間隔,即使失敗也要等,避免連續失敗時反而打更快
        time.sleep(delay_seconds)

    print(f"\n=== 批次完成 ===")
    print(f"成功: {len(success)} / {total}")
    print(f"失敗: {len(failed)} 檔: {failed}")

    return {"success": success, "failed": failed}

    
def fetch_yahoo_history(
    stock_id: str,
    market: str,
    start_date: date,
    end_date: date,
    max_retries: int = 3,
) -> list[dict] | None:
    """
    抓取單一股票在 [start_date, end_date] 區間(含頭含尾)的歷史資料。

    注意: yfinance 的 end 參數本身是不包含的,所以這裡內部會自動 +1 天,
          讓呼叫端可以用直覺的「含頭含尾」方式指定區間,不用自己記這個細節。

    回傳: list of dict,每筆記錄一天。失敗或無資料回傳 None。
    """
    ticker_symbol = build_yahoo_ticker(stock_id, market)
    yahoo_end = end_date + timedelta(days=1)  # 修正 yfinance 右邊界不含的行為

    for attempt in range(1, max_retries + 1):
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(start=start_date.isoformat(), end=yahoo_end.isoformat())

            if hist.empty:
                print(f"⚠️ {ticker_symbol} 無資料(可能是新股、下市或非交易區間)")
                return None

            # 把 DatetimeIndex 轉成單純的日期字串,並保留原始股票代號/市場資訊
            records = []
            for idx, row in hist.iterrows():
                records.append({
                    "stock_id": stock_id,
                    "market": market,
                    "trade_date": idx.strftime("%Y-%m-%d"),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),  # 成交量是整數股數,用 int 更準確
                })

            print(f"✅ {ticker_symbol} 取得 {len(records)} 筆資料")
            return records

        except Exception as e:
            print(f"⚠️ {ticker_symbol} 第 {attempt} 次嘗試失敗: {e}")
            if attempt < max_retries:
                time.sleep(3)

    print(f"❌ {ticker_symbol} 已達最大重試次數,放棄")
    return None


# if __name__ == "__main__":
#     # 先只測單一股票、單一天,驗證核心函式邏輯正確
#     result = fetch_yahoo_history(
#         stock_id="2330",
#         market="TWSE",
#         start_date=date(2026, 7, 8),
#         end_date=date(2026, 7, 8),
#     )
#     if result:
#         print("\n=== 結果 ===")
#         for row in result:
#             print(row)

if __name__ == "__main__":
    test_stocks = [
        {"stock_id": "2330", "market": "TWSE"},
        {"stock_id": "1240", "market": "TPEx"},
        {"stock_id": "0000", "market": "TWSE"},  # 故意放一個不存在的代號,測試失敗處理
    ]

    result = fetch_batch(
        stock_list=test_stocks,
        start_date=date(2026, 7, 8),
        end_date=date(2026, 7, 8),
        delay_seconds=1.5,
    )

    save_failed_list(result["failed"])