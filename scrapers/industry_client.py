"""
產業分類清單爬蟲(上市 + 上櫃)

資料源: 公開資訊觀測站 opendata
  上市: https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv
  上櫃: https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv

用途:
  1. 提供股票代號、名稱、產業別的官方權威清單
  2. 階段三用來 join 過濾 TPEx 每日行情裡混雜的 ETF/權證/可轉債

設計原則: 忠實存下完整欄位(33 欄),不做篩選;產業代碼轉譯留待後續階段。

已知限制(寫入 README):此清單為「現在」名單回溯套用,歷史區間的股票異動未反映。
"""

import sys
import io
import requests
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from scrapers.common import FetchStatus, FetchResult

URLS = {
    "TWSE": "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv",
    "TPEx": "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv",
}


def fetch_industry_list(market: str) -> FetchResult:
    """
    抓取指定市場(TWSE 或 TPEx)的公司基本資料清單。

    market 若不是 "TWSE"/"TPEx",直接 raise ValueError——這是呼叫端傳錯參數的
    programming error,不是執行期的資料狀態,不該包裝成一種「失敗結果」悄悄
    回傳(比照 yahoo_client.build_yahoo_ticker 對不支援 market 的處理方式)。

    回傳 FetchResult - status 有兩種可能:
        - SUCCESS: 成功取得資料,data 是 list[dict],每個 dict 是一列完整欄位(33 欄)
        - UNKNOWN_FAILURE: 請求失敗,需要之後重試
    """
    url = URLS.get(market)
    if url is None:
        raise ValueError(f"不支援的 market: {market}")

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig")
        # 全部當字串讀,避免代號被誤判成數字型態(例如去掉開頭的 0)
        df = pd.read_csv(io.StringIO(text), dtype=str)
    except requests.exceptions.RequestException as e:
        print(f"❌ {market} 產業分類清單請求失敗: {e}")
        return FetchResult(status=FetchStatus.UNKNOWN_FAILURE)

    records = df.to_dict(orient="records")
    print(f"✅ {market} 取得 {len(records)} 筆公司基本資料")
    return FetchResult(status=FetchStatus.SUCCESS, data=records)


# if __name__ == "__main__":
#     import json
#     from datetime import date
#     from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME

#     client = get_gcs_client()
#     today = date.today()

#     for market in ("TWSE", "TPEx"):
#         result = fetch_industry_list(market)
#         if result.status == FetchStatus.SUCCESS:
#             records = result.data
#             print(f"\n=== {market} 前 1 筆範例 ===")
#             print(records[0])

#             # 存到本地檔案先驗證,還不上傳 GCS
#             Path("local_output").mkdir(exist_ok=True)
#             with open(f"local_output/industry_list_{market.lower()}.json", "w", encoding="utf-8") as f:
#                 json.dump(records, f, ensure_ascii=False)
#             print(f"\n✅ 已存到 local_output/industry_list_{market.lower()}.json")

#             content = json.dumps(records, ensure_ascii=False)
#             write_raw_json(
#                 client=client,
#                 bucket_name=BUCKET_NAME,
#                 source_name=f"industry_list_{market.lower()}",
#                 target_date=today,
#                 content=content,
#             )
