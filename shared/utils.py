import os
from google.cloud import storage
from datetime import date
from typing import Tuple


def get_gcs_client(key_path: str = "secrets/gcp-sa-key.json") -> storage.Client:
    """回傳一個已認證的 GCS client。"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    return storage.Client()


def write_raw_json(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    target_date: date,
    content: str,
) -> str:
    """
    將原始資料(JSON 字串)以冪等方式寫入 GCS Raw Layer。

    路徑格式: raw/{source_name}/dt={YYYY-MM-DD}/data.json
    - 同一個 source_name + target_date 重複執行,會直接覆蓋舊檔,
      不會產生重複檔案。這就是「冪等性」:多次執行結果等同一次執行。

    回傳: 寫入後的完整 GCS path,方便呼叫端印 log 確認。
    """
    dt_str = target_date.strftime("%Y-%m-%d")
    blob_path = f"raw/{source_name}/dt={dt_str}/data.json"

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="application/json")

    full_path = f"gs://{bucket_name}/{blob_path}"
    print(f"✅ 已寫入(冪等覆蓋): {full_path}")
    return full_path


def normalize_stock_id(raw_id: str) -> Tuple[str, str]:
    """
    將不同來源格式的股票代號,正規化為 (純代號, market) 的 tuple。

    market 只會是以下三種之一:
        "TWSE"  - 上市(對應 Yahoo 的 .TW 後綴)
        "TPEx"  - 上櫃(對應 Yahoo 的 .TWO 後綴)
        "UNKNOWN" - 無法判斷市場別時的保底值(交由呼叫端決定如何處理,
                     例如記錄警告 log,不在這裡直接拋例外中斷整批資料)

    範例:
        normalize_stock_id("2330.TW")  -> ("2330", "TWSE")
        normalize_stock_id("6488.TWO") -> ("6488", "TPEx")
        normalize_stock_id("2330")     -> ("2330", "UNKNOWN")
    """
    raw_id = raw_id.strip().upper()

    if raw_id.endswith(".TW"):
        return raw_id[:-3], "TWSE"
    elif raw_id.endswith(".TWO"):
        return raw_id[:-4], "TPEx"
    else:
        # 純數字代號,沒有後綴時無法單靠代號本身判斷市場別
        # (TWSE/TPEx 官方 API 回傳資料時通常會在別的欄位註明來源,
        #  之後寫爬蟲時,market 會由呼叫端根據「這支爬蟲抓的是哪個 API」直接指定,
        #  而不是依賴這個函式去猜)
        return raw_id, "UNKNOWN"