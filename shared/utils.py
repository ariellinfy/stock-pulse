import os
import json
from google.cloud import storage
from datetime import date
from typing import Tuple

from dotenv import load_dotenv

load_dotenv()  # 讀取專案根目錄的 .env 檔案

def get_required_env(key: str) -> str:
    """
    讀取必要的環境變數,若不存在則立即中斷並給出清楚的錯誤訊息。
    回傳型態明確是 str(不是 str | None),讓呼叫端不需要再處理 None 的情況。
    """
    value = os.environ.get(key)
    if value is None:
        raise RuntimeError(f"環境變數 {key} 未設定,請確認 .env 檔案存在且內容正確")
    return value


BUCKET_NAME: str = get_required_env("GCP_BUCKET_NAME")
SA_KEY_PATH: str = os.environ.get("GCP_SA_KEY_PATH", "secrets/gcp-sa-key.json")


def get_gcs_client(key_path: str | None = None) -> storage.Client:
    """
    回傳一個已認證的 GCS client。
    key_path 優先順序: 明確傳入的參數 > 環境變數 GCP_SA_KEY_PATH > 預設相對路徑
    (相對路徑僅為向下相容本機開發習慣,不建議在容器/正式環境依賴它)
    """
    resolved_path = key_path or os.environ.get("GCP_SA_KEY_PATH", "secrets/gcp-sa-key.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = resolved_path
    return storage.Client()


def write_raw_partitioned(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    partition_key: str,
    partition_value: str,
    content: str,
) -> str:
    """
    通用的冪等寫入函式,分區方式由呼叫端決定(不限於日期)。

    路徑格式: raw/{source_name}/{partition_key}={partition_value}/data.json

    例如:
        partition_key="dt", partition_value="2026-07-08"       → 按日期分區(TWSE/TPEx)
        partition_key="stock_id", partition_value="1240"        → 按股票代號分區(Yahoo 歷史回補)
    """
    blob_path = f"raw/{source_name}/{partition_key}={partition_value}/data.json"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="application/json")

    full_path = f"gs://{bucket_name}/{blob_path}"
    print(f"✅ 已寫入(冪等覆蓋): {full_path}")
    return full_path


def write_raw_json(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    target_date: date,
    content: str,
) -> str:
    """按日期分區的寫入(既有邏輯不變,只是內部改呼叫通用函式)。"""
    dt_str = target_date.strftime("%Y-%m-%d")
    return write_raw_partitioned(client, bucket_name, source_name, "dt", dt_str, content)


def raw_blob_exists_partitioned(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    partition_key: str,
    partition_value: str,
) -> bool:
    """通用版本的斷點續跑檢查,對應 write_raw_partitioned。"""
    blob_path = f"raw/{source_name}/{partition_key}={partition_value}/data.json"
    blob = client.bucket(bucket_name).blob(blob_path)
    return blob.exists()


def raw_blob_exists(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    target_date: date,
) -> bool:
    """既有邏輯不變,只是內部改呼叫通用函式。"""
    dt_str = target_date.strftime("%Y-%m-%d")
    return raw_blob_exists_partitioned(client, bucket_name, source_name, "dt", dt_str)


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


def load_industry_list_from_gcs(bucket_name: str, market: str) -> list[dict]:
    """
    直接從 GCS 讀取最新一份產業分類清單,取代依賴本機 local_output/ 檔案的做法。
    容器環境沒有本機探索階段留下的檔案,必須直接讀權威來源。
    """

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    prefix = f"raw/industry_list_{market.lower()}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise RuntimeError(f"找不到 {prefix} 底下的任何資料")

    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]
    content = json.loads(latest_blob.download_as_text())
    return content