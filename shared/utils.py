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


def __getattr__(name: str):
    """
    延遲評估 BUCKET_NAME / SA_KEY_PATH:只有真的有人存取這兩個名稱時,才去檢查
    對應的環境變數是否存在(PEP 562 模組層級 __getattr__)。

    這裡刻意不在模組頂層直接賦值,是因為那樣會讓「只是想 import 這個模組裡
    完全不需要 GCP 環境的純函式」(例如 normalize_stock_id)時,也被迫要求整組
    GCP 環境變數就緒——這對輕量測試(見 tests/test_normalize_stock_id.py)是
    不必要的耦合。實際會用到 BUCKET_NAME/SA_KEY_PATH 的呼叫端行為不變:
    import 當下一樣會立刻觸發檢查。
    """
    if name == "BUCKET_NAME":
        return get_required_env("GCP_BUCKET_NAME")
    if name == "SA_KEY_PATH":
        return get_required_env("GCP_SA_KEY_PATH")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_gcs_client(key_path: str | None = None) -> storage.Client:
    """
    回傳一個已認證的 GCS client。
    key_path 優先順序: 明確傳入的參數 > 環境變數 GCP_SA_KEY_PATH > 預設相對路徑
    (相對路徑僅為向下相容本機開發習慣,不建議在容器/正式環境依賴它)
    """
    resolved_path = key_path or os.environ.get(
        "GCP_SA_KEY_PATH", "secrets/gcp-sa-key.json"
    )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = resolved_path
    return storage.Client()


# --- Raw / Clean 資料湖路徑慣例的唯一定義來源 ---
# raw 層 source_name:寫入端(scrapers 觸發的 Airflow task、backfill 腳本)跟讀取端
# (spark jobs、completeness check、手動分析腳本)過去分別重複寫死同一個字串,
# 這裡統一定義,兩邊都改成 import 這裡的常數,避免其中一邊改了忘記同步另一邊。
RAW_TWSE_DAILY = "twse_daily"
RAW_TPEX_DAILY = "tpex_daily"
RAW_FEAR_GREED = "fear_greed"
RAW_FEAR_GREED_HISTORY = "fear_greed_history"
RAW_YAHOO_TPEX_HISTORY = "yahoo_tpex_history"

# clean 層路徑前綴(不含 bucket/scheme,呼叫端視需要自行組 gs://{bucket}/ 開頭)
CLEAN_STOCK_DAILY = "clean/stock_daily"
CLEAN_FEAR_GREED_DAILY = "clean/fear_greed_daily"
CLEAN_INDUSTRY_LIST = "clean/industry_list"


def raw_industry_list_source_name(market: str) -> str:
    """對應 industry_client 抓回來、寫入 raw 層時用的 source_name。"""
    return f"industry_list_{market.lower()}"


def gcs_uri(bucket_name: str, path: str) -> str:
    """組出 gs://{bucket}/{path} 形式的完整 URI,供 Spark 讀寫使用。"""
    return f"gs://{bucket_name}/{path}"


def raw_blob_path(source_name: str, partition_key: str, partition_value: str) -> str:
    """
    raw 層單一分區的路徑樣板: raw/{source_name}/{partition_key}={partition_value}/data.json
    寫入(write_raw_partitioned)、讀取(spark jobs、completeness check 等)都呼叫
    這裡,避免雙方各自重複寫死同一個字串樣板。
    """
    return f"raw/{source_name}/{partition_key}={partition_value}/data.json"


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

    例如:
        partition_key="dt", partition_value="2026-07-08"       → 按日期分區(TWSE/TPEx)
        partition_key="stock_id", partition_value="1240"        → 按股票代號分區(Yahoo 歷史回補)
    """
    blob_path = raw_blob_path(source_name, partition_key, partition_value)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content, content_type="application/json")

    full_path = gcs_uri(bucket_name, blob_path)
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
    return write_raw_partitioned(
        client, bucket_name, source_name, "dt", dt_str, content
    )


def raw_blob_exists_partitioned(
    client: storage.Client,
    bucket_name: str,
    source_name: str,
    partition_key: str,
    partition_value: str,
) -> bool:
    """通用版本的斷點續跑檢查,對應 write_raw_partitioned。"""
    blob_path = raw_blob_path(source_name, partition_key, partition_value)
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
    直接從 GCS 讀取最新一份產業分類清單原始資料(raw layer)。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    prefix = f"raw/{raw_industry_list_source_name(market)}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise RuntimeError(f"找不到 {prefix} 底下的任何資料")

    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]
    print(f"讀取檔案: {latest_blob.name}")
    content = json.loads(latest_blob.download_as_text())
    return content
