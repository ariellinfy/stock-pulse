"""
共用工具模組 v4
- USE_GCS=true 時上傳 GCS
- Blob 格式:
    每日資料: raw/{source}/dt=YYYY-MM-DD/data.json          （覆蓋 = idempotent）
    每小時資料: raw/{source}/dt=YYYY-MM-DD/hr=HH/data.json  （各小時獨立）
- 重跑同一天/同一小時會直接覆蓋，保證 Idempotency
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 自動載入專案根目錄的 .env
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── 路徑設定 ──────────────────────────────────────────────
PROJECT_ROOT   = Path(__file__).resolve().parent.parent
DATA_TEST_ROOT = PROJECT_ROOT / "data_test"

RAW_STOCK_DIR = DATA_TEST_ROOT / "raw" / "stock"
RAW_NEWS_DIR  = DATA_TEST_ROOT / "raw" / "news"
RAW_FG_DIR    = DATA_TEST_ROOT / "raw" / "fear_greed"

# ── GCS 設定 ─────────────────────────────────────────────
USE_GCS        = os.environ.get("USE_GCS", "false").lower() == "true"
GCS_BUCKET     = os.environ.get("GCP_BUCKET_NAME", "")
GCS_RAW_PREFIX = os.environ.get("GCS_RAW_PREFIX", "raw")


# ── Logger ───────────────────────────────────────────────
def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


_logger = get_logger("utils")


# ── 時間工具 ─────────────────────────────────────────────
def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def hour_str() -> str:
    return datetime.now().strftime("%H")


def ts_str() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── 儲存邏輯 ─────────────────────────────────────────────

def save_json(
    data: list | dict,
    directory: Path,
    filename: str,
    gcs_source: str = "",
    hourly: bool = False,     # True = 每小時執行（新聞），加 hr=HH 分層
) -> Path:
    """
    統一儲存入口。本地永遠寫檔，USE_GCS=true 時額外上傳 GCS。

    GCS Blob 路徑（Idempotent — 重跑同一週期會覆蓋）:
      每日: raw/{source}/dt=YYYY-MM-DD/data.json
      每時: raw/{source}/dt=YYYY-MM-DD/hr=HH/data.json

    範例:
      raw/twse/dt=2026-07-02/data.json
      raw/cnyes/dt=2026-07-02/hr=08/data.json
      raw/cnn_fear_greed/dt=2026-07-02/data.json
    """
    # 1. 寫本地
    ensure_dir(directory)
    local_path = directory / filename
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    if not USE_GCS:
        _logger.debug(f"[本地] 已儲存 {local_path}")
        return local_path

    # 2. 上傳 GCS
    if not gcs_source:
        _logger.error("[GCS] gcs_source 未傳入，無法上傳")
        return local_path

    _upload_to_gcs(local_path, gcs_source, hourly)
    return local_path


def _upload_to_gcs(local_path: Path, gcs_source: str, hourly: bool) -> None:
    """
    上傳到 GCS。重跑同一週期會覆蓋同一個 blob，保證 Idempotency。
    """
    if not GCS_BUCKET:
        _logger.error("USE_GCS=true 但 GCP_BUCKET_NAME 未設定，請檢查 .env")
        return

    try:
        from google.cloud import storage

        date_part = f"dt={today_str()}"
        hour_part = f"/hr={hour_str()}" if hourly else ""
        blob_path = f"{GCS_RAW_PREFIX}/{gcs_source}/{date_part}{hour_part}/data.json"

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(blob_path)

        # GCS upload_from_filename 預設覆蓋已存在的 blob → Idempotent ✅
        blob.upload_from_filename(str(local_path), content_type="application/json")

        _logger.info(f"[GCS] gs://{GCS_BUCKET}/{blob_path}")

    except Exception as e:
        _logger.error(f"[GCS] 上傳失敗（本地檔案仍保留）: {e}")