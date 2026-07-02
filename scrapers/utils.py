"""
共用工具模組
- 統一管理本地測試時的儲存路徑
- 後續換成 GCS 上傳時，只需修改此檔案的 save_json / save_csv 即可
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

# ── 基本路徑設定 ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_TEST_ROOT = PROJECT_ROOT / "data_test"

RAW_STOCK_DIR   = DATA_TEST_ROOT / "raw" / "stock"
RAW_NEWS_DIR    = DATA_TEST_ROOT / "raw" / "news"
RAW_FG_DIR      = DATA_TEST_ROOT / "raw" / "fear_greed"


def get_logger(name: str) -> logging.Logger:
    """統一 log 格式"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_json(data: list | dict, directory: Path, filename: str) -> Path:
    """
    將資料存成 JSON 檔。
    filename 範例: "twse_2025-01-15.json"
    """
    ensure_dir(directory)
    filepath = directory / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filepath


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def ts_str() -> str:
    """含小時的時間戳，給每小時執行的腳本用"""
    return datetime.now().strftime("%Y-%m-%d_%H")