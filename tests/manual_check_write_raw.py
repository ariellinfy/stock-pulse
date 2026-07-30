from datetime import date
from shared.utils import get_gcs_client, write_raw_json

BUCKET_NAME = "stock-pulse-data-lake"


def check_idempotent_write():
    """
    手動驗證用,不是 pytest 測試(檔名故意不用 test_ 開頭,避免 pytest tests/
    自動抓到這支、意外打真實 GCS)。需要真實憑證,執行方式:
        PYTHONPATH=. python tests/manual_check_write_raw.py
    """
    client = get_gcs_client()
    target_date = date(2025, 1, 2)

    # 第一次寫入
    write_raw_json(
        client,
        BUCKET_NAME,
        "twse_daily",
        target_date,
        '{"stock_id": "2330", "close": 590.0}',
    )

    # 模擬重複執行同一天(內容不同,驗證是否真的覆蓋而非疊加)
    write_raw_json(
        client,
        BUCKET_NAME,
        "twse_daily",
        target_date,
        '{"stock_id": "2330", "close": 999.0}',
    )


if __name__ == "__main__":
    check_idempotent_write()
