from datetime import date
from shared.utils import get_gcs_client, write_raw_json

BUCKET_NAME = "stock-pulse-data-lake"


def test_idempotent_write():
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
    test_idempotent_write()
