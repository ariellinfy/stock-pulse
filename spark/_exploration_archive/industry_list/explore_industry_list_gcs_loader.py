"""
[ARCHIVED] 驗證直接從 GCS 讀取最新一份產業分類清單(取最新檔名排序),
取代依賴本機 local_output/ 檔案的做法。

狀態: 若驗證通過,load_industry_list_from_gcs() 適合搬進 shared/utils.py
或 spark/common/ 作為正式共用函式。
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME
import json


def load_industry_list_from_gcs(bucket_name: str, market: str) -> list[dict]:
    """
    直接從 GCS 讀取最新一份產業分類清單,取代依賴本機 local_output/ 檔案的做法。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    prefix = f"raw/industry_list_{market.lower()}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise RuntimeError(f"找不到 {prefix} 底下的任何資料")

    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]
    print(f"讀取檔案: {latest_blob.name}")
    content = json.loads(latest_blob.download_as_text())
    return content


if __name__ == "__main__":
    twse_records = load_industry_list_from_gcs(BUCKET_NAME, "TWSE")
    print(f"TWSE 筆數: {len(twse_records)}")
    print(f"範例(前 1 筆): {twse_records[0]}")

    tpex_records = load_industry_list_from_gcs(BUCKET_NAME, "TPEx")
    print(f"\nTPEx 筆數: {len(tpex_records)}")
