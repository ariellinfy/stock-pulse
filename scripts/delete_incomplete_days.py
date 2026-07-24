import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME


def delete_and_requeue(bucket_name: str, dates_to_fix: list[str]):
    """
    刪除指定日期的不完整 raw data,讓斷點續跑機制之後能重新抓取。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    for dt_str in dates_to_fix:
        blob_path = f"raw/twse_daily/dt={dt_str}/data.json"
        blob = bucket.blob(blob_path)
        if blob.exists():
            blob.delete()
            print(f"🗑️ 已刪除: {blob_path}")
        else:
            print(f"⚠️ 找不到: {blob_path}")


if __name__ == "__main__":
    dates_to_fix = ["2025-12-17", "2024-07-11", "2024-10-13", "2024-11-06"]
    delete_and_requeue(BUCKET_NAME, dates_to_fix)
