import os
from google.cloud import storage

# 設定金鑰路徑
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/gcp-sa-key.json"

BUCKET_NAME = "stock-pulse-data-lake"


def check_connection():
    """
    手動驗證用,不是 pytest 測試(檔名故意不用 test_ 開頭,避免 pytest tests/
    自動抓到這支、意外打真實 GCS)。需要真實憑證,執行方式:
        PYTHONPATH=. python tests/manual_check_gcs_connection.py
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 寫入一個測試檔案,確認有寫入權限
    blob = bucket.blob("raw/_connection_test.txt")
    blob.upload_from_string("hello from stock-pulse setup test")
    print(f"✅ 成功寫入 gs://{BUCKET_NAME}/raw/_connection_test.txt")

    # 讀回來,確認有讀取權限
    content = blob.download_as_text()
    print(f"✅ 成功讀回內容:{content}")


if __name__ == "__main__":
    check_connection()
