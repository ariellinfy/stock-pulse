import os
from google.cloud import storage

# 設定金鑰路徑
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/gcp-sa-key.json"

BUCKET_NAME = "stock-pulse-data-lake"


def test_connection():
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
    test_connection()
