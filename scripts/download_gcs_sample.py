"""
從 GCS 下載少量樣本檔案到本機,用於快速迭代測試 Spark 邏輯,
避免每次調整程式碼都要連 GCS 跑一次(啟動 GCS Connector 較耗時)。
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME


def download_blob(bucket_name: str, blob_path: str, local_path: str):
    """下載單一 GCS 檔案到本機指定路徑。"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)
    print(f"✅ 已下載: gs://{bucket_name}/{blob_path} → {local_path}")


if __name__ == "__main__":
    # 挑幾檔 Yahoo TPEx 歷史資料下載到本機測試用
    samples = ["6026", "1240", "6488"]

    for stock_id in samples:
        blob_path = f"raw/yahoo_tpex_history/stock_id={stock_id}/data.json"
        local_path = f"local_output/yahoo_sample/stock_id_{stock_id}.json"
        download_blob(BUCKET_NAME, blob_path, local_path)