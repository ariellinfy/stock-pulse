"""
[ARCHIVED] 驗證 Spark 能透過 GCS Connector 讀取 GCS 上的資料。
先做最小驗證(能不能連上、能不能讀到一個既有檔案),不牽扯清洗邏輯。

狀態: 一次性環境驗證,結論(連線設定可行)已固定寫入各正式 job 的 SparkSession config。
"""

from pyspark.sql import SparkSession


def explore():
    spark = (
        SparkSession.builder.appName("stock-pulse-gcs-test")
        .master("local[*]")
        .config("spark.jars", "/home/fy/spark_jars/gcs-connector-hadoop3-latest.jar")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/home/fy/stock-pulse/secrets/gcp-sa-key.json",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .getOrCreate()
    )

    print("✅ SparkSession(含 GCS Connector 設定)啟動成功")

    # 讀取一個我們已知存在的檔案:TWSE 2026-07-08 的原始資料
    gcs_path = "gs://stock-pulse-data-lake/raw/twse_daily/dt=2026-07-08/data.json"

    try:
        df = spark.read.text(
            gcs_path
        )  # 先用最簡單的 text 讀取,只驗證連線,不解析 JSON 結構
        print(f"✅ 成功連上 GCS,讀到 {df.count()} 行原始文字")
        df.show(1, truncate=100)
    except Exception as e:
        print(f"❌ GCS 連線失敗: {e}")

    spark.stop()


if __name__ == "__main__":
    explore()
