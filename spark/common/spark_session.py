"""
共用的 SparkSession 建立工具。
本地測試與之後 Docker/Airflow 內執行都呼叫這裡，確保設定一致。
"""
import os
from pathlib import Path
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# 自動載入專案根目錄的 .env
load_dotenv()

def build_spark_session(app_name: str) -> SparkSession:
    """
    建立含 GCS Connector 設定的 SparkSession。

    需要的環境變數:
        SPARK_GCS_JAR_PATH: GCS connector JAR 檔案路徑
        GCP_SA_KEY_PATH: GCP Service Account 金鑰路徑
    """
    gcs_jar_path = os.environ.get("SPARK_GCS_JAR_PATH")
    gcp_key_path = os.environ.get("GCP_SA_KEY_PATH")

    if not gcs_jar_path or not gcp_key_path:
        raise RuntimeError(
            "缺少必要環境變數 SPARK_GCS_JAR_PATH 或 GCP_SA_KEY_PATH,"
            "請確認 .env 檔案已正確設定並載入"
        )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # ==========================================
        # 1. 資源與效能優化
        # ==========================================
        .config("spark.driver.memory", "2g") 
        # WSL/Docker 資源有限，限制 Driver 記憶體避免 OutOfMemoryError 導致系統崩潰。

        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        .config("spark.sql.shuffle.partitions", "4") 
        # 絕對必要的設定！預設 200 會在 Join 或 GroupBy 時產生 200 個小檔案，對於幾 MB 的股市資料來說，光是協調 Task 就會拖慢幾十倍速度。
        
        .config("spark.sql.parquet.compression.codec", "snappy")
        # 寫入 GCS Clean Layer 時的預設壓縮格式，讀寫平衡最佳。
        
        # ==========================================
        # 2. 業務邏輯設定
        # ==========================================
        .config("spark.sql.session.timeZone", "Asia/Taipei")
        # 處理股票交易日與新聞發布時間時，統一使用台北時區，避免因為 UTC 時差導致 Join 錯位。

        # ==========================================
        # 3. GCS 連線
        # ==========================================
        .config("spark.jars", gcs_jar_path)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark