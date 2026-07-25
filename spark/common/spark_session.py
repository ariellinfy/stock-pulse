"""
共用的 SparkSession 建立工具。
本地測試與之後 Docker/Airflow 內執行都呼叫這裡，確保設定一致。
"""
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# 自動載入專案根目錄的 .env
load_dotenv()


def build_spark_session(app_name: str) -> SparkSession:
    """
    建立含 GCS Connector 設定的 SparkSession。

    環境變數:
        SPARK_GCS_JAR_PATH: GCS connector JAR 路徑(選填)。
            本機開發需要明確指定;若在已內建 GCS Connector 的容器環境
            (JAR 已放在 /opt/spark/jars/)執行,可以不設定此變數。
        GCP_SA_KEY_PATH: GCP Service Account 金鑰路徑(必填)
    """
    gcs_jar_path = os.environ.get("SPARK_GCS_JAR_PATH")  # 選填,不再強制檢查
    gcp_key_path = os.environ.get("GCP_SA_KEY_PATH")

    if not gcp_key_path:
        raise RuntimeError(
            "缺少必要環境變數 GCP_SA_KEY_PATH,請確認 .env 檔案已正確設定並載入"
        )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "Asia/Taipei")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    )

    # 只有明確提供 JAR 路徑時才加這個設定(本機開發情境);
    # 容器環境已內建 GCS Connector 在 /opt/spark/jars/,不需要此設定
    if gcs_jar_path:
        builder = builder.config("spark.jars", gcs_jar_path)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
