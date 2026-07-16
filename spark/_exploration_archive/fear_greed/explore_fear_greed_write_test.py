"""
[ARCHIVED] fear & greed 清洗後實際寫出至 GCS(parquet, partitionBy dt)的驗證。

狀態: 驗證通過後,寫出邏輯應正式收斂進 spark/jobs/ 底下的正式 job 檔案。
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def clean_fear_greed_history(df):
    df = df.withColumn(
        "dt",
        F.from_unixtime((F.col("x") / 1000).cast("long"), "yyyy-MM-dd")
    )
    df = df.withColumnRenamed("y", "score")
    df = df.withColumnRenamed("rating", "fear_greed_rating")
    df = df.dropDuplicates(["dt"])
    df = df.select("dt", "score", "fear_greed_rating")
    return df


def explore():
    spark = (
        SparkSession.builder
        .appName("fear-greed-write-test")
        .master("local[*]")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/app/secrets/gcp-sa-key.json")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )

    bucket_name = "stock-pulse-data-lake"
    raw_path = f"gs://{bucket_name}/raw/fear_greed_history/range=full/data.json"
    fg_raw = spark.read.option("multiline", "true").json(raw_path)

    fg_data = fg_raw.select(F.explode(F.col("fear_and_greed_historical.data")).alias("record"))
    fg_flat = fg_data.select("record.x", "record.y", "record.rating")

    cleaned = clean_fear_greed_history(fg_flat)
    print(f"清洗後總筆數: {cleaned.count()}")

    output_path = f"gs://{bucket_name}/clean/fear_greed_daily/"
    (
        cleaned
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    print(f"✅ 已寫出至 {output_path}")

    spark.stop()


if __name__ == "__main__":
    explore()
