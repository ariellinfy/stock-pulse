"""
[ARCHIVED] 在 Docker 容器內驗證 fear & greed 清洗邏輯(展平巢狀 JSON、去重 dt),
含針對重複 dt 案例的原始資料稽核。

原始執行方式(專案根目錄):
docker run --rm \
  -v ~/stock-pulse/secrets:/app/secrets:ro \
  -v ~/stock-pulse/explore_spark_docker_fear_greed.py:/app/test.py \
  stock-pulse-spark \
  /opt/spark/bin/spark-submit /app/test.py

狀態: clean_fear_greed_history() 已併入正式清洗邏輯(見 explore_fear_greed_write_test.py 的寫出版本)。
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
        .appName("fear-greed-clean-test")
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
    print(f"清洗後筆數: {cleaned.count()}")
    cleaned.orderBy("dt").show(5)
    cleaned.orderBy(F.desc("dt")).show(5)

    # 在轉換出 dt 之後、去重之前,先看看有沒有重複的 dt
    fg_with_dt = fg_flat.withColumn(
        "dt",
        F.from_unixtime((F.col("x") / 1000).cast("long"), "yyyy-MM-dd")
    )

    dt_counts = fg_with_dt.groupBy("dt").count().filter(F.col("count") > 1)
    print(f"重複的 dt 數量: {dt_counts.count()}")
    dt_counts.show()

    # 如果有重複,把那幾天的完整原始資料印出來看
    if dt_counts.count() > 0:
        duplicate_dates = [row["dt"] for row in dt_counts.collect()]
        fg_with_dt.filter(F.col("dt").isin(duplicate_dates)).orderBy("dt", "x").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    explore()
