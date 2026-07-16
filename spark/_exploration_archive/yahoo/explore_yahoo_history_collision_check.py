"""
[ARCHIVED] Yahoo TPEx 歷史資料清洗/統一 schema 探索,
以及 GCS 上依 stock_id 分區讀取時是否會有欄位衝突的驗證。

狀態: clean_yahoo_history() / unify_yahoo_tpex() 已併入 spark/jobs/clean_stock.py。
若要移動此檔案的位置,請確認下方 sys.path.append 的 .parent 層數
與新的資料夾深度一致(目前假設: 專案根目錄/_exploration_archived/yahoo/)。
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, LongType


def clean_yahoo_history(df):
    df = df.withColumn("open", F.round(F.col("open"), 2))
    df = df.withColumn("high", F.round(F.col("high"), 2))
    df = df.withColumn("low", F.round(F.col("low"), 2))
    df = df.withColumn("close", F.round(F.col("close"), 2))
    return df


def unify_yahoo_tpex(df):
    return df.select(
        F.col("trade_date").alias("dt"),
        F.col("stock_id").cast(StringType()),
        F.lit(None).cast(StringType()).alias("stock_name"),
        F.col("market"),
        F.col("open").alias("open_price"),
        F.col("high").alias("high_price"),
        F.col("low").alias("low_price"),
        F.col("close").alias("close_price"),
        F.lit(None).cast(DoubleType()).alias("average_price"),
        F.col("volume").alias("trade_volume"),
        F.lit(None).cast(LongType()).alias("trade_value"),
        F.lit(None).cast(LongType()).alias("transaction_count"),
        F.lit(None).cast(DoubleType()).alias("change_amount"),
        F.lit(None).cast(DoubleType()).alias("last_bid_price"),
        F.lit(None).cast(LongType()).alias("last_bid_volume"),
        F.lit(None).cast(DoubleType()).alias("last_ask_price"),
        F.lit(None).cast(LongType()).alias("last_ask_volume"),
        F.lit(None).cast(DoubleType()).alias("pe_ratio"),
        F.lit(None).cast(LongType()).alias("issued_shares"),
    )


def explore():
    # 只讀單一分區資料夾,範圍小,速度快,專門驗證欄位衝突的實際結果
    spark = (
        SparkSession.builder
        .appName("stock-pulse-yahoo-collision-check")
        .master("local[*]")
        .config("spark.jars", "/home/fy/spark_jars/gcs-connector-hadoop3-latest.jar")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/fy/stock-pulse/secrets/gcp-sa-key.json")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )

#     raw_path = "gs://stock-pulse-data-lake/raw/yahoo_tpex_history/"
#     raw_df = spark.read.option("multiline", "true").json(raw_path)

#     cleaned_yahoo = clean_yahoo_history(raw_df)
#     unified_yahoo = unify_yahoo_tpex(cleaned_yahoo)

#     print(f"轉換後總筆數: {unified_yahoo.count()}")
#     unified_yahoo.printSchema()
#     unified_yahoo.filter(F.col("stock_id") == "6026").show(5, truncate=False)

#     # print(f"讀取到 {raw_df.count()} 個檔案(每列代表一支股票,還未展開每日資料)")
#     # raw_df.printSchema()
#     # raw_df.show(3, truncate=False)

    single_path = "gs://stock-pulse-data-lake/raw/yahoo_tpex_history/stock_id=6026/"
    raw_df = spark.read.option("multiline", "true").json(single_path)

    print("=== 欄位清單(確認是否真的只剩一個 stock_id)===")
    print(raw_df.columns)
    raw_df.select("stock_id").distinct().show()

    spark.stop()


if __name__ == "__main__":
    explore()
