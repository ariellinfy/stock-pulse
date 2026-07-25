from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def clean_yahoo_history(df):
    for col_name in ["open", "high", "low", "close"]:
        df = df.withColumn(
            col_name,
            F.when(F.isnan(F.col(col_name)), F.lit(None)).otherwise(F.col(col_name)),
        )
    df = df.withColumn("open", F.round(F.col("open"), 2))
    df = df.withColumn("high", F.round(F.col("high"), 2))
    df = df.withColumn("low", F.round(F.col("low"), 2))
    df = df.withColumn("close", F.round(F.col("close"), 2))
    return df


spark = SparkSession.builder.appName("nan-fix-test").master("local[*]").getOrCreate()

raw_df = spark.read.option("multiline", "true").json(
    "local_output/yahoo_sample/stock_id_7794.json"
)
cleaned = clean_yahoo_history(raw_df)

print("=== 2025-07-03 這筆,修正後的結果 ===")
cleaned.filter(F.col("trade_date") == "2025-07-03").show(truncate=False)

print("\n=== 確認全部 NaN 都已轉成 null(這個 count 應該是 0)===")
nan_count = cleaned.filter(
    F.isnan(F.col("close"))
    | F.isnan(F.col("open"))
    | F.isnan(F.col("high"))
    | F.isnan(F.col("low"))
).count()
print(f"仍為 NaN 的筆數: {nan_count}")

spark.stop()
