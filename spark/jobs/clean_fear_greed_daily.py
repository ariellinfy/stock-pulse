"""
Fear & Greed 單日清洗 job,供每日排程(Airflow)呼叫。
直接取用 API 回應裡的 fear_and_greed(即時值)欄位,
不展開 fear_and_greed_historical.data(那是給歷史回補用的,且已知有尾端重複的現象)。
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import BUCKET_NAME
from spark.common.spark_session import build_spark_session


def clean_single_day_fear_greed(spark, bucket_name: str, target_date: str):
    raw_path = f"gs://{bucket_name}/raw/fear_greed/dt={target_date}/data.json"
    raw_df = spark.read.option("multiline", "true").json(raw_path)

    # 展開 historical.data 陣列,找出精確對應 target_date 這一天的記錄
    exploded = raw_df.select(
        F.explode(F.col("fear_and_greed_historical.data")).alias("record")
    )

    matched = exploded.select(
        F.from_unixtime((F.col("record.x") / 1000).cast("long"), "yyyy-MM-dd").alias("record_date"),
        F.col("record.y").alias("score"),
        F.col("record.rating").alias("fear_greed_rating"),
    ).filter(F.col("record_date") == target_date)

    cleaned = matched.select(
        F.lit(target_date).alias("dt"),
        F.round(F.col("score"), 2).alias("score"),
        "fear_greed_rating",
    )

    count = cleaned.count()
    if count == 0:
        raise ValueError(f"{target_date} 在 historical.data 裡找不到對應記錄,清洗失敗")
    if count > 1:
        print(f"⚠️ {target_date} 找到 {count} 筆記錄(預期 1 筆),取第一筆")
        cleaned = cleaned.limit(1)

    print(f"{target_date} Fear & Greed 清洗後總筆數: {cleaned.count()}")
    cleaned.show(truncate=False)

    output_path = f"gs://{bucket_name}/clean/fear_greed_daily/"
    cleaned.write.mode("overwrite").partitionBy("dt").parquet(output_path)

    print(f"✅ {target_date} Fear & Greed 清洗完成並寫出")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="格式 YYYY-MM-DD")
    args = parser.parse_args()

    spark = build_spark_session(f"stock-pulse-clean-fear-greed-{args.date}")
    clean_single_day_fear_greed(spark, BUCKET_NAME, args.date)
    spark.stop()
