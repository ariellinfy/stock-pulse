"""
Fear & Greed 單日清洗 job,供每日排程(Airflow)呼叫。
直接取用 API 回應裡的 fear_and_greed(即時值)欄位,
不展開 fear_and_greed_historical.data(那是給歷史回補用的,且已知有尾端重複的現象)。
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import (
    BUCKET_NAME,
    RAW_FEAR_GREED,
    CLEAN_FEAR_GREED_DAILY,
    raw_blob_path,
    gcs_uri,
)
from spark.common.spark_session import build_spark_session


def clean_single_day_fear_greed(spark, bucket_name: str, target_date: str):
    raw_path = gcs_uri(bucket_name, raw_blob_path(RAW_FEAR_GREED, "dt", target_date))
    raw_df = spark.read.option("multiline", "true").json(raw_path)

    # 展開 historical.data 陣列,找出精確對應 target_date 這一天的記錄
    exploded = raw_df.select(
        F.explode(F.col("fear_and_greed_historical.data")).alias("record")
    )

    all_records = exploded.select(
        F.from_unixtime((F.col("record.x") / 1000).cast("long"), "yyyy-MM-dd").alias(
            "record_date"
        ),
        F.col("record.y").alias("score"),
        F.col("record.rating").alias("fear_greed_rating"),
    )

    # 找出「小於等於 target_date」裡最新的一筆,而非要求精確相等。
    # 這對應美股/台股交易日曆不完全重疊的情況(如台股週末,美股同樣休市,
    # historical.data 裡不會有精確等於今天的記錄,應取用最近一個有效交易日的資料)
    latest_available = (
        all_records.filter(F.col("record_date") <= target_date)
        .orderBy(F.desc("record_date"))
        .limit(1)
    )

    count = latest_available.count()
    if count == 0:
        raise ValueError(
            f"{target_date} 在 historical.data 裡找不到任何 <= 此日期的記錄,清洗失敗"
        )

    cleaned = latest_available.select(
        F.lit(target_date).alias(
            "dt"
        ),  # 仍以 target_date 當作 dt(對應每日排程的分區日期)
        F.round(F.col("score"), 2).alias("score"),
        "fear_greed_rating",
    )

    print(f"{target_date} Fear & Greed 清洗後總筆數: {cleaned.count()}")
    cleaned.show(truncate=False)

    output_path = gcs_uri(bucket_name, CLEAN_FEAR_GREED_DAILY) + "/"
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
