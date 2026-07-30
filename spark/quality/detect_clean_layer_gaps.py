"""
在 clean/stock_daily/(已清洗、已過濾)這一層,用上市日期校正後的應有股票數,
跟實際出現的股票數比對,找出交易日層級的資料缺口。

用途: 跟 scripts/adhoc/scan_raw_data_gaps.py(raw 層)交叉驗證同一批異常日期,
      確保結論不是單一計算路徑的偶然結果。
"""

import sys
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import Row

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import BUCKET_NAME
from spark.common.spark_session import build_spark_session


def detect_clean_layer_gaps(
    spark, bucket_name: str, twse_industry_records: list[dict], gap_threshold: int = 10
):
    """
    gap_threshold: 只回報 gap 超過此門檻的日子(預設 10,排除清洗過程正常的個位數落差,如個股停牌)。
    """
    df = spark.read.parquet(f"gs://{bucket_name}/clean/stock_daily/").filter(
        F.col("market") == "TWSE"
    )

    listing_dates = spark.createDataFrame(
        [(r["公司代號"], r["上市日期"]) for r in twse_industry_records],
        ["stock_id", "listing_date_raw"],
    ).withColumn("listing_date", F.to_date(F.col("listing_date_raw"), "yyyyMMdd"))

    daily_counts = df.groupBy("dt").agg(
        F.countDistinct("stock_id").alias("actual_count")
    )

    trading_days = [row["dt"] for row in daily_counts.select("dt").distinct().collect()]
    expected_counts = [
        Row(
            dt=day,
            expected_count_adjusted=listing_dates.filter(
                F.col("listing_date") <= F.to_date(F.lit(day))
            ).count(),
        )
        for day in trading_days
    ]
    expected_df = spark.createDataFrame(expected_counts)

    result = daily_counts.join(expected_df, on="dt")
    result = result.withColumn(
        "gap", F.col("expected_count_adjusted") - F.col("actual_count")
    )

    gaps = result.filter(F.col("gap") > gap_threshold).orderBy(F.desc("gap"))
    print(
        f"clean 層,gap > {gap_threshold} 的交易日數: {gaps.count()} / {result.count()}"
    )
    gaps.show(30, truncate=False)

    return gaps


if __name__ == "__main__":
    spark = build_spark_session("stock-pulse-detect-clean-gaps")

    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    detect_clean_layer_gaps(spark, BUCKET_NAME, twse_industry_records)

    spark.stop()
