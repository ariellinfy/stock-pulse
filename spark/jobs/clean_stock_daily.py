"""
單日清洗 job,供每日排程(Airflow)呼叫。
只處理指定的單一交易日,不像 backfill_clean_stock.py 那樣處理整個歷史範圍。
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import (
    BUCKET_NAME,
    RAW_TWSE_DAILY,
    RAW_TPEX_DAILY,
    CLEAN_STOCK_DAILY,
    load_industry_list_from_gcs,
    raw_blob_path,
    gcs_uri,
)
from spark.common.schemas import TWSE_RAW_SCHEMA, TPEX_RAW_SCHEMA
from spark.common.spark_session import build_spark_session
from spark.jobs.clean_stock import (
    clean_twse,
    clean_tpex,
    unify_twse,
    unify_tpex,
    filter_official_stocks,
)


def clean_single_day(
    spark,
    bucket_name: str,
    target_date: str,
    twse_official_ids: list[str],
    tpex_official_ids: list[str],
):
    """
    清洗指定單一日期的 TWSE + TPEx 每日行情,動態分區覆寫寫出。
    只會動到 dt=target_date 這一個分區,不影響其他歷史資料。
    """
    twse_path = gcs_uri(bucket_name, raw_blob_path(RAW_TWSE_DAILY, "dt", target_date))
    tpex_path = gcs_uri(bucket_name, raw_blob_path(RAW_TPEX_DAILY, "dt", target_date))

    # 單日原始檔案結構是 {"fields":[...], "data":[[...]]},用我們熟悉的單日讀取方式,
    # 不需要像 backfill 版本那樣 explode 多天份的巢狀結構
    twse_raw_json = spark.read.option("multiline", "true").json(twse_path)
    twse_df = spark.createDataFrame(
        twse_raw_json.select(F.explode("data").alias("row")).rdd.map(
            lambda r: r["row"]
        ),
        schema=TWSE_RAW_SCHEMA,
    )
    twse_df = twse_df.withColumn("dt", F.lit(target_date))

    tpex_raw_json = spark.read.option("multiline", "true").json(tpex_path)
    tpex_df = spark.createDataFrame(
        tpex_raw_json.select(F.explode("data").alias("row")).rdd.map(
            lambda r: r["row"]
        ),
        schema=TPEX_RAW_SCHEMA,
    )
    tpex_df = tpex_df.withColumn("dt", F.lit(target_date))

    twse_cleaned = clean_twse(twse_df)
    twse_unified = unify_twse(twse_cleaned)
    twse_filtered = filter_official_stocks(twse_unified, twse_official_ids)

    tpex_cleaned = clean_tpex(tpex_df)
    tpex_unified = unify_tpex(tpex_cleaned)
    tpex_filtered = filter_official_stocks(tpex_unified, tpex_official_ids)

    combined = twse_filtered.unionByName(tpex_filtered)

    print(f"{target_date} 清洗後總筆數: {combined.count()}")

    output_path = gcs_uri(bucket_name, CLEAN_STOCK_DAILY) + "/"
    combined.write.mode("overwrite").partitionBy("dt").parquet(output_path)

    print(f"✅ {target_date} 清洗完成並寫出")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="格式 YYYY-MM-DD")
    args = parser.parse_args()

    spark = build_spark_session(f"stock-pulse-clean-daily-{args.date}")

    twse_records = load_industry_list_from_gcs(BUCKET_NAME, "TWSE")
    tpex_records = load_industry_list_from_gcs(BUCKET_NAME, "TPEx")

    clean_single_day(
        spark,
        BUCKET_NAME,
        args.date,
        [r["公司代號"] for r in twse_records],
        [r["公司代號"] for r in tpex_records],
    )

    spark.stop()
