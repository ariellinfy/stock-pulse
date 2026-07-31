"""
TWSE + Yahoo TPEx 歷史資料批次清洗 job。
讀取 raw/twse_daily/ 底下全部日期分區,套用既有清洗邏輯,
讀取 raw/yahoo_tpex_history/ 底下全部股票分區,套用既有清洗轉換邏輯,
依日期動態分區覆寫寫出至 clean/stock_daily/。
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import (
    BUCKET_NAME,
    RAW_TWSE_DAILY,
    RAW_YAHOO_TPEX_HISTORY,
    RAW_FEAR_GREED_HISTORY,
    CLEAN_STOCK_DAILY,
    CLEAN_FEAR_GREED_DAILY,
    load_industry_list_from_gcs,
    raw_blob_path,
    gcs_uri,
)
from spark.common.schemas import TWSE_RAW_SCHEMA
from spark.common.spark_session import build_spark_session
from spark.jobs.clean_stock import (
    clean_twse,
    clean_yahoo_history,
    unify_twse,
    unify_yahoo_tpex,
    filter_official_stocks,
    clean_fear_greed_history,
    explode_and_flatten,
)
from spark.jobs.clean_industry_list import clean_and_write_industry_list


def backfill_all_markets(spark, bucket_name: str, twse_official_ids: list[str]):
    # TWSE 歷史;dt 欄位由 Hive-style 分區資料夾(dt=yyyy-MM-dd)自動推斷得到
    twse_raw = (
        spark.read.option("multiline", "true")
        .option("pathGlobFilter", "data.json")
        .json(gcs_uri(bucket_name, f"raw/{RAW_TWSE_DAILY}/"))
    )
    twse_flattened = explode_and_flatten(twse_raw, TWSE_RAW_SCHEMA)
    twse_cleaned = clean_twse(twse_flattened)
    twse_unified = unify_twse(twse_cleaned)
    twse_filtered = filter_official_stocks(twse_unified, twse_official_ids)
    twse_filtered = twse_filtered.withColumn("dt", F.col("dt").cast("string"))

    # Yahoo TPEx 歷史(TPEx 唯一的歷史資料來源,官方每日端點無法查歷史)
    yahoo_raw = spark.read.option("multiline", "true").json(
        gcs_uri(bucket_name, f"raw/{RAW_YAHOO_TPEX_HISTORY}/")
    )
    yahoo_cleaned = clean_yahoo_history(yahoo_raw)
    yahoo_unified = unify_yahoo_tpex(yahoo_cleaned)

    # 關鍵: 先合併,再一次寫出,避免動態分區覆寫互相沖掉彼此的資料
    combined = twse_filtered.unionByName(yahoo_unified)

    print(f"合併後總筆數: {combined.count()}")
    combined.groupBy("market").count().show()

    output_path = gcs_uri(bucket_name, CLEAN_STOCK_DAILY) + "/"

    (
        combined.repartition(F.col("dt"))
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    print(f"✅ 全市場歷史已寫出至 {output_path}")


def backfill_clean_fear_greed(spark, bucket_name: str):
    """
    命名特意跟 scripts/backfill/backfill_fear_greed.py 的 backfill_fear_greed()
    區分開來:那支是抓 raw 資料寫入 GCS,這支是清洗 raw 寫出 clean layer——
    兩者曾經同名,搜尋/閱讀時容易搞混。
    """
    raw_path = gcs_uri(
        bucket_name, raw_blob_path(RAW_FEAR_GREED_HISTORY, "range", "full")
    )
    fg_raw = spark.read.option("multiline", "true").json(raw_path)

    fg_data = fg_raw.select(
        F.explode(F.col("fear_and_greed_historical.data")).alias("record")
    )
    fg_flat = fg_data.select("record.x", "record.y", "record.rating")

    cleaned = clean_fear_greed_history(fg_flat)

    print(f"Fear & Greed 清洗後總筆數: {cleaned.count()}")

    output_path = gcs_uri(bucket_name, CLEAN_FEAR_GREED_DAILY) + "/"
    (cleaned.write.mode("overwrite").partitionBy("dt").parquet(output_path))

    print(f"✅ Fear & Greed 已寫出至 {output_path}")


if __name__ == "__main__":
    spark = build_spark_session("stock-pulse-backfill-clean-all")

    clean_and_write_industry_list(spark, BUCKET_NAME)

    twse_industry_records = load_industry_list_from_gcs(BUCKET_NAME, "TWSE")
    twse_official_ids = [r["公司代號"] for r in twse_industry_records]

    backfill_all_markets(spark, BUCKET_NAME, twse_official_ids)
    backfill_clean_fear_greed(spark, BUCKET_NAME)

    spark.stop()
