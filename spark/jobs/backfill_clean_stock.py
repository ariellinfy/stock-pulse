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
from shared.utils import BUCKET_NAME, load_industry_list_from_gcs
from spark.common.schemas import TWSE_RAW_SCHEMA
from spark.common.spark_session import build_spark_session
from spark.jobs.clean_stock import (
    clean_twse,
    clean_yahoo_history,
    unify_twse,
    unify_yahoo_tpex,
    filter_official_stocks,
    clean_fear_greed_history,
)
from spark.jobs.clean_industry_list import clean_and_write_industry_list


def explode_daily_data(raw_df):
    """
    把巢狀結構 {dt, fields, data: [[...], [...]]} 展開成扁平結構:
    每一列代表某天某支股票的原始資料(16 個值的陣列),並保留 dt 欄位。
    """
    raw_df = raw_df.filter(
        F.col("data").isNotNull()
    )  # 明確排除沒有 data 欄位的記錄(如標記檔)
    exploded = raw_df.select(
        F.col("dt"),
        F.explode(F.col("data")).alias(
            "row_values"
        ),  # 把 data 陣列展開,一個陣列元素變成一列
    )
    return exploded


def flatten_to_columns(exploded_df):
    """
    把 row_values 陣列(16 個位置固定的值),依照 TWSE_RAW_SCHEMA 的欄位順序,
    拆成獨立的欄位,對應到跟本機探索階段完全一致的 schema。
    """
    field_names = [
        f.name for f in TWSE_RAW_SCHEMA.fields
    ]  # 取得我們定義好的 16 個欄位名稱,順序一致

    select_exprs = [F.col("dt")]
    for i, name in enumerate(field_names):
        select_exprs.append(F.col("row_values")[i].alias(name))

    return exploded_df.select(*select_exprs)


def backfill_all_markets(spark, bucket_name: str, twse_official_ids: list[str]):
    # TWSE 歷史
    twse_raw = (
        spark.read.option("multiline", "true")
        .option("pathGlobFilter", "data.json")
        .json(f"gs://{bucket_name}/raw/twse_daily/")
    )
    twse_exploded = explode_daily_data(twse_raw)
    twse_flattened = flatten_to_columns(twse_exploded)
    twse_cleaned = clean_twse(twse_flattened)
    twse_unified = unify_twse(twse_cleaned)
    twse_filtered = filter_official_stocks(twse_unified, twse_official_ids)
    twse_filtered = twse_filtered.withColumn("dt", F.col("dt").cast("string"))

    # Yahoo TPEx 歷史(TPEx 唯一的歷史資料來源,官方每日端點無法查歷史)
    yahoo_raw = spark.read.option("multiline", "true").json(
        f"gs://{bucket_name}/raw/yahoo_tpex_history/"
    )
    yahoo_cleaned = clean_yahoo_history(yahoo_raw)
    yahoo_unified = unify_yahoo_tpex(yahoo_cleaned)

    # 關鍵: 先合併,再一次寫出,避免動態分區覆寫互相沖掉彼此的資料
    combined = twse_filtered.unionByName(yahoo_unified)

    print(f"合併後總筆數: {combined.count()}")
    combined.groupBy("market").count().show()

    output_path = f"gs://{bucket_name}/clean/stock_daily/"

    (
        combined.repartition(F.col("dt"))
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    print(f"✅ 全市場歷史已寫出至 {output_path}")


def backfill_fear_greed(spark, bucket_name: str):
    raw_path = f"gs://{bucket_name}/raw/fear_greed_history/range=full/data.json"
    fg_raw = spark.read.option("multiline", "true").json(raw_path)

    fg_data = fg_raw.select(
        F.explode(F.col("fear_and_greed_historical.data")).alias("record")
    )
    fg_flat = fg_data.select("record.x", "record.y", "record.rating")

    cleaned = clean_fear_greed_history(fg_flat)

    print(f"Fear & Greed 清洗後總筆數: {cleaned.count()}")

    output_path = f"gs://{bucket_name}/clean/fear_greed_daily/"
    (cleaned.write.mode("overwrite").partitionBy("dt").parquet(output_path))

    print(f"✅ Fear & Greed 已寫出至 {output_path}")


if __name__ == "__main__":
    spark = build_spark_session("stock-pulse-backfill-clean-all")

    clean_and_write_industry_list(spark, BUCKET_NAME)

    twse_industry_records = load_industry_list_from_gcs(BUCKET_NAME, "TWSE")
    twse_official_ids = [r["公司代號"] for r in twse_industry_records]

    backfill_all_markets(spark, BUCKET_NAME, twse_official_ids)
    backfill_fear_greed(spark, BUCKET_NAME)

    spark.stop()
