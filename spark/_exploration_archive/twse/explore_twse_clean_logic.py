"""
[ARCHIVED] TWSE 清洗邏輯探索(先在本機真實樣本上驗證,還不寫成正式 job)。

狀態: 邏輯已併入 spark/jobs/clean_stock.py 的 clean_twse()。
若要移動此檔案的位置,請確認下方 sys.path.append 的 .parent 層數
與新的資料夾深度一致(目前假設: 專案根目錄/_exploration_archived/twse/)。
"""

import sys
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))


from shared.utils import BUCKET_NAME, load_industry_list_from_gcs
from spark.common.schemas import TWSE_RAW_SCHEMA
from spark.common.spark_session import build_spark_session
from spark.jobs.clean_stock import clean_twse, clean_yahoo_history, unify_twse, unify_yahoo_tpex, filter_official_stocks, clean_fear_greed_history


def explode_daily_data(raw_df):
    """
    把巢狀結構 {dt, fields, data: [[...], [...]]} 展開成扁平結構:
    每一列代表某天某支股票的原始資料(16 個值的陣列),並保留 dt 欄位。
    """
    exploded = raw_df.select(
        F.col("dt"),
        F.explode(F.col("data")).alias("row_values")  # 把 data 陣列展開,一個陣列元素變成一列
    )
    return exploded


def flatten_to_columns(exploded_df):
    """
    把 row_values 陣列(16 個位置固定的值),依照 TWSE_RAW_SCHEMA 的欄位順序,
    拆成獨立的欄位,對應到跟本機探索階段完全一致的 schema。
    """
    field_names = [f.name for f in TWSE_RAW_SCHEMA.fields]  # 取得我們定義好的 16 個欄位名稱,順序一致

    select_exprs = [F.col("dt")]
    for i, name in enumerate(field_names):
        select_exprs.append(F.col("row_values")[i].alias(name))

    return exploded_df.select(*select_exprs)


def explore():
    # spark = (
    #     SparkSession.builder
    #     .appName("stock-pulse-clean-explore")
    #     .master("local[*]")
    #     .getOrCreate()
    # )

    spark = build_spark_session("stock-pulse-backfill-clean-twse-test")

    # with open("local_output/twse_daily_2026-07-09.json", "r", encoding="utf-8") as f:
    #     raw = json.load(f)
    # rows = raw["data"]

    twse_industry_records = load_industry_list_from_gcs(BUCKET_NAME, "TWSE")
    twse_official_ids = [r["公司代號"] for r in twse_industry_records]

    twse_raw = spark.read.option("multiline", "true").option("pathGlobFilter", "data.json").json(f"gs://{BUCKET_NAME}/raw/twse_daily/")
    twse_exploded = explode_daily_data(twse_raw)
    twse_flattened = flatten_to_columns(twse_exploded)
    twse_cleaned = clean_twse(twse_flattened)
    twse_unified = unify_twse(twse_cleaned)
    twse_filtered = filter_official_stocks(twse_unified, twse_official_ids)
    twse_filtered = twse_filtered.withColumn("dt", F.col("dt").cast("string"))

    # df = spark.createDataFrame(rows, schema=TWSE_RAW_SCHEMA)
    # cleaned = clean_twse(df)

    print("=== 清洗後 schema ===")
    twse_filtered.printSchema()

    # print("\n=== 驗證關鍵欄位(前 5 筆,涵蓋 +/-/X 三種案例)===")
    twse_filtered.show(5, truncate=False)

    # print("\n=== 驗證關鍵欄位(前 5 筆,涵蓋 +/-/X 三種案例)===")
    # cleaned.select(
    #     "stock_id", "close_price", "change_symbol_raw",
    #     "change_direction", "change_amount", "signed_change_amount"
    # ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    explore()
