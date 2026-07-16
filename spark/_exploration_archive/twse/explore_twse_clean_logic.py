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
from spark.common.schemas import TWSE_RAW_SCHEMA


def strip_commas_and_cast(col_name: str, target_type):
    """去除千分位逗號,轉型成指定數字型態。"""
    return F.regexp_replace(F.col(col_name), ",", "").cast(target_type)


def clean_twse(df):
    # 1. 數值欄位:去逗號 + 轉型
    df = df.withColumn("trade_volume", strip_commas_and_cast("trade_volume", LongType()))
    df = df.withColumn("transaction_count", strip_commas_and_cast("transaction_count", LongType()))
    df = df.withColumn("trade_value", strip_commas_and_cast("trade_value", LongType()))
    df = df.withColumn("open_price", strip_commas_and_cast("open_price", DoubleType()))
    df = df.withColumn("high_price", strip_commas_and_cast("high_price", DoubleType()))
    df = df.withColumn("low_price", strip_commas_and_cast("low_price", DoubleType()))
    df = df.withColumn("close_price", strip_commas_and_cast("close_price", DoubleType()))
    df = df.withColumn("last_bid_price", strip_commas_and_cast("last_bid_price", DoubleType()))
    df = df.withColumn("last_bid_volume", strip_commas_and_cast("last_bid_volume", LongType()))
    df = df.withColumn("last_ask_price", strip_commas_and_cast("last_ask_price", DoubleType()))
    df = df.withColumn("last_ask_volume", strip_commas_and_cast("last_ask_volume", LongType()))
    df = df.withColumn("pe_ratio", strip_commas_and_cast("pe_ratio", DoubleType()))

    # 2. 從 HTML tag 中抽出純漲跌符號
    #    pattern 解釋: <p 開頭(不管後面帶什麼屬性)> 中間文字 </p>,取出中間文字並去除頭尾空白
    df = df.withColumn(
        "change_direction",
        F.trim(F.regexp_extract(F.col("change_symbol_raw"), r"<p[^>]*>(.*?)</p>", 1))
    )

    # 3. change_amount 先去逗號轉型成數字(這裡先不管正負號,只是把字串變數字)
    df = df.withColumn("change_amount", strip_commas_and_cast("change_amount", DoubleType()))

    # 4. 依照 change_direction 組出帶正負號的漲跌數字
    #    X = 不適用比較,存 null(不是 0),避免語意上被誤解為「持平」
    df = df.withColumn(
        "signed_change_amount",
        F.when(F.col("change_direction") == "-", -F.col("change_amount"))
         .when(F.col("change_direction") == "X", F.lit(None).cast(DoubleType()))
         .otherwise(F.col("change_amount"))
    )

    return df


def explore():
    spark = (
        SparkSession.builder
        .appName("stock-pulse-clean-explore")
        .master("local[*]")
        .getOrCreate()
    )

    with open("local_output/twse_daily_2026-07-09.json", "r", encoding="utf-8") as f:
        raw = json.load(f)
    rows = raw["data"]

    df = spark.createDataFrame(rows, schema=TWSE_RAW_SCHEMA)
    cleaned = clean_twse(df)

    print("=== 清洗後 schema ===")
    cleaned.printSchema()

    print("\n=== 驗證關鍵欄位(前 5 筆,涵蓋 +/-/X 三種案例)===")
    cleaned.select(
        "stock_id", "close_price", "change_symbol_raw",
        "change_direction", "change_amount", "signed_change_amount"
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    explore()
