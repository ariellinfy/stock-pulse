"""
產業分類清單清洗邏輯。
獨立成一個模組,供 daily 排程(單日/最新快照)與 backfill(歷史回補流程)共用。

設計原則: 產業分類清單是「現在」的快照,不像股票行情按日期分區保留歷史,
          下游 dim_stock 只使用最新一份。
"""

import sys
import json
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import get_gcs_client, load_industry_list_from_gcs


def clean_industry_list(df, market: str):
    """
    清洗產業分類清單,只保留 dim_stock 需要的關鍵欄位。
    TWSE 用「上市日期」、TPEx 用「上櫃日期」,統一改名成 listing_date。
    """
    listing_date_col = "上市日期" if market == "TWSE" else "上櫃日期"

    cleaned = df.select(
        F.col("公司代號").alias("stock_id"),
        F.col("公司名稱").alias("company_name"),
        F.col("公司簡稱").alias("stock_name"),
        F.col("產業別").alias("industry_code"),
        F.to_date(F.col(listing_date_col), "yyyyMMdd").alias("listing_date"),
        F.lit(market).alias("market"),
    )
    return cleaned


def clean_and_write_industry_list(spark, bucket_name: str):
    """
    完整流程: 讀取 TWSE + TPEx 最新清單、清洗、合併、寫出。
    daily 排程與 backfill 都呼叫這一支函式,行為完全一致。
    """
    twse_records = load_industry_list_from_gcs(bucket_name, "TWSE")
    tpex_records = load_industry_list_from_gcs(bucket_name, "TPEx")

    twse_df = spark.createDataFrame(twse_records)
    tpex_df = spark.createDataFrame(tpex_records)

    twse_cleaned = clean_industry_list(twse_df, "TWSE")
    tpex_cleaned = clean_industry_list(tpex_df, "TPEx")

    combined = twse_cleaned.unionByName(tpex_cleaned)

    print(f"產業分類清單清洗後總筆數: {combined.count()}")

    output_path = f"gs://{bucket_name}/clean/industry_list/"
    combined.write.mode("overwrite").parquet(output_path)

    print(f"✅ 產業分類清單已寫出至 {output_path}")


if __name__ == "__main__":
    from shared.utils import BUCKET_NAME
    from spark.common.spark_session import build_spark_session

    spark = build_spark_session("stock-pulse-clean-industry-list")
    clean_and_write_industry_list(spark, BUCKET_NAME)
    spark.stop()