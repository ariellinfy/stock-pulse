from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from spark.common.schemas import TWSE_RAW_SCHEMA


def explore():
    spark = (
        SparkSession.builder
        .appName("stock-pulse-schema-test")
        .master("local[*]")
        .getOrCreate()
    )

    # 我們存的原始 JSON 結構是 {"fields": [...], "data": [[...], ...], ...}
    # 先讀成單一欄位的 JSON,再手動把 data 陣列展開成符合 schema 的 rows
    import json
    with open("local_output/twse_daily_2026-07-09.json", "r", encoding="utf-8") as f:
        raw = json.load(f)

    rows = raw["data"]  # list of list,每個內層 list 是一支股票的 16 個欄位值

    df = spark.createDataFrame(rows, schema=TWSE_RAW_SCHEMA)

    print(f"✅ 成功讀入 {df.count()} 筆資料,套用明確 schema")
    df.printSchema()
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    explore()