"""
[ARCHIVED] TWSE / TPEx / Yahoo 三個來源清洗、統一 schema 之後,
驗證欄位是否真能對齊(名稱 + 型態),並實際執行 unionByName 三方合併。

狀態: verify_schema_compatibility() 是很實用的除錯工具,可考慮保留為共用 debug 工具;
merge_markets() 的合併邏輯已併入 spark/jobs/clean_stock.py。
若要移動此檔案的位置,請確認下方 sys.path.append 的 .parent 層數
與新的資料夾深度一致(目前假設: 專案根目錄/_exploration_archived/merge/)。
"""

import sys
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from spark.common.schemas import TWSE_RAW_SCHEMA, TPEX_RAW_SCHEMA
from spark.jobs.clean_stock import (
    clean_twse,
    clean_tpex,
    clean_yahoo_history,
    unify_twse,
    unify_tpex,
    unify_yahoo_tpex,
    merge_markets,
    filter_official_stocks,
    add_trade_date,
)


def verify_schema_compatibility(twse_unified, tpex_unified, yahoo_unified):
    """
    驗證三個來源統一後的 schema 是否真的能合併。
    比對欄位名稱集合是否一致,型態是否相容,不依賴「應該沒問題」的假設。
    """
    twse_schema = {
        f.name: f.dataType.simpleString() for f in twse_unified.schema.fields
    }
    tpex_schema = {
        f.name: f.dataType.simpleString() for f in tpex_unified.schema.fields
    }
    yahoo_schema = {
        f.name: f.dataType.simpleString() for f in yahoo_unified.schema.fields
    }

    print("=== 欄位名稱是否一致 ===")
    print(
        f"TWSE 欄位數: {len(twse_schema)}, TPEx 欄位數: {len(tpex_schema)}, Yahoo 欄位數: {len(yahoo_schema)}"
    )
    print(f"TWSE - TPEx 欄位差異: {set(twse_schema) ^ set(tpex_schema)}")
    print(f"TWSE - Yahoo 欄位差異: {set(twse_schema) ^ set(yahoo_schema)}")

    print("\n=== 同名欄位,型態是否一致 ===")
    common_fields = set(twse_schema) & set(tpex_schema) & set(yahoo_schema)
    for field in sorted(common_fields):
        types = {twse_schema[field], tpex_schema[field], yahoo_schema[field]}
        if len(types) > 1:
            print(
                f"⚠️ 欄位 '{field}' 型態不一致: TWSE={twse_schema[field]}, TPEx={tpex_schema[field]}, Yahoo={yahoo_schema[field]}"
            )

    print("\n✅ 檢查完成,若上方沒有 ⚠️ 且欄位差異為空集合,代表可以安全合併")


def explore():
    spark = (
        SparkSession.builder.appName("stock-pulse-yahoo-local-test")
        .master("local[*]")
        .getOrCreate()
    )

    # twse
    with open("local_output/twse_daily_2026-07-09.json", "r", encoding="utf-8") as f:
        twse_raw = json.load(f)

    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)
    twse_official_ids = [r["公司代號"] for r in twse_industry_records]

    twse_df = spark.createDataFrame(twse_raw["data"], schema=TWSE_RAW_SCHEMA)

    # print("=== TWSE 原始資料(清洗前)裡,含有 '--' 的欄位掃描 ===")
    # numeric_cols_twse = ["trade_volume", "transaction_count", "trade_value", "open_price",
    #                     "high_price", "low_price", "close_price", "change_amount",
    #                     "last_bid_price", "last_bid_volume", "last_ask_price",
    #                     "last_ask_volume", "pe_ratio"]

    # for col_name in numeric_cols_twse:
    #     bad_count = twse_df.filter(F.trim(F.col(col_name)) == "--").count()
    #     if bad_count > 0:
    #         print(f"{col_name}: {bad_count} 筆是 '--'")

    cleaned_twse = clean_twse(twse_df)
    cleaned_twse = add_trade_date(cleaned_twse, "2026-07-09")
    unified_twse = unify_twse(cleaned_twse)
    unified_twse = filter_official_stocks(unified_twse, twse_official_ids)

    # print("=== TWSE stock_id 長度分布 ===")
    # cleaned_twse.withColumn("id_length", F.length("stock_id")) \
    #     .groupBy("id_length").count().orderBy("id_length").show()

    # print("\n=== 讀取 TWSE 官方股票清單,比對實際有多少符合 ===")
    # with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
    #     twse_industry_records = json.load(f)
    # twse_official_ids = [r["公司代號"] for r in twse_industry_records]
    # print(f"TWSE 官方清單筆數: {len(twse_official_ids)}")

    # matched = cleaned_twse.filter(F.col("stock_id").isin(twse_official_ids)).count()
    # unmatched = cleaned_twse.filter(~F.col("stock_id").isin(twse_official_ids)).count()
    # print(f"符合官方清單: {matched} 筆")
    # print(f"不在官方清單: {unmatched} 筆")

    # print("\n=== 不在官方清單的範例(前 15 筆)===")
    # cleaned_twse.filter(~F.col("stock_id").isin(twse_official_ids)) \
    #     .select("stock_id", "stock_name").show(15, truncate=False)

    # tpex
    with open("local_output/tpex_daily_2026-07-09.json", "r", encoding="utf-8") as f:
        tpex_raw = json.load(f)

    with open("local_output/industry_list_tpex.json", "r", encoding="utf-8") as f:
        tpex_industry_records = json.load(f)
    tpex_official_ids = [r["公司代號"] for r in tpex_industry_records]

    tpex_df = spark.createDataFrame(tpex_raw["data"], schema=TPEX_RAW_SCHEMA)

    cleaned_tpex = clean_tpex(tpex_df)
    cleaned_tpex = add_trade_date(cleaned_tpex, "2026-07-09")
    unified_tpex = unify_tpex(cleaned_tpex)
    unified_tpex = filter_official_stocks(unified_tpex, tpex_official_ids)

    # yahoo
    yahoo_df = spark.read.option("multiline", "true").json(
        "local_output/yahoo_sample/stock_id_6026.json"
    )

    cleaned_yahoo = clean_yahoo_history(yahoo_df)
    unified_yahoo = unify_yahoo_tpex(cleaned_yahoo)

    print("=== 分別檢查各來源筆數(合併前)===")
    print(f"twse_unified 筆數: {unified_twse.count()}")
    print(f"tpex_unified 筆數(過濾前): {unified_tpex.count()}")
    print(f"yahoo_unified 筆數: {unified_yahoo.count()}")

    verify_schema_compatibility(unified_twse, unified_tpex, unified_yahoo)

    # 實際嘗試三方合併,讓 Spark 用真實執行結果證明是否可行
    try:
        combined = unified_twse.unionByName(unified_tpex).unionByName(unified_yahoo)
        print(f"\n✅ 三方合併成功,總筆數: {combined.count()}")
        combined.groupBy("market").count().show()
    except Exception as e:
        print(f"\n❌ 合併失敗: {e}")

    spark.stop()


if __name__ == "__main__":
    explore()
