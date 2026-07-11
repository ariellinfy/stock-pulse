from pyspark.sql import SparkSession
import sys
import json
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

sys.path.append(str(Path(__file__).resolve().parent.parent))
from spark.common.schemas import TPEX_RAW_SCHEMA, TPEX_EXPECTED_RAW_FIELDS, assert_fields_match

def safe_cast_numeric(col_name: str, target_type):
    """
    去除千分位逗號跟頭尾空白,轉型成數字。
    遇到無法解析的值(例如 '---',代表當日無此價格資料的已知情況)一律轉成 null,
    不讓程式崩潰,因為我們已經確認過這是資料裡真實存在、有規律的正常現象。
    """
    cleaned = F.trim(F.regexp_replace(F.col(col_name), ",", ""))
    return F.when(cleaned.rlike(r"^[+-]?\d+\.?\d*$"), cleaned.cast(target_type)).otherwise(F.lit(None).cast(target_type))

def clean_tpex(df):
    # 統一先 trim 所有字串欄位,防禦潛在的尾隨空白問題(今天已確認漲跌欄位存在此問題)
    numeric_cols_long = ["trade_volume", "transaction_count", "trade_value",
                          "last_bid_volume", "last_ask_volume", "issued_shares"]
    numeric_cols_double = ["close_price", "open_price", "high_price", "low_price",
                            "average_price", "last_bid_price", "last_ask_price",
                            "next_day_reference_price", "next_day_limit_up", "next_day_limit_down"]

    # 在轉型之前,先掃描所有預定轉數字的欄位,找出「無法被解析成標準數字」的異常值
    # numeric_cols_all = numeric_cols_long + numeric_cols_double

    # print("=== 各數值欄位的非標準格式異常值 ===")
    # for col_name in numeric_cols_all:
    #     bad_values = df.select(col_name).distinct() \
    #         .filter(~F.trim(F.regexp_replace(F.col(col_name), ",", "")).rlike(r"^[+-]?\d+\.?\d*$")) \
    #         .filter(F.col(col_name).isNotNull())
    #     count = bad_values.count()
    #     if count > 0:
    #         print(f"\n【{col_name}】異常值筆數: {count}")
    #         bad_values.show(10, truncate=False)

    # print("=== 找出 close_price 為 '---' 的完整那一列 ===")
    # df.filter(F.trim(F.col("close_price")) == "---").show(truncate=False)

    for col_name in numeric_cols_long:
        df = df.withColumn(col_name, safe_cast_numeric(col_name, LongType()))
    for col_name in numeric_cols_double:
        df = df.withColumn(col_name, safe_cast_numeric(col_name, DoubleType()))

    df = df.withColumn("change_amount", safe_cast_numeric("change_symbol_raw", DoubleType()))

    return df


def explore():
    spark = (
        SparkSession.builder
        .appName("stock-pulse-tpex-schema-test")
        .master("local[*]")
        .getOrCreate()
    )

    # 如果本機沒有現成的 TPEx 樣本檔案,先執行 python scrapers/tpex_client.py
    # 讓它產生 GCS 上的資料後,你也可以額外加一行本機存檔邏輯,
    # 或直接從 GCS 下載一份下來測試 —— 這裡先假設你有一份本機 JSON
    with open("local_output/tpex_daily_2026-07-09.json", "r", encoding="utf-8") as f:
        raw = json.load(f)

    assert_fields_match(raw["fields"], TPEX_EXPECTED_RAW_FIELDS, "TPEx")  # 先驗證,不通過就直接中斷

    rows = raw["data"]
    df = spark.createDataFrame(rows, schema=TPEX_RAW_SCHEMA)

    # print("\n=== change_symbol_raw 的相異值樣式(篩選非典型格式)===")
    # df.select("change_symbol_raw").distinct() \
    # .filter(~F.col("change_symbol_raw").rlike(r"^[+-]?\d+\.?\d*$")) \
    # .show(30, truncate=False)

    # sample = df.select("change_symbol_raw").filter(~F.col("change_symbol_raw").rlike(r"^[+-]?\d+\.?\d*$")).first()[0]
    # print(f"原始字串: {repr(sample)}")
    # for ch in sample:
    #     print(f"字元: {repr(ch)}, Unicode 編碼: U+{ord(ch):04X}")

    # # 檢查其他數值欄位是不是也有同樣的尾隨空白問題
    # for col_name in ["close_price", "open_price", "trade_volume"]:
    #     sample_with_space = df.filter(F.col(col_name).rlike(r"\s$")).select(col_name).limit(3)
    #     count = df.filter(F.col(col_name).rlike(r"\s$")).count()
    #     print(f"{col_name}: 有尾隨空白的筆數 = {count}")
    #     sample_with_space.show(truncate=False)

    print(f"✅ 成功讀入 {df.count()} 筆資料")
    # df.select("stock_id", "stock_name", "close_price", "change_symbol_raw").show(10, truncate=False)

    # cleaned = clean_tpex(df)
    # cleaned.select("stock_id", "close_price", "change_symbol_raw", "change_amount").show(10, truncate=False)

    # # 順便確認一下:剛剛那批「尾隨空白」的異常值,清洗後應該能正常轉型,不會變成 null
    # cleaned.filter(F.col("change_amount").isNull() & F.col("change_symbol_raw").isNotNull()).count()


    cleaned = clean_tpex(df)

    # # 驗證:剛剛那 20 筆 '---' 案例,清洗後應該變成 null,而不是報錯或變成 0
    # cleaned.filter(F.col("stock_id") == "00986B").select(
    #     "stock_id", "close_price", "open_price", "average_price", "trade_volume"
    # ).show(truncate=False)

    # # 統計全部有多少筆落入這個「開高低收為 null 但均價有值」的情況
    # null_ohlc_count = cleaned.filter(
    #     F.col("close_price").isNull() & F.col("average_price").isNotNull()
    # ).count()
    # print(f"開高低收為 null、但均價有值的筆數: {null_ohlc_count}")

    # # 檢查「開高低收為 null」這群 stock_id 的長度分布跟樣式
    # null_ohlc_ids = cleaned.filter(
    #     F.col("close_price").isNull() & F.col("average_price").isNotNull()
    # ).select("stock_id", "stock_name")

    # print("=== stock_id 長度分布(null OHLC 這群)===")
    # null_ohlc_ids.withColumn("id_length", F.length("stock_id")) \
    #     .groupBy("id_length").count().orderBy("id_length").show()

    # print("\n=== 範例名稱(前 15 筆)===")
    # null_ohlc_ids.show(15, truncate=False)

    # # 對照組:開高低收「有值」的這群,長度分布長怎樣
    # print("\n=== 對照: 開高低收有值的這群,stock_id 長度分布 ===")
    # cleaned.filter(F.col("close_price").isNotNull()) \
    #     .withColumn("id_length", F.length("stock_id")) \
    #     .groupBy("id_length").count().orderBy("id_length").show()

    # 讀取我們之前存的 TPEx 產業分類清單(891 檔權威股票名冊)
    with open("local_output/industry_list_tpex.json", "r", encoding="utf-8") as f:
        industry_records = json.load(f)

    official_stock_ids = [r["公司代號"] for r in industry_records]
    print(f"官方股票清單筆數: {len(official_stock_ids)}")

    # 用官方清單過濾,只留下「真正的股票」
    filtered = cleaned.filter(F.col("stock_id").isin(official_stock_ids))
    print(f"過濾後剩餘筆數: {filtered.count()}")

    # 過濾後,還有多少筆是「開高低收為 null」?
    still_null_ohlc = filtered.filter(
        F.col("close_price").isNull() & F.col("average_price").isNotNull()
    ).count()
    print(f"過濾後,開高低收仍為 null 的筆數: {still_null_ohlc}")

    if still_null_ohlc > 0:
        print("\n=== 過濾後仍有 null OHLC 的股票範例 ===")
        filtered.filter(
            F.col("close_price").isNull() & F.col("average_price").isNotNull()
        ).select("stock_id", "stock_name", "trade_volume", "average_price").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    explore()