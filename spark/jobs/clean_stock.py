"""
TWSE + TPEx 股市資料清洗與合併邏輯

這裡集中了 3.2 階段驗證過的所有清洗規則:
  - TWSE: 千分位逗號、HTML tag 漲跌符號抽取、X(不比價)處理為 null
  - TPEx: 千分位逗號、尾隨空白、---(無資料)安全轉型為 null
  - 合併: 聯集欄位,unionByName 依名稱對齊,避免位置錯位風險
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType


def add_trade_date(df, trade_date: str):
    """
    幫清洗後的資料加上交易日期欄位,用於後續按日期分區寫出。
    trade_date 格式: 'YYYY-MM-DD'
    """
    return df.withColumn("dt", F.lit(trade_date))


def safe_cast_numeric(col_name: str, target_type):
    cleaned = F.trim(F.regexp_replace(F.col(col_name), ",", ""))
    return F.when(cleaned.rlike(r"^[+-]?\d+\.?\d*$"), cleaned.cast(target_type)).otherwise(F.lit(None).cast(target_type))


def clean_twse(df):
    df = df.withColumn("trade_volume", safe_cast_numeric("trade_volume", LongType()))
    df = df.withColumn("transaction_count", safe_cast_numeric("transaction_count", LongType()))
    df = df.withColumn("trade_value", safe_cast_numeric("trade_value", LongType()))
    df = df.withColumn("open_price", safe_cast_numeric("open_price", DoubleType()))
    df = df.withColumn("high_price", safe_cast_numeric("high_price", DoubleType()))
    df = df.withColumn("low_price", safe_cast_numeric("low_price", DoubleType()))
    df = df.withColumn("close_price", safe_cast_numeric("close_price", DoubleType()))
    df = df.withColumn("last_bid_price", safe_cast_numeric("last_bid_price", DoubleType()))
    df = df.withColumn("last_bid_volume", safe_cast_numeric("last_bid_volume", LongType()))
    df = df.withColumn("last_ask_price", safe_cast_numeric("last_ask_price", DoubleType()))
    df = df.withColumn("last_ask_volume", safe_cast_numeric("last_ask_volume", LongType()))
    df = df.withColumn("pe_ratio", safe_cast_numeric("pe_ratio", DoubleType()))

    df = df.withColumn(
        "change_direction",
        F.trim(F.regexp_extract(F.col("change_symbol_raw"), r"<p[^>]*>(.*?)</p>", 1))
    )
    df = df.withColumn("change_amount", safe_cast_numeric("change_amount", DoubleType()))
    df = df.withColumn(
        "signed_change_amount",
        F.when(F.col("change_direction") == "-", -F.col("change_amount"))
         .when(F.col("change_direction") == "X", F.lit(None).cast(DoubleType()))
         .otherwise(F.col("change_amount"))
    )
    return df


def clean_tpex(df):
    # (把之前在 explore_read_tpex_raw.py 驗證過的內容整個搬過來)
    numeric_cols_long = ["trade_volume", "transaction_count", "trade_value",
                          "last_bid_volume", "last_ask_volume", "issued_shares"]
    numeric_cols_double = ["close_price", "open_price", "high_price", "low_price",
                            "average_price", "last_bid_price", "last_ask_price",
                            "next_day_reference_price", "next_day_limit_up", "next_day_limit_down"]

    for col_name in numeric_cols_long:
        df = df.withColumn(col_name, safe_cast_numeric(col_name, LongType()))
    for col_name in numeric_cols_double:
        df = df.withColumn(col_name, safe_cast_numeric(col_name, DoubleType()))

    df = df.withColumn("change_amount", safe_cast_numeric("change_symbol_raw", DoubleType()))
    return df


def unify_twse(df):
    return df.select(
        F.col("stock_id"), F.col("stock_name"), F.lit("TWSE").alias("market"),
        F.col("open_price"), F.col("high_price"), F.col("low_price"), F.col("close_price"),
        F.lit(None).cast(DoubleType()).alias("average_price"),
        F.col("trade_volume"), F.col("trade_value"), F.col("transaction_count"),
        F.col("signed_change_amount").alias("change_amount"),
        F.col("last_bid_price"), F.col("last_bid_volume"),
        F.col("last_ask_price"), F.col("last_ask_volume"),
        F.col("pe_ratio"),
        F.lit(None).cast(LongType()).alias("issued_shares"),
    )


def unify_tpex(df):
    return df.select(
        F.col("stock_id"), F.col("stock_name"), F.lit("TPEx").alias("market"),
        F.col("open_price"), F.col("high_price"), F.col("low_price"), F.col("close_price"),
        F.col("average_price"),
        F.col("trade_volume"), F.col("trade_value"), F.col("transaction_count"),
        F.col("change_amount"),
        F.col("last_bid_price"), F.col("last_bid_volume"),
        F.col("last_ask_price"), F.col("last_ask_volume"),
        F.lit(None).cast(DoubleType()).alias("pe_ratio"),
        F.col("issued_shares"),
    )


def filter_official_stocks(df, official_stock_ids: list[str]):
    """
    用官方產業分類清單過濾,只保留真正的股票(排除權證/可轉債/特殊 ETF 等)。
    這是我們在探索階段驗證過的關鍵步驟:10,093 筆 TPEx 原始資料中,
    只有約 889 筆是官方認證的真實股票。
    """
    return df.filter(F.col("stock_id").isin(official_stock_ids))


def merge_markets(twse_df, tpex_df, twse_official_ids: list[str], tpex_official_ids: list[str]):
    """
    合併兩個已對齊 schema 的 DataFrame。
    TWSE、TPEx 資料在合併前都先用各自的官方清單過濾,排除 ETF/權證/可轉債等非個股證券。
    """
    unified_twse = unify_twse(twse_df)
    unified_tpex = unify_tpex(tpex_df)

    filtered_twse = filter_official_stocks(unified_twse, twse_official_ids)
    filtered_tpex = filter_official_stocks(unified_tpex, tpex_official_ids)

    return filtered_twse.unionByName(filtered_tpex)