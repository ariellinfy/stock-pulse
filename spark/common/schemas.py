"""
TWSE/TPEx 原始資料的明確 schema 定義。

設計原則:
  - Raw Layer 存的是「未清洗」的原始字串(含千分位逗號、HTML tag),
    所以這裡的 schema 全部先用 StringType 讀入,轉型/清洗留給後續步驟,
    不要在讀取這一步就嘗試自動轉數字型態(容易因為逗號、HTML tag 而失敗或出錯)。
"""

from pyspark.sql.types import StructType, StructField, DoubleType, StringType


def assert_fields_match(
    actual_fields: list[str], expected_fields: list[str], source_name: str
):
    """
    在建立 DataFrame 前,強制驗證原始資料的欄位順序跟我們寫死的 schema 假設一致。
    不一致就直接中斷,不讓錯誤悄悄流入下游。
    """
    if actual_fields != expected_fields:
        raise ValueError(
            f"❌ {source_name} 欄位順序與預期不符!\n"
            f"預期: {expected_fields}\n"
            f"實際: {actual_fields}\n"
            f"這代表 schema 定義可能已經過期,需要重新核對並更新 TPEX_RAW_SCHEMA"
        )
    print(f"✅ {source_name} 欄位順序驗證通過,共 {len(actual_fields)} 欄")


# 對應我們在 twse_client.py 裡驗證過的 16 個欄位,順序一致
TWSE_RAW_SCHEMA = StructType(
    [
        StructField("stock_id", StringType(), nullable=False),
        StructField("stock_name", StringType(), nullable=True),
        StructField("trade_volume", StringType(), nullable=True),
        StructField("transaction_count", StringType(), nullable=True),
        StructField("trade_value", StringType(), nullable=True),
        StructField("open_price", StringType(), nullable=True),
        StructField("high_price", StringType(), nullable=True),
        StructField("low_price", StringType(), nullable=True),
        StructField("close_price", StringType(), nullable=True),
        # 含 HTML tag,例如 <p style='color:red'>+</p>
        StructField("change_symbol_raw", StringType(), nullable=True),
        StructField("change_amount", StringType(), nullable=True),
        StructField("last_bid_price", StringType(), nullable=True),
        StructField("last_bid_volume", StringType(), nullable=True),
        StructField("last_ask_price", StringType(), nullable=True),
        StructField("last_ask_volume", StringType(), nullable=True),
        StructField("pe_ratio", StringType(), nullable=True),
    ]
)


# 對應我們在 tpex_client.py 裡驗證過的 19 個欄位,順序一致
# 注意: 欄位順序與 TWSE 不同(收盤/漲跌在前,開高低在後),命名時刻意保留語意對應,
#       方便之後合併時能清楚知道哪個欄位對應到 TWSE 的哪個欄位
TPEX_RAW_SCHEMA = StructType(
    [
        StructField("stock_id", StringType(), nullable=False),
        StructField("stock_name", StringType(), nullable=True),
        StructField("close_price", StringType(), nullable=True),
        # TPEx 這裡不含 HTML tag,格式跟 TWSE 不同,要驗證
        StructField("change_symbol_raw", StringType(), nullable=True),
        StructField("open_price", StringType(), nullable=True),
        StructField("high_price", StringType(), nullable=True),
        StructField("low_price", StringType(), nullable=True),
        StructField("average_price", StringType(), nullable=True),  # TWSE 沒有這個欄位
        StructField("trade_volume", StringType(), nullable=True),
        StructField("trade_value", StringType(), nullable=True),
        StructField("transaction_count", StringType(), nullable=True),
        StructField("last_bid_price", StringType(), nullable=True),
        StructField("last_bid_volume", StringType(), nullable=True),
        StructField("last_ask_price", StringType(), nullable=True),
        StructField("last_ask_volume", StringType(), nullable=True),
        StructField("issued_shares", StringType(), nullable=True),  # TWSE 沒有
        StructField(
            "next_day_reference_price", StringType(), nullable=True
        ),  # TWSE 沒有
        StructField("next_day_limit_up", StringType(), nullable=True),  # TWSE 沒有
        StructField("next_day_limit_down", StringType(), nullable=True),  # TWSE 沒有
    ]
)


FEAR_GREED_SCHEMA = StructType(
    [
        StructField("x", DoubleType(), nullable=False),  # Unix 毫秒時間戳(浮點數)
        StructField("y", DoubleType(), nullable=True),  # 指數分數
        # 文字評級(fear/neutral/greed 等)
        StructField("rating", StringType(), nullable=True),
    ]
)
