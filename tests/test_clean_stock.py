from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType,
)

from spark.common.schemas import TWSE_RAW_SCHEMA
from spark.jobs.clean_stock import (
    safe_cast_numeric,
    add_trade_date,
    clean_twse,
    unify_twse,
    clean_tpex,
    filter_official_stocks,
    clean_yahoo_history,
    clean_fear_greed_history,
    merge_markets,
    explode_and_flatten,
)


def _spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_clean_stock")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def test_safe_cast_numeric_strips_commas_and_nulls_placeholders():
    spark = _spark()
    schema = StructType([StructField("v", StringType(), True)])
    df = spark.createDataFrame(
        [("1,234",), ("56.7",), ("-12.5",), ("---",), ("",), (None,)], schema=schema
    )
    values = [
        r["v"]
        for r in df.select(safe_cast_numeric("v", DoubleType()).alias("v")).collect()
    ]
    assert values == [1234.0, 56.7, -12.5, None, None, None]


def test_explode_and_flatten_maps_array_positions_to_named_columns():
    """
    這是 clean_stock_daily.py(單一檔案,手動加 dt)與 backfill_clean_stock.py
    (整個資料夾 glob,dt 由 Hive-style 分區資料夾自動推斷)共用的攤平邏輯:
    釘住「陣列位置 → schema 具名欄位」的對應關係,以及 fields 欄位不該流入輸出。
    """
    spark = _spark()
    schema = StructType(
        [StructField("a", StringType(), True), StructField("b", StringType(), True)]
    )
    raw_schema = StructType(
        [
            StructField("dt", StringType(), True),
            StructField("fields", StringType(), True),  # 不該出現在輸出裡
            StructField("data", ArrayType(ArrayType(StringType())), True),
        ]
    )
    df = spark.createDataFrame(
        [("2026-07-09", "irrelevant", [["v1", "v2"], ["v3", "v4"]])],
        schema=raw_schema,
    )

    result = explode_and_flatten(df, schema)

    assert set(result.columns) == {"dt", "a", "b"}
    rows = sorted((r["dt"], r["a"], r["b"]) for r in result.collect())
    assert rows == [("2026-07-09", "v1", "v2"), ("2026-07-09", "v3", "v4")]


def test_explode_and_flatten_excludes_rows_with_null_data():
    """模擬 backfill 讀整個資料夾時混進 _no_data_marker.json(沒有 data 欄位)的情況。"""
    spark = _spark()
    schema = StructType([StructField("a", StringType(), True)])
    raw_schema = StructType(
        [
            StructField("dt", StringType(), True),
            StructField("data", ArrayType(ArrayType(StringType())), True),
        ]
    )
    df = spark.createDataFrame(
        [
            ("2026-07-09", [["v1"]]),
            ("2026-07-10", None),  # 標記檔,沒有 data
        ],
        schema=raw_schema,
    )

    result = explode_and_flatten(df, schema)
    assert [(r["dt"], r["a"]) for r in result.collect()] == [("2026-07-09", "v1")]


def test_clean_twse_extracts_change_direction_and_signs_change_amount():
    """
    釘住全 repo 最容易悄悄壞掉的一段邏輯:HTML tag 漲跌符號抽取
    + 依方向決定正負號 + X(不比價)轉 null。
    """
    spark = _spark()
    rows = [
        # 上漲(紅色 +):signed_change_amount 應為正
        (
            "2330",
            "台積電",
            "1,234",
            "10",
            "1,000,000",
            "590.0",
            "595.0",
            "588.0",
            "592.0",
            "<p style='color:red'>+</p>",
            "2.5",
            "591",
            "10",
            "593",
            "5",
            "20.5",
        ),
        # 下跌(綠色 -):signed_change_amount 應為負
        (
            "2317",
            "鴻海",
            "500",
            "5",
            "50,000",
            "100",
            "105",
            "99",
            "101",
            "<p style='color:green'>-</p>",
            "1.0",
            "100.5",
            "3",
            "101.5",
            "2",
            "15.0",
        ),
        # 不比價(X):change_amount 應為 null;close_price 是 "---" 也應轉 null
        (
            "9999",
            "全額交割股",
            "0",
            "0",
            "0",
            "---",
            "---",
            "---",
            "---",
            "<p>X</p>",
            "0",
            "---",
            "0",
            "---",
            "0",
            "---",
        ),
    ]
    df = add_trade_date(
        spark.createDataFrame(rows, schema=TWSE_RAW_SCHEMA), "2026-07-09"
    )
    cleaned = clean_twse(df)

    result = {
        r["stock_id"]: r
        for r in cleaned.select(
            "stock_id",
            "change_direction",
            "signed_change_amount",
            "trade_volume",
            "close_price",
        ).collect()
    }

    assert result["2330"]["change_direction"] == "+"
    assert result["2330"]["signed_change_amount"] == 2.5
    assert result["2330"]["trade_volume"] == 1234  # 千分位逗號要被拿掉
    assert result["2330"]["close_price"] == 592.0

    assert result["2317"]["change_direction"] == "-"
    assert result["2317"]["signed_change_amount"] == -1.0

    assert result["9999"]["change_direction"] == "X"
    assert result["9999"]["signed_change_amount"] is None
    assert result["9999"]["close_price"] is None


def test_unify_twse_sets_market_literal_and_nulls_tpex_only_columns():
    spark = _spark()
    rows = [
        (
            "2330",
            "台積電",
            "1,234",
            "10",
            "1,000,000",
            "590.0",
            "595.0",
            "588.0",
            "592.0",
            "<p>+</p>",
            "2.5",
            "591",
            "10",
            "593",
            "5",
            "20.5",
        ),
    ]
    df = add_trade_date(
        spark.createDataFrame(rows, schema=TWSE_RAW_SCHEMA), "2026-07-09"
    )
    unified = unify_twse(clean_twse(df))
    row = unified.collect()[0]

    assert row["market"] == "TWSE"
    assert row["average_price"] is None  # TWSE 沒有這個欄位
    assert row["issued_shares"] is None  # 同上


def test_filter_official_stocks_excludes_unlisted_ids():
    spark = _spark()
    df = spark.createDataFrame([("1101",), ("9999",)], ["stock_id"])
    filtered = filter_official_stocks(df, ["1101"])
    assert [r["stock_id"] for r in filtered.collect()] == ["1101"]


def test_clean_yahoo_history_converts_nan_to_null_and_rounds():
    spark = _spark()
    schema = StructType(
        [
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(590.126, 595.999, float("nan"), 592.004)], schema=schema
    )
    row = clean_yahoo_history(df).collect()[0]

    assert row["open"] == 590.13
    assert row["high"] == 596.0
    assert row["low"] is None  # NaN 必須明確轉成 null,不能污染下游比較
    assert row["close"] == 592.0


def test_clean_fear_greed_history_dedups_same_day_and_renames_columns():
    spark = _spark()
    schema = StructType(
        [
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("rating", StringType(), True),
        ]
    )
    ts1 = datetime(2026, 7, 9, 3, 0, tzinfo=timezone.utc).timestamp() * 1000
    ts2 = datetime(2026, 7, 9, 15, 0, tzinfo=timezone.utc).timestamp() * 1000
    df = spark.createDataFrame(
        [(ts1, 55.555, "greed"), (ts2, 60.0, "greed")], schema=schema
    )

    rows = clean_fear_greed_history(df).collect()

    assert len(rows) == 1  # 同一天兩筆(歷史數列尾端跟即時值重疊)要去重成一筆
    assert rows[0]["dt"] == "2026-07-09"
    assert rows[0]["fear_greed_rating"] == "greed"


def test_merge_markets_unions_after_filtering_official_stocks():
    spark = _spark()
    twse_rows = [
        (
            "2330",
            "台積電",
            "1,234",
            "10",
            "1,000,000",
            "590.0",
            "595.0",
            "588.0",
            "592.0",
            "<p>+</p>",
            "2.5",
            "591",
            "10",
            "593",
            "5",
            "20.5",
        ),
        (
            "8888",
            "非官方清單股票",
            "1",
            "1",
            "1",
            "1",
            "1",
            "1",
            "1",
            "<p>+</p>",
            "0",
            "1",
            "1",
            "1",
            "1",
            "1",
        ),
    ]
    twse_df = add_trade_date(
        spark.createDataFrame(twse_rows, schema=TWSE_RAW_SCHEMA), "2026-07-09"
    )

    tpex_schema = StructType(
        [
            StructField("stock_id", StringType(), True),
            StructField("stock_name", StringType(), True),
            StructField("close_price", StringType(), True),
            StructField("change_symbol_raw", StringType(), True),
            StructField("open_price", StringType(), True),
            StructField("high_price", StringType(), True),
            StructField("low_price", StringType(), True),
            StructField("average_price", StringType(), True),
            StructField("trade_volume", StringType(), True),
            StructField("trade_value", StringType(), True),
            StructField("transaction_count", StringType(), True),
            StructField("last_bid_price", StringType(), True),
            StructField("last_bid_volume", StringType(), True),
            StructField("last_ask_price", StringType(), True),
            StructField("last_ask_volume", StringType(), True),
            StructField("issued_shares", StringType(), True),
            StructField("next_day_reference_price", StringType(), True),
            StructField("next_day_limit_up", StringType(), True),
            StructField("next_day_limit_down", StringType(), True),
        ]
    )
    tpex_rows = [
        (
            "6488",
            "環球晶",
            "690",
            "1.0",
            "685",
            "692",
            "680",
            "688",
            "100",
            "68900",
            "5",
            "689",
            "10",
            "691",
            "8",
            "50000",
            "700",
            "760",
            "620",
        )
    ]
    tpex_df = add_trade_date(
        spark.createDataFrame(tpex_rows, schema=tpex_schema), "2026-07-09"
    )

    twse_cleaned = clean_twse(twse_df)
    tpex_cleaned = clean_tpex(tpex_df)

    combined = merge_markets(
        twse_cleaned,
        tpex_cleaned,
        twse_official_ids=["2330"],
        tpex_official_ids=["6488"],
    )
    rows = {r["stock_id"]: r for r in combined.collect()}

    assert set(rows.keys()) == {"2330", "6488"}  # 8888 不在官方清單,應被過濾掉
    assert rows["2330"]["market"] == "TWSE"
    assert rows["6488"]["market"] == "TPEx"
    assert rows["6488"]["average_price"] == 688.0  # TPEx 有真實均價,不像 TWSE 補 null


if __name__ == "__main__":
    test_safe_cast_numeric_strips_commas_and_nulls_placeholders()
    test_explode_and_flatten_maps_array_positions_to_named_columns()
    test_explode_and_flatten_excludes_rows_with_null_data()
    test_clean_twse_extracts_change_direction_and_signs_change_amount()
    test_unify_twse_sets_market_literal_and_nulls_tpex_only_columns()
    test_filter_official_stocks_excludes_unlisted_ids()
    test_clean_yahoo_history_converts_nan_to_null_and_rounds()
    test_clean_fear_greed_history_dedups_same_day_and_renames_columns()
    test_merge_markets_unions_after_filtering_official_stocks()
    print("✅ 全部測試通過")
