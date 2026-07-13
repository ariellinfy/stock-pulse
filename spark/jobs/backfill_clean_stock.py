"""
TWSE + Yahoo TPEx 歷史資料批次清洗 job。
讀取 raw/twse_daily/ 底下全部日期分區,套用既有清洗邏輯,
讀取 raw/yahoo_tpex_history/ 底下全部股票分區,套用既有清洗轉換邏輯,
依日期動態分區覆寫寫出至 clean/stock_daily/。
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from spark.common.schemas import TWSE_RAW_SCHEMA
from spark.jobs.clean_stock import clean_twse, clean_yahoo_history, unify_twse, unify_yahoo_tpex, filter_official_stocks


def build_spark_session():
    """共用的 SparkSession 建立邏輯,含 GCS Connector 設定。"""
    return (
        SparkSession.builder
        .appName("stock-pulse-backfill-clean-twse")
        .master("local[*]")
        .config("spark.jars", "/home/fy/spark_jars/gcs-connector-hadoop3-latest.jar")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/fy/stock-pulse/secrets/gcp-sa-key.json")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .getOrCreate()
    )


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
    

def backfill_all_markets(spark, bucket_name: str, twse_official_ids: list[str]):
    # TWSE 歷史
    twse_raw = spark.read.option("multiline", "true").json(f"gs://{bucket_name}/raw/twse_daily/")
    twse_exploded = explode_daily_data(twse_raw)
    twse_flattened = flatten_to_columns(twse_exploded)
    twse_cleaned = clean_twse(twse_flattened)
    twse_unified = unify_twse(twse_cleaned)
    twse_filtered = filter_official_stocks(twse_unified, twse_official_ids)
    twse_filtered = twse_filtered.withColumn("dt", F.col("dt").cast("string"))

    # Yahoo TPEx 歷史(TPEx 唯一的歷史資料來源,官方每日端點無法查歷史)
    yahoo_raw = spark.read.option("multiline", "true").json(f"gs://{bucket_name}/raw/yahoo_tpex_history/")
    yahoo_cleaned = clean_yahoo_history(yahoo_raw)
    yahoo_unified = unify_yahoo_tpex(yahoo_cleaned)

    # 關鍵: 先合併,再一次寫出,避免動態分區覆寫互相沖掉彼此的資料
    combined = twse_filtered.unionByName(yahoo_unified)

    print(f"合併後總筆數: {combined.count()}")
    combined.groupBy("market").count().show()

    output_path = f"gs://{bucket_name}/clean/stock_daily/"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        combined
        .repartition(F.col("dt"))
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    print(f"✅ 全市場歷史已寫出至 {output_path}")


def detect_twse_gaps(spark, bucket_name: str, twse_official_ids: list[str]):
    """
    偵測 TWSE 歷史資料裡,是否有「某支股票在某個交易日缺席」的情況。
    做法: 找出資料集裡實際出現過的所有交易日,理論上每個交易日
          都應該要有全部 1088 檔股票的紀錄,實際筆數若不足,代表當天有缺漏。
    """
    df = spark.read.parquet(f"gs://{bucket_name}/clean/stock_daily/").filter(F.col("market") == "TWSE")

    total_stocks = len(twse_official_ids)

    print("=== 每個交易日,實際出現的股票檔數 vs 應有檔數 ===")
    daily_counts = df.groupBy("dt").agg(F.countDistinct("stock_id").alias("actual_count"))
    daily_counts = daily_counts.withColumn("expected_count", F.lit(total_stocks))
    daily_counts = daily_counts.withColumn("gap", F.col("expected_count") - F.col("actual_count"))

    # 只列出有缺漏的日子(gap > 0),不要把 489 天全部印出來
    gaps = daily_counts.filter(F.col("gap") > 0).orderBy(F.desc("gap"))
    gap_count = gaps.count()

    print(f"總交易日數: {daily_counts.count()}")
    print(f"有缺漏的交易日數: {gap_count}")

    if gap_count > 0:
        print("\n=== 缺漏最嚴重的前 20 天 ===")
        gaps.show(20, truncate=False)
    else:
        print("✅ 沒有發現任何缺漏,500 天歷史資料完整")


def detect_twse_gaps_v2(spark, bucket_name: str, twse_industry_records: list[dict]):
    """
    改良版缺漏偵測: 用每檔股票實際的上市日期,計算「每一天真正應該有幾檔股票」,
    而不是死板套用現在的官方清單總數,避免把「這天這支股票還沒上市」誤判為缺漏。
    """
    # 從產業清單建立 (股票代號 -> 上市日期) 對照,轉成 date 型態方便比較
    # listing_dates = spark.createDataFrame(
    #     [(r["公司代號"], r["上市日期"]) for r in twse_industry_records],
    #     ["stock_id", "listing_date_raw"]
    # )
    # # 上市日期原始格式是 '19620209' 這種 8 碼字串,轉成標準日期格式
    # listing_dates = listing_dates.withColumn(
    #     "listing_date",
    #     F.to_date(F.col("listing_date_raw"), "yyyyMMdd")
    # )

    # twse_official_ids = [r["公司代號"] for r in twse_industry_records]

    # df = spark.read.parquet(f"gs://{bucket_name}/clean/stock_daily/").filter(F.col("market") == "TWSE")

    
    # print("=== 2025-12-17 這天,實際有出現 vs 沒出現的股票對照 ===")
    # df_1217 = df.filter(F.col("dt") == "2025-12-17").select("stock_id").distinct()
    # present_ids_1217 = set(row["stock_id"] for row in df_1217.collect())

    # all_official_ids = set(twse_official_ids)
    # missing_1217 = all_official_ids - present_ids_1217
    # print(f"缺席股票數: {len(missing_1217)}")
    # print(f"缺席股票範例(前 15 檔): {list(missing_1217)[:15]}")

    # print("\n=== 2024-07-11 同樣邏輯檢查 ===")
    # df_0711 = df.filter(F.col("dt") == "2024-07-11").select("stock_id").distinct()
    # present_ids_0711 = set(row["stock_id"] for row in df_0711.collect())
    # missing_0711 = all_official_ids - present_ids_0711
    # print(f"缺席股票數: {len(missing_0711)}")
    # print(f"缺席股票範例(前 15 檔): {list(missing_0711)[:15]}")


    # print("=== 這 221 檔股票,在整個 489 天資料集裡,出現過幾次? ===")
    # missing_ids_list = list(missing_1217)  # 用剛剛算出的 221 檔清單

    # check_df = df.filter(F.col("stock_id").isin(missing_ids_list))
    # appearance_count = check_df.groupBy("stock_id").count()

    # print(f"這 221 檔裡,完全沒出現過的: {221 - appearance_count.count()} 檔")
    # print(f"至少出現過一次的: {appearance_count.count()} 檔")

    # if appearance_count.count() > 0:
    #     print("\n=== 有出現過的,分布狀況 ===")
    #     appearance_count.orderBy("count").show(10)

    # print("=== 這 221 檔股票的代號長度分布 ===")
    # missing_details = spark.createDataFrame([(sid,) for sid in missing_ids_list], ["stock_id"])
    # missing_details.withColumn("id_length", F.length("stock_id")).groupBy("id_length").count().show()

    # print("\n=== 對照官方清單,看這些代號對應的公司名稱 ===")
    # import json
    # with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
    #     twse_records = json.load(f)
    # name_lookup = {r["公司代號"]: r["公司名稱"] for r in twse_records}

    # for sid in missing_ids_list[:15]:
    #     print(f"{sid}: {name_lookup.get(sid, '(查無)')}")

    # print("=== 以 5283(禾聯碩)為例,看它出現的所有交易日分布 ===")
    # sample_df = df.filter(F.col("stock_id") == "5283").select("dt", "close_price", "trade_volume").orderBy("dt")
    # print(f"總共出現次數: {sample_df.count()}")
    # sample_df.show(30, truncate=False)

    # print("\n=== 這 221 檔股票的產業別分布 ===")
    # industry_lookup = {r["公司代號"]: r["產業別"] for r in twse_records}
    # from collections import Counter
    # industries = [industry_lookup.get(sid, "查無") for sid in missing_ids_list]
    # print(Counter(industries).most_common(10))

    # 直接比對: 5283 這個代號,在兩邊集合裡,是否真的以完全相同的字串存在
    # print(f"'5283' in all_official_ids: {'5283' in all_official_ids}")
    # print(f"'5283' in present_ids_1217: {'5283' in present_ids_1217}")

    # 檢查 present_ids_1217 裡,有沒有「看起來像 5283 但其實不同」的值(例如帶空白)
    # for sid in present_ids_1217:
    #     if '5283' in sid:
    #         print(f"找到相似值: {repr(sid)}")  # 用 repr() 才能看出隱藏的空白或特殊字元
    
    # # 對每一天,只計算「當天已經上市」的股票數當作應有基準
    # daily_counts = df.groupBy("dt").agg(F.countDistinct("stock_id").alias("actual_count"))

    # # 對每一天,算出當天已上市的股票總數(用交叉比對: listing_date <= dt)
    from pyspark.sql import Row
    # trading_days = [row["dt"] for row in daily_counts.select("dt").distinct().collect()]

    # expected_counts = []
    # for day in trading_days:
    #     count = listing_dates.filter(F.col("listing_date") <= F.to_date(F.lit(day))).count()
    #     expected_counts.append(Row(dt=day, expected_count_adjusted=count))

    # expected_df = spark.createDataFrame(expected_counts)

    # result = daily_counts.join(expected_df, on="dt")
    # result = result.withColumn("gap", F.col("expected_count_adjusted") - F.col("actual_count"))

    # gaps = result.filter(F.col("gap") > 0).orderBy(F.desc("gap"))
    # print(f"考慮上市日期後,仍有缺漏的交易日數: {gaps.count()} / {result.count()}")
    # gaps.show(20, truncate=False)

    df = spark.read.parquet(f"gs://{bucket_name}/clean/stock_daily/").filter(F.col("market") == "TWSE")

    listing_dates = spark.createDataFrame(
        [(r["公司代號"], r["上市日期"]) for r in twse_industry_records],
        ["stock_id", "listing_date_raw"]
    ).withColumn("listing_date", F.to_date(F.col("listing_date_raw"), "yyyyMMdd"))

    daily_counts = df.groupBy("dt").agg(F.countDistinct("stock_id").alias("actual_count"))

    trading_days = [row["dt"] for row in daily_counts.select("dt").distinct().collect()]
    expected_counts = []
    for day in trading_days:
        count = listing_dates.filter(F.col("listing_date") <= F.to_date(F.lit(day))).count()
        expected_counts.append(Row(dt=day, expected_count_adjusted=count))

    expected_df = spark.createDataFrame(expected_counts)
    result = daily_counts.join(expected_df, on="dt")
    result = result.withColumn("gap", F.col("expected_count_adjusted") - F.col("actual_count"))

    gaps = result.filter(F.col("gap") > 10).orderBy(F.desc("gap"))  # 門檻拉高一點,排除掉清洗過程正常的個位數落差(如停牌)
    print(f"clean 層,gap > 10 的交易日數: {gaps.count()} / {result.count()}")
    gaps.show(30, truncate=False)

    
# if __name__ == "__main__":
#     spark = build_spark_session()

#     import json
#     with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
#         twse_official_ids = [r["公司代號"] for r in json.load(f)]

#     backfill_all_markets(spark, "stock-pulse-data-lake", twse_official_ids)

#     spark.stop()


if __name__ == "__main__":
    spark = build_spark_session()
    import json

    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    detect_twse_gaps_v2(spark, "stock-pulse-data-lake", twse_industry_records)
    spark.stop()