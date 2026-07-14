import json
from datetime import date

from shared.utils import get_gcs_client, write_raw_json, BUCKET_NAME

def scan_incomplete_days_v2(bucket_name: str, twse_industry_records: list[dict]):
    """
    掃描 raw/twse_daily/ 底下所有交易日,比對「當天應有股票數(依上市日期反推)」
    跟「raw data 實際筆數」,抓出所有真正的擷取不完整案例。
    """
    from datetime import datetime

    # 建立 (股票代號 -> 上市日期) 對照
    listing_dates = {}
    for r in twse_industry_records:
        raw_date = r.get("上市日期", "")
        if raw_date and len(raw_date) == 8:
            listing_dates[r["公司代號"]] = datetime.strptime(raw_date, "%Y%m%d").date()

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="raw/twse_daily/"))

    suspicious_days = []

    for blob in blobs:
        # 從路徑解析出日期,例如 raw/twse_daily/dt=2025-12-17/data.json
        dt_str = blob.name.split("dt=")[1].split("/")[0]
        trade_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        actual_count = len(rows)

        # 當天「應該」已經上市的股票數(只算純股票清單,不含之後可能混雜的 ETF 等,
        # 這裡先用寬鬆基準: 只要上市日期 <= 當天,就算應該存在)
        expected_count = sum(1 for d in listing_dates.values() if d <= trade_date)

        # 用比例而非絕對筆數判斷,避免掛過多沒意義的小差異
        # (畢竟 MI_INDEX 含 ETF 等,不會剛好等於純股票清單數,重點是抓「異常大」的落差)
        if expected_count > 0:
            ratio = actual_count / expected_count
            if ratio < 0.85:  # 實際筆數低於預期的 85%,視為可疑
                suspicious_days.append((dt_str, actual_count, expected_count, round(ratio, 3)))

    print(f"發現 {len(suspicious_days)} 天疑似擷取不完整:")
    for dt_str, actual, expected, ratio in sorted(suspicious_days, key=lambda x: x[3]):
        print(f"  {dt_str}: 實際 {actual} / 預期至少 {expected} (比例 {ratio})")

    return suspicious_days


def scan_incomplete_days_debug(bucket_name: str, twse_industry_records: list[dict]):
    from datetime import datetime

    listing_dates = {}
    for r in twse_industry_records:
        raw_date = r.get("上市日期", "")
        if raw_date and len(raw_date) == 8:
            listing_dates[r["公司代號"]] = datetime.strptime(raw_date, "%Y%m%d").date()

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="raw/twse_daily/"))

    print(f"總共掃描到 {len(blobs)} 個檔案\n")

    # 先只印出我們已知有問題的那天,驗證程式邏輯本身對不對
    for blob in blobs:
        if "2025-12-17" not in blob.name:
            continue

        dt_str = blob.name.split("dt=")[1].split("/")[0]
        trade_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        actual_count = len(rows)

        expected_count = sum(1 for d in listing_dates.values() if d <= trade_date)

        print(f"檔案: {blob.name}")
        print(f"actual_count: {actual_count}")
        print(f"expected_count: {expected_count}")
        print(f"ratio: {actual_count / expected_count if expected_count > 0 else 'N/A'}")


def scan_incomplete_days_v3(bucket_name: str, twse_official_ids: list[str]):
    """
    正確版本: 直接用集合差集,而非籠統的總數比例,
    找出每一天「officially 已上市但當天缺席」的股票數量。
    """
    from datetime import datetime

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="raw/twse_daily/"))

    twse_official_ids = [r["公司代號"] for r in twse_industry_records]
    all_official = set(twse_official_ids)
    suspicious_days = []

    for blob in blobs:
        dt_str = blob.name.split("dt=")[1].split("/")[0]

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        if not rows:
            continue  # 非交易日,跳過

        actual_ids = set(row[0] for row in rows)
        missing = all_official & (all_official - actual_ids)  # 官方清單裡有、但當天沒出現的

        # 用絕對數量判斷,不要用比例(避免被 ETF/特別股等干擾)
        if len(missing) > 50:  # 門檻先設寬鬆一點,任何明顯異常的都先抓出來看
            suspicious_days.append((dt_str, len(actual_ids), len(missing)))

    print(f"發現 {len(suspicious_days)} 天缺席股票數超過 50 檔:")
    for dt_str, actual, missing_count in sorted(suspicious_days, key=lambda x: -x[2]):
        print(f"  {dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔")

    return suspicious_days


def scan_incomplete_days_v4(bucket_name: str, twse_industry_records: list[dict]):
    """
    正確版本: 結合集合差集 + 上市日期過濾,排除「當天尚未上市」造成的假警報,
    只抓出真正的擷取缺漏。
    """
    from datetime import datetime

    listing_dates = {}
    for r in twse_industry_records:
        raw_date = r.get("上市日期", "")
        if raw_date and len(raw_date) == 8:
            listing_dates[r["公司代號"]] = datetime.strptime(raw_date, "%Y%m%d").date()

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="raw/twse_daily/"))

    suspicious_days = []

    for blob in blobs:
        dt_str = blob.name.split("dt=")[1].split("/")[0]
        trade_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        if not rows:
            continue

        actual_ids = set(row[0] for row in rows)

        # 只算「當天已經上市」的股票,才拿來跟實際出現的代號比對
        should_exist_today = {sid for sid, ld in listing_dates.items() if ld <= trade_date}
        missing = should_exist_today - actual_ids

        if len(missing) > 0:  # 門檻可以抓緊一點,因為已排除了「尚未上市」的雜訊
            suspicious_days.append((dt_str, len(actual_ids), len(missing)))

    print(f"發現 {len(suspicious_days)} 天真正疑似擷取不完整:")
    for dt_str, actual, missing_count in sorted(suspicious_days, key=lambda x: -x[2]):
        print(f"  {dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔，missing: {missing}")

    return suspicious_days


def scan_incomplete_days_v5(bucket_name: str, twse_industry_records: list[dict]):
    """
    掃描 raw/twse_daily/ 底下所有交易日,比對「當天應有股票數(依上市日期反推)」
    跟「raw data 實際筆數」,抓出真正的擷取不完整案例,並列出實際缺席的股票代號。
    """
    from datetime import datetime

    listing_dates = {}
    for r in twse_industry_records:
        raw_date = r.get("上市日期", "")
        if raw_date and len(raw_date) == 8:
            listing_dates[r["公司代號"]] = datetime.strptime(raw_date, "%Y%m%d").date()

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="raw/twse_daily/"))

    suspicious_days = []

    for blob in blobs:
        dt_str = blob.name.split("dt=")[1].split("/")[0]
        trade_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        if not rows:
            continue

        actual_ids = set(row[0] for row in rows)

        should_exist_today = {sid for sid, ld in listing_dates.items() if ld <= trade_date}
        missing = should_exist_today - actual_ids

        if len(missing) > 0:
            # 額外帶出股票名稱,方便直接看懂缺席的是哪些公司,不用再回頭查代號對照表
            name_lookup = {r["公司代號"]: r["公司名稱"] for r in twse_industry_records}
            missing_with_names = [(sid, name_lookup.get(sid, "(查無名稱)")) for sid in sorted(missing)]
            suspicious_days.append((dt_str, len(actual_ids), len(missing), missing_with_names))

    print(f"發現 {len(suspicious_days)} 天有缺席股票:")
    for dt_str, actual, missing_count, missing_list in sorted(suspicious_days, key=lambda x: -x[2]):
        print(f"{dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔")
        # for sid, name in missing_list:
        #     print(f"    {sid}  {name}")

    return suspicious_days


if __name__ == "__main__":
    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    scan_incomplete_days_v5(BUCKET_NAME, twse_industry_records)


def verify_fixed_days(bucket_name: str, dates_to_check: list[str]):
    """
    確認之前重新回補的日期,現在的筆數是否恢復到合理範圍。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    for dt_str in dates_to_check:
        blob_path = f"raw/twse_daily/dt={dt_str}/data.json"
        blob = bucket.blob(blob_path)

        if not blob.exists():
            print(f"❌ {dt_str}: 檔案不存在,回補似乎沒有成功寫入")
            continue

        content = json.loads(blob.download_as_text())
        row_count = len(content.get("data", []))
        print(f"{dt_str}: {row_count} 筆" + (" ✅ 看起來正常" if row_count > 1300 else " ⚠️ 仍然偏低"))


# if __name__ == "__main__":
#     verify_fixed_days(BUCKET_NAME, ["2024-07-11", "2025-12-17", "2024-10-13", "2024-11-06"])
    # import requests

    # headers = {"User-Agent": "Mozilla/5.0"}
    # for date_str in ["20240711", "20241106"]:
    #     resp = requests.get(
    #         "https://www.twse.com.tw/exchangeReport/MI_INDEX",
    #         params={"response": "json", "date": date_str, "type": "ALLBUT0999"},
    #         headers=headers, timeout=15
    #     )
    #     data = resp.json()
    #     rows = data["tables"][8]["data"]
    #     print(f"{date_str}: 即時查詢筆數 = {len(rows)}")


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


if __name__ == "__main__":
    spark = build_spark_session("stock-pulse-backfill-clean-twse")

    import json
    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_official_ids = [r["公司代號"] for r in json.load(f)]

    detect_twse_gaps_v2(spark, "stock-pulse-data-lake", twse_official_ids)

    spark.stop()