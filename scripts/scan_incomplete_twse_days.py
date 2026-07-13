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
        print(f"\n{dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔")
        # for sid, name in missing_list:
        #     print(f"    {sid}  {name}")

    return suspicious_days


if __name__ == "__main__":
    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    scan_incomplete_days_v5(BUCKET_NAME, twse_industry_records)