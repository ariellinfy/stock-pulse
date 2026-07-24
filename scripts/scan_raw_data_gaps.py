"""
掃描 raw/twse_daily/ 底下所有交易日,比對「當天應有股票數(依上市日期反推)」
跟「raw data 實際出現的股票代號」,找出真正的擷取不完整案例。

不使用比例判斷(會被 ETF/特別股等非股票證券干擾,誤判失效已於開發過程驗證),
改用集合差集 + 上市日期過濾,排除「當天尚未上市」造成的假警報。
"""

import json
from datetime import datetime

from shared.utils import get_gcs_client, BUCKET_NAME


def scan_raw_data_gaps(bucket_name: str, twse_industry_records: list[dict], min_missing: int = 1):
    """
    回傳每個交易日「應已上市但當天缺席」的股票清單。
    min_missing: 只回報缺席數 >= 此門檻的日子(預設 1,回傳全部有缺席的日子)。
    """
    listing_dates = {}
    for r in twse_industry_records:
        raw_date = r.get("上市日期", "")
        if raw_date and len(raw_date) == 8:
            listing_dates[r["公司代號"]] = datetime.strptime(raw_date, "%Y%m%d").date()

    name_lookup = {r["公司代號"]: r["公司名稱"] for r in twse_industry_records}

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
            continue  # 非交易日,跳過

        actual_ids = set(row[0] for row in rows)
        should_exist_today = {sid for sid, ld in listing_dates.items() if ld <= trade_date}
        missing = should_exist_today - actual_ids

        if len(missing) >= min_missing:
            missing_with_names = [(sid, name_lookup.get(sid, "(查無名稱)")) for sid in sorted(missing)]
            suspicious_days.append((dt_str, len(actual_ids), len(missing), missing_with_names))

    suspicious_days.sort(key=lambda x: -x[2])

    print(f"發現 {len(suspicious_days)} 天有缺席股票(門檻: >= {min_missing} 檔):")
    for dt_str, actual, missing_count, missing_list in suspicious_days:
        print(f"  {dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔")

    return suspicious_days


def verify_days_fixed(bucket_name: str, dates_to_check: list[str], min_expected_rows: int = 1300):
    """確認指定日期重新回補後,筆數是否恢復到合理範圍。"""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    for dt_str in dates_to_check:
        blob = bucket.blob(f"raw/twse_daily/dt={dt_str}/data.json")
        if not blob.exists():
            print(f"❌ {dt_str}: 檔案不存在")
            continue

        content = json.loads(blob.download_as_text())
        row_count = len(content.get("data", []))
        status = "✅ 正常" if row_count > min_expected_rows else "⚠️ 仍偏低"
        print(f"{dt_str}: {row_count} 筆 {status}")


if __name__ == "__main__":
    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    scan_raw_data_gaps(BUCKET_NAME, twse_industry_records)
