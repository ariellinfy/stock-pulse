"""
一次性/手動分析腳本:掃描 raw/twse_daily/ 底下所有交易日,比對「當天應有股票數
(依上市日期反推)」跟「raw data 實際出現的股票代號」,找出真正的擷取不完整案例。

用途: 跟 spark/quality/detect_clean_layer_gaps.py(clean 層)交叉驗證同一批異常
      日期,確保結論不是單一計算路徑的偶然結果。這是手動觸發的分析工具,不像
      shared/completeness_check.py 裡的函式會被每日排程自動呼叫。

不使用比例判斷(會被 ETF/特別股等非股票證券干擾,誤判失效已於開發過程驗證),
改用集合差集 + 上市日期過濾,排除「當天尚未上市」造成的假警報。
"""

import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME, RAW_TWSE_DAILY


def scan_raw_data_gaps(
    bucket_name: str, twse_industry_records: list[dict], min_missing: int = 1
):
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
    blobs = list(bucket.list_blobs(prefix=f"raw/{RAW_TWSE_DAILY}/"))

    suspicious_days = []

    for blob in blobs:
        dt_str = blob.name.split("dt=")[1].split("/")[0]
        trade_date = datetime.strptime(dt_str, "%Y-%m-%d").date()

        content = json.loads(blob.download_as_text())
        rows = content.get("data", [])
        if not rows:
            continue  # 非交易日,跳過

        actual_ids = set(row[0] for row in rows)
        should_exist_today = {
            sid for sid, ld in listing_dates.items() if ld <= trade_date
        }
        missing = should_exist_today - actual_ids

        if len(missing) >= min_missing:
            missing_with_names = [
                (sid, name_lookup.get(sid, "(查無名稱)")) for sid in sorted(missing)
            ]
            suspicious_days.append(
                (dt_str, len(actual_ids), len(missing), missing_with_names)
            )

    suspicious_days.sort(key=lambda x: -x[2])

    print(f"發現 {len(suspicious_days)} 天有缺席股票(門檻: >= {min_missing} 檔):")
    for dt_str, actual, missing_count, missing_list in suspicious_days:
        print(f"  {dt_str}: 實際出現 {actual} 檔, 缺席 {missing_count} 檔")

    return suspicious_days


if __name__ == "__main__":
    with open("local_output/industry_list_twse.json", "r", encoding="utf-8") as f:
        twse_industry_records = json.load(f)

    scan_raw_data_gaps(BUCKET_NAME, twse_industry_records)
