"""
每日排程用的 raw 層資料完整性檢查——只檢查『今天』這一天,不掃描整個歷史。

這裡的函式會被 airflow/dags/dag_taiwan_market.py 的每日排程自動呼叫,屬於生產
邏輯。若需要一次性掃描整個歷史、手動排查資料缺口,請用
scripts/adhoc/scan_raw_data_gaps.py(手動觸發的分析工具,不在排程裡)。
"""

import json

from shared.utils import get_gcs_client, raw_blob_path, RAW_TWSE_DAILY, RAW_TPEX_DAILY


def check_single_day_twse_completeness(
    bucket_name: str,
    target_date: str,
    twse_official_ids: list[str],
    min_ratio: float = 0.95,
):
    """
    每日排程用:只檢查『今天』這一天的擷取完整性,
    不像 scripts/adhoc/scan_raw_data_gaps.py 那樣掃描整個歷史(效能考量,且歷史已驗證過)。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(raw_blob_path(RAW_TWSE_DAILY, "dt", target_date))

    if not blob.exists():
        # 沒有檔案,可能是非交易日(已由 check_trading_day 短路處理),不算異常
        print(f"ℹ️ {target_date} 無 raw data(可能為非交易日)")
        return True

    content = json.loads(blob.download_as_text())
    rows = content.get("data", [])
    actual_ids = set(row[0] for row in rows)

    should_exist_today = set(
        twse_official_ids
    )  # 簡化: 用完整清單即可,新股上市邊界案例影響極小

    missing = should_exist_today - actual_ids
    actual_ratio = (
        len(actual_ids) / len(should_exist_today) if should_exist_today else 1.0
    )

    print(
        f"{target_date}: 實際 {len(actual_ids)} / 應有約 {len(should_exist_today)}(比例 {actual_ratio:.2%})"
    )

    if actual_ratio < min_ratio:
        raise ValueError(
            f"⚠️ {target_date} TWSE 完整性異常: 只取得 {len(actual_ids)} 檔,"
            f"低於門檻 {min_ratio:.0%}。可能缺漏股票: {list(missing)[:10]}"
        )
    return True


def check_single_day_tpex_completeness(
    bucket_name: str,
    target_date: str,
    tpex_official_ids: list[str],
    min_ratio: float = 0.95,
):
    """
    每日排程用:檢查 TPEx『今天』的擷取完整性。
    與 TWSE 版本的差異: TPEx 原始資料混雜大量權證/可轉債,
    必須先用官方股票清單過濾,只比對『真正股票』的完整性,
    不能直接比較總筆數(該數字包含大量非股票證券,波動大且無意義)。
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(raw_blob_path(RAW_TPEX_DAILY, "dt", target_date))

    if not blob.exists():
        print(f"ℹ️ {target_date} 無 TPEx raw data(可能為非交易日)")
        return True

    content = json.loads(blob.download_as_text())
    rows = content.get("data", [])

    # TPEx 原始資料代號在第一個位置,跟 TWSE 格式一致
    actual_ids = set(row[0] for row in rows)
    official_set = set(tpex_official_ids)

    # 只比對「真正股票」的部分,而非全部混雜的原始筆數
    matched = actual_ids & official_set
    missing = official_set - actual_ids

    actual_ratio = len(matched) / len(official_set) if official_set else 1.0

    print(
        f"{target_date} TPEx: 真正股票匹配 {len(matched)} / 應有 {len(official_set)}(比例 {actual_ratio:.2%})"
    )

    if actual_ratio < min_ratio:
        raise ValueError(
            f"⚠️ {target_date} TPEx 完整性異常: 官方股票清單中,只有 {len(matched)} 檔對到當日資料,"
            f"低於門檻 {min_ratio:.0%}。可能缺漏: {list(missing)[:10]}"
        )
    return True
