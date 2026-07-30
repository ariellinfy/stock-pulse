from datetime import date
from unittest.mock import patch, Mock

from scrapers.twse_client import (
    fetch_daily_quotes_for_daily_schedule,
    fetch_daily_quotes_for_backfill,
    FetchStatus,
)


def _no_data_response():
    """模擬 TWSE 對『非交易日』與『資料尚未彙整完成』回傳的、完全相同的無資料回應。"""
    resp = Mock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "stat": "很抱歉，沒有符合條件的資料!",
        "type": "ALLBUT0999",
    }
    return resp


def test_daily_schedule_reports_unknown_failure_without_guessing_holiday():
    """
    每日排程版本遇到 stat != OK 時,必須直接回報 UNKNOWN_FAILURE、只打一次請求,
    把「這天到底是還沒統計完,還是真的休市」的判斷交給 Airflow 之後的 retry,
    而不是自己嘗試連續確認後判定為非交易日——否則會重現「當天資料還沒統計完成,
    卻被誤判成假日、靜默漏資料」的問題。
    """
    with patch(
        "scrapers.twse_client.requests.get", return_value=_no_data_response()
    ) as mock_get:
        result = fetch_daily_quotes_for_daily_schedule(date(2026, 7, 9))

    assert result.status == FetchStatus.UNKNOWN_FAILURE
    assert result.data is None
    assert mock_get.call_count == 1  # 不應該為了猜測是不是假日而重試


def test_backfill_still_confirms_no_trading_day_for_past_dates():
    """
    backfill 版本的行為維持不變:對已確定過去的日期,連續
    no_data_confirm_attempts 次收到無資料回應後,才判定為非交易日。
    這支測試同時保證兩個函式改名後,行為沒有被不小心互換或改壞。
    """
    with patch(
        "scrapers.twse_client.requests.get", return_value=_no_data_response()
    ) as mock_get, patch("scrapers.twse_client.time.sleep"):
        result = fetch_daily_quotes_for_backfill(
            date(2024, 7, 6), no_data_confirm_attempts=2
        )

    assert result.status == FetchStatus.NO_TRADING_DAY
    assert mock_get.call_count == 2


if __name__ == "__main__":
    test_daily_schedule_reports_unknown_failure_without_guessing_holiday()
    test_backfill_still_confirms_no_trading_day_for_past_dates()
    print("✅ 全部測試通過")
