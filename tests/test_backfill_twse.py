from datetime import date
from unittest.mock import patch, MagicMock

from scripts.backfill.backfill_twse import (
    generate_date_range,
    mark_no_trading_day,
    is_marked_no_trading_day,
    backfill_twse,
)
from scrapers.twse_client import FetchStatus, FetchResult


def test_generate_date_range_is_inclusive_of_start_and_end():
    days = generate_date_range(date(2026, 7, 1), date(2026, 7, 3))
    assert days == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]


def test_generate_date_range_single_day():
    assert generate_date_range(date(2026, 7, 1), date(2026, 7, 1)) == [date(2026, 7, 1)]


def test_mark_no_trading_day_writes_marker_at_expected_path():
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    mark_no_trading_day(mock_client, "bkt", "twse_daily", date(2026, 7, 9))

    mock_bucket.blob.assert_called_once_with(
        "raw/twse_daily/dt=2026-07-09/_no_data_marker.json"
    )
    mock_blob.upload_from_string.assert_called_once()


def test_is_marked_no_trading_day_checks_expected_path():
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    result = is_marked_no_trading_day(
        mock_client, "bkt", "twse_daily", date(2026, 7, 9)
    )

    assert result is True
    mock_bucket.blob.assert_called_once_with(
        "raw/twse_daily/dt=2026-07-09/_no_data_marker.json"
    )


def test_backfill_twse_handles_all_branches_across_date_range():
    """
    對一個 5 天的區間,分別讓每一天落在不同分支(已存在跳過、已標記非交易日
    跳過、新確認的非交易日、未知失敗、成功寫入),驗證整個迴圈的分支邏輯。
    """
    d_skip_existing = date(2026, 7, 1)
    d_skip_marked = date(2026, 7, 2)
    d_new_holiday = date(2026, 7, 3)
    d_unknown_failure = date(2026, 7, 4)
    d_success = date(2026, 7, 5)

    def fake_raw_blob_exists(client, bucket, source, target_date):
        return target_date == d_skip_existing

    def fake_is_marked(client, bucket, source, target_date):
        return target_date == d_skip_marked

    def fake_fetch(target_date):
        if target_date == d_new_holiday:
            return FetchResult(status=FetchStatus.NO_TRADING_DAY)
        if target_date == d_unknown_failure:
            return FetchResult(status=FetchStatus.UNKNOWN_FAILURE)
        return FetchResult(status=FetchStatus.SUCCESS, data={"fields": [], "data": []})

    with patch(
        "scripts.backfill.backfill_twse.get_gcs_client", return_value=MagicMock()
    ), patch(
        "scripts.backfill.backfill_twse.raw_blob_exists",
        side_effect=fake_raw_blob_exists,
    ), patch(
        "scripts.backfill.backfill_twse.is_marked_no_trading_day",
        side_effect=fake_is_marked,
    ), patch(
        "scripts.backfill.backfill_twse.mark_no_trading_day"
    ) as mock_mark, patch(
        "scripts.backfill.backfill_twse.fetch_daily_quotes_for_backfill",
        side_effect=fake_fetch,
    ), patch(
        "scripts.backfill.backfill_twse.write_raw_json"
    ) as mock_write:
        backfill_twse(d_skip_existing, d_success)

    mock_mark.assert_called_once()  # 只有 d_new_holiday 這天需要新標記
    mock_write.assert_called_once()  # 只有 d_success 這天需要寫入


if __name__ == "__main__":
    test_generate_date_range_is_inclusive_of_start_and_end()
    test_generate_date_range_single_day()
    test_mark_no_trading_day_writes_marker_at_expected_path()
    test_is_marked_no_trading_day_checks_expected_path()
    test_backfill_twse_handles_all_branches_across_date_range()
    print("✅ 全部測試通過")
