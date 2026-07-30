from datetime import date
from unittest.mock import patch, Mock

import requests

from scrapers.common import FetchStatus
from scrapers.tpex_client import (
    fetch_daily_quotes,
    to_roc_date,
    roc_to_gregorian,
    EXPECTED_FIELDS,
)


def _response_with_tables(tables):
    resp = Mock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"tables": tables, "date": "115/07/09"}
    return resp


def test_to_roc_date_and_back_are_inverse():
    assert to_roc_date(date(2026, 7, 9)) == "115/07/09"
    assert roc_to_gregorian("115/07/09") == "2026-07-09"


def test_fetch_daily_quotes_success_returns_expected_shape():
    table = {"fields": EXPECTED_FIELDS, "data": [["1101", "台泥"]], "date": "115/07/09"}
    with patch(
        "scrapers.tpex_client.requests.get", return_value=_response_with_tables([table])
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result.status == FetchStatus.SUCCESS
    assert result.data["fields_match_expected"] is True
    assert result.data["data"] == [["1101", "台泥"]]
    assert result.data["actual_trade_date"] == "2026-07-09"


def test_fetch_daily_quotes_no_tables_returns_no_data():
    """TPEx 官方端點在非交易日時 tables 是空陣列——這是唯一的「無資料」訊號。"""
    with patch(
        "scrapers.tpex_client.requests.get", return_value=_response_with_tables([])
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result.status == FetchStatus.NO_DATA
    assert result.data is None


def test_fetch_daily_quotes_field_mismatch_still_returns_data():
    table = {"fields": ["不一樣的欄位"], "data": [["1101"]], "date": "115/07/09"}
    with patch(
        "scrapers.tpex_client.requests.get", return_value=_response_with_tables([table])
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result.status == FetchStatus.SUCCESS
    assert result.data["fields_match_expected"] is False


def test_fetch_daily_quotes_request_exception_returns_unknown_failure():
    with patch(
        "scrapers.tpex_client.requests.get",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result.status == FetchStatus.UNKNOWN_FAILURE
    assert result.data is None


if __name__ == "__main__":
    test_to_roc_date_and_back_are_inverse()
    test_fetch_daily_quotes_success_returns_expected_shape()
    test_fetch_daily_quotes_no_tables_returns_no_data()
    test_fetch_daily_quotes_field_mismatch_still_returns_data()
    test_fetch_daily_quotes_request_exception_returns_unknown_failure()
    print("✅ 全部測試通過")
