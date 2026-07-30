from datetime import date
from unittest.mock import patch, Mock

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

    assert result is not None
    assert result["fields_match_expected"] is True
    assert result["data"] == [["1101", "台泥"]]
    assert result["actual_trade_date"] == "2026-07-09"


def test_fetch_daily_quotes_no_tables_returns_none():
    """
    TPEx 官方端點在非交易日時 tables 是空陣列——這是唯一「無資料」的訊號,
    跟 TWSE 的多重狀態設計不同,呼叫端只能用 is None 判斷。
    """
    with patch(
        "scrapers.tpex_client.requests.get", return_value=_response_with_tables([])
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result is None


def test_fetch_daily_quotes_field_mismatch_still_returns_data():
    table = {"fields": ["不一樣的欄位"], "data": [["1101"]], "date": "115/07/09"}
    with patch(
        "scrapers.tpex_client.requests.get", return_value=_response_with_tables([table])
    ):
        result = fetch_daily_quotes(date(2026, 7, 9))

    assert result is not None
    assert result["fields_match_expected"] is False


if __name__ == "__main__":
    test_to_roc_date_and_back_are_inverse()
    test_fetch_daily_quotes_success_returns_expected_shape()
    test_fetch_daily_quotes_no_tables_returns_none()
    test_fetch_daily_quotes_field_mismatch_still_returns_data()
    print("✅ 全部測試通過")
