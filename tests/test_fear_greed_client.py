from datetime import date
from unittest.mock import patch, Mock

import requests

from scrapers.common import FetchStatus
from scrapers.fear_greed_client import fetch_fear_greed, fetch_fear_greed_full_history


def _response(payload):
    resp = Mock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


def test_fetch_fear_greed_success_returns_full_payload():
    payload = {
        "fear_and_greed": {"score": 55.5, "rating": "greed", "timestamp": "2026-07-09"},
        "fear_and_greed_historical": {"data": []},
    }
    with patch(
        "scrapers.fear_greed_client.requests.get", return_value=_response(payload)
    ):
        result = fetch_fear_greed(date(2026, 7, 9))

    assert result.status == FetchStatus.SUCCESS
    assert result.data == payload  # 忠實回傳整包,不篩選欄位


def test_fetch_fear_greed_missing_required_key_returns_unknown_failure():
    payload = {"some_other_key": {}}
    with patch(
        "scrapers.fear_greed_client.requests.get", return_value=_response(payload)
    ):
        result = fetch_fear_greed(date(2026, 7, 9))

    assert result.status == FetchStatus.UNKNOWN_FAILURE
    assert result.data is None


def test_fetch_fear_greed_missing_sub_key_returns_unknown_failure():
    payload = {"fear_and_greed": {"score": 55.5}}  # 缺 rating/timestamp
    with patch(
        "scrapers.fear_greed_client.requests.get", return_value=_response(payload)
    ):
        result = fetch_fear_greed(date(2026, 7, 9))

    assert result.status == FetchStatus.UNKNOWN_FAILURE


def test_fetch_fear_greed_request_exception_returns_unknown_failure():
    with patch(
        "scrapers.fear_greed_client.requests.get",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        result = fetch_fear_greed(date(2026, 7, 9))

    assert result.status == FetchStatus.UNKNOWN_FAILURE


def test_fetch_fear_greed_full_history_delegates_to_fetch_fear_greed():
    payload = {
        "fear_and_greed": {"score": 10.0, "rating": "fear", "timestamp": "2026-07-09"}
    }
    with patch(
        "scrapers.fear_greed_client.requests.get", return_value=_response(payload)
    ):
        result = fetch_fear_greed_full_history(date(2026, 7, 9))

    assert result.status == FetchStatus.SUCCESS
    assert result.data == payload


if __name__ == "__main__":
    test_fetch_fear_greed_success_returns_full_payload()
    test_fetch_fear_greed_missing_required_key_returns_unknown_failure()
    test_fetch_fear_greed_missing_sub_key_returns_unknown_failure()
    test_fetch_fear_greed_request_exception_returns_unknown_failure()
    test_fetch_fear_greed_full_history_delegates_to_fetch_fear_greed()
    print("✅ 全部測試通過")
