from unittest.mock import patch, Mock

import requests

from scrapers.common import FetchStatus
from scrapers.industry_client import fetch_industry_list


def _csv_response(csv_text: str):
    resp = Mock()
    resp.raise_for_status.return_value = None
    resp.content = csv_text.encode("utf-8-sig")
    return resp


def test_fetch_industry_list_success_parses_all_rows_as_strings():
    csv_text = "公司代號,公司名稱,產業別\n1101,台泥,水泥工業\n0050,元大台灣50,ETF\n"
    with patch(
        "scrapers.industry_client.requests.get", return_value=_csv_response(csv_text)
    ):
        result = fetch_industry_list("TWSE")

    assert result.status == FetchStatus.SUCCESS
    assert result.data == [
        {"公司代號": "1101", "公司名稱": "台泥", "產業別": "水泥工業"},
        {"公司代號": "0050", "公司名稱": "元大台灣50", "產業別": "ETF"},
    ]


def test_fetch_industry_list_keeps_leading_zero_stock_ids():
    """
    dtype=str 是刻意設計:代號開頭是 0(如 0050)不能被當數字讀,否則會被
    自動去掉開頭的 0。這支測試釘住這個已知會踩到的陷阱不再回歸。
    """
    csv_text = "公司代號,公司名稱\n0050,元大台灣50\n"
    with patch(
        "scrapers.industry_client.requests.get", return_value=_csv_response(csv_text)
    ):
        result = fetch_industry_list("TWSE")

    assert result.data[0]["公司代號"] == "0050"


def test_fetch_industry_list_unknown_market_raises_value_error():
    """
    未知市場別是呼叫端傳錯參數(programming error),不是執行期的資料狀態,
    應該直接 raise,而不是包裝成一種「失敗結果」悄悄回傳。
    """
    try:
        fetch_industry_list("NYSE")
        raise AssertionError("不支援的 market 應該要 raise ValueError")
    except ValueError:
        pass


def test_fetch_industry_list_request_exception_returns_unknown_failure():
    with patch(
        "scrapers.industry_client.requests.get",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        result = fetch_industry_list("TWSE")

    assert result.status == FetchStatus.UNKNOWN_FAILURE
    assert result.data is None


if __name__ == "__main__":
    test_fetch_industry_list_success_parses_all_rows_as_strings()
    test_fetch_industry_list_keeps_leading_zero_stock_ids()
    test_fetch_industry_list_unknown_market_raises_value_error()
    test_fetch_industry_list_request_exception_returns_unknown_failure()
    print("✅ 全部測試通過")
