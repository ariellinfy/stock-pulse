from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd

from scrapers.common import FetchStatus
from scrapers.yahoo_client import build_yahoo_ticker, fetch_yahoo_history


def test_build_yahoo_ticker_appends_market_suffix():
    assert build_yahoo_ticker("2330", "TWSE") == "2330.TW"
    assert build_yahoo_ticker("6488", "TPEx") == "6488.TWO"


def test_build_yahoo_ticker_rejects_unknown_market():
    try:
        build_yahoo_ticker("2330", "NYSE")
        raise AssertionError("不支援的 market 應該要 raise ValueError")
    except ValueError:
        pass


def _fake_history_df():
    return pd.DataFrame(
        {
            "Open": [590.0],
            "High": [595.0],
            "Low": [588.0],
            "Close": [592.0],
            "Volume": [12345678],
        },
        index=pd.to_datetime(["2026-07-08"]),
    )


def test_fetch_yahoo_history_success_returns_records():
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = _fake_history_df()

    with patch("scrapers.yahoo_client.yf.Ticker", return_value=mock_ticker):
        result = fetch_yahoo_history("2330", "TWSE", date(2026, 7, 8), date(2026, 7, 8))

    assert result.status == FetchStatus.SUCCESS
    assert result.data == [
        {
            "stock_id": "2330",
            "market": "TWSE",
            "trade_date": "2026-07-08",
            "open": 590.0,
            "high": 595.0,
            "low": 588.0,
            "close": 592.0,
            "volume": 12345678,
        }
    ]


def test_fetch_yahoo_history_empty_returns_no_data():
    """新股/下市/非交易區間時 yfinance 回傳空 DataFrame,確認無資料,不重試。"""
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()

    with patch("scrapers.yahoo_client.yf.Ticker", return_value=mock_ticker):
        result = fetch_yahoo_history("0000", "TWSE", date(2026, 7, 8), date(2026, 7, 8))

    assert result.status == FetchStatus.NO_DATA
    mock_ticker.history.assert_called_once()  # 空資料不算暫時性錯誤,不需要重試


def test_fetch_yahoo_history_exhausts_retries_on_exception():
    mock_ticker = MagicMock()
    mock_ticker.history.side_effect = RuntimeError("network boom")

    with patch("scrapers.yahoo_client.yf.Ticker", return_value=mock_ticker), patch(
        "scrapers.yahoo_client.time.sleep"
    ):
        result = fetch_yahoo_history(
            "2330", "TWSE", date(2026, 7, 8), date(2026, 7, 8), max_retries=3
        )

    assert result.status == FetchStatus.UNKNOWN_FAILURE
    assert mock_ticker.history.call_count == 3


if __name__ == "__main__":
    test_build_yahoo_ticker_appends_market_suffix()
    test_build_yahoo_ticker_rejects_unknown_market()
    test_fetch_yahoo_history_success_returns_records()
    test_fetch_yahoo_history_empty_returns_no_data()
    test_fetch_yahoo_history_exhausts_retries_on_exception()
    print("✅ 全部測試通過")
