import json
from unittest.mock import patch, MagicMock

from shared.completeness_check import (
    check_single_day_twse_completeness,
    check_single_day_tpex_completeness,
)


def _mock_client(blob_exists: bool, content: dict | None = None):
    mock_blob = MagicMock()
    mock_blob.exists.return_value = blob_exists
    if content is not None:
        mock_blob.download_as_text.return_value = json.dumps(content)

    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client


def test_twse_completeness_no_blob_is_treated_as_non_trading_day():
    """沒有 raw data 的檔案時(可能是非交易日),不算異常,直接回傳 True。"""
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=False),
    ):
        assert (
            check_single_day_twse_completeness("bkt", "2026-07-09", ["1101", "2330"])
            is True
        )


def test_twse_completeness_passes_when_ratio_above_threshold():
    content = {"data": [["1101"], ["2330"]]}
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=True, content=content),
    ):
        assert (
            check_single_day_twse_completeness("bkt", "2026-07-09", ["1101", "2330"])
            is True
        )


def test_twse_completeness_raises_when_ratio_below_threshold():
    # 官方清單有 4 檔,今天只抓到 2 檔 → 50%,低於預設門檻 95%
    content = {"data": [["1101"], ["2330"]]}
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=True, content=content),
    ):
        try:
            check_single_day_twse_completeness(
                "bkt", "2026-07-09", ["1101", "2330", "2317", "6488"]
            )
            raise AssertionError("完整性低於門檻應該要 raise ValueError")
        except ValueError as e:
            assert "TWSE 完整性異常" in str(e)


def test_tpex_completeness_no_blob_is_treated_as_non_trading_day():
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=False),
    ):
        assert check_single_day_tpex_completeness("bkt", "2026-07-09", ["6488"]) is True


def test_tpex_completeness_ignores_non_official_rows_when_computing_ratio():
    """
    TPEx 原始資料混雜大量非股票證券,只有 official 清單裡的代號才算數——
    這支測試釘住「原始資料裡混了一堆不相干代號」不會拉低比對到的比例。
    """
    content = {"data": [["6488"], ["WARRANT_1"], ["WARRANT_2"], ["BOND_1"]]}
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=True, content=content),
    ):
        # official 清單只有 1 檔(6488),且有對到 → 100%,即使原始資料裡還有其他 3 筆雜訊
        assert check_single_day_tpex_completeness("bkt", "2026-07-09", ["6488"]) is True


def test_tpex_completeness_raises_when_matched_ratio_below_threshold():
    content = {"data": [["6488"]]}
    with patch(
        "shared.completeness_check.get_gcs_client",
        return_value=_mock_client(blob_exists=True, content=content),
    ):
        try:
            check_single_day_tpex_completeness(
                "bkt", "2026-07-09", ["6488", "1234", "5678", "9012"]
            )
            raise AssertionError("完整性低於門檻應該要 raise ValueError")
        except ValueError as e:
            assert "TPEx 完整性異常" in str(e)


if __name__ == "__main__":
    test_twse_completeness_no_blob_is_treated_as_non_trading_day()
    test_twse_completeness_passes_when_ratio_above_threshold()
    test_twse_completeness_raises_when_ratio_below_threshold()
    test_tpex_completeness_no_blob_is_treated_as_non_trading_day()
    test_tpex_completeness_ignores_non_official_rows_when_computing_ratio()
    test_tpex_completeness_raises_when_matched_ratio_below_threshold()
    print("✅ 全部測試通過")
