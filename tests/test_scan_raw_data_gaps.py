import json
from unittest.mock import patch, MagicMock

from scripts.adhoc.scan_raw_data_gaps import scan_raw_data_gaps


def _make_blob(name, data_rows):
    blob = MagicMock()
    blob.name = name
    blob.download_as_text.return_value = json.dumps({"data": data_rows})
    return blob


def _mock_client(blobs):
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = blobs
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client


INDUSTRY_RECORDS = [
    {"公司代號": "1101", "公司名稱": "台泥", "上市日期": "20000101"},
    {"公司代號": "2330", "公司名稱": "台積電", "上市日期": "20250101"},
    {
        "公司代號": "9999",
        "公司名稱": "尚未上市",
        "上市日期": "20260801",
    },  # 晚於下面所有測試日期
]


def test_scan_raw_data_gaps_excludes_not_yet_listed_stocks():
    """
    9999 的上市日期晚於這一天,即使它完全沒出現在 raw data 裡,也不該被算成
    「缺席」——這是這支腳本存在的核心理由(排除「尚未上市」造成的假警報)。
    """
    blob = _make_blob("raw/twse_daily/dt=2026-07-01/data.json", [["1101", "台泥"]])
    with patch(
        "scripts.adhoc.scan_raw_data_gaps.get_gcs_client",
        return_value=_mock_client([blob]),
    ):
        result = scan_raw_data_gaps("bkt", INDUSTRY_RECORDS)

    assert len(result) == 1
    dt_str, actual_count, missing_count, missing_list = result[0]
    assert dt_str == "2026-07-01"
    assert missing_count == 1  # 只有 2330 算缺席,9999 不算
    assert missing_list == [("2330", "台積電")]


def test_scan_raw_data_gaps_skips_days_with_no_rows():
    """空 data(非交易日)不該被當成缺席異常。"""
    blob = _make_blob("raw/twse_daily/dt=2026-07-04/data.json", [])
    with patch(
        "scripts.adhoc.scan_raw_data_gaps.get_gcs_client",
        return_value=_mock_client([blob]),
    ):
        result = scan_raw_data_gaps("bkt", INDUSTRY_RECORDS)

    assert result == []


def test_scan_raw_data_gaps_respects_min_missing_threshold_and_sorts_desc():
    blob_1_missing = _make_blob(
        "raw/twse_daily/dt=2026-07-01/data.json", [["1101", "台泥"]]
    )
    # 兩檔都缺席,但用非空 data(混雜一個非官方清單代號)避免被誤判成非交易日
    blob_2_missing = _make_blob(
        "raw/twse_daily/dt=2026-07-03/data.json", [["OTHER_STOCK", "非官方清單股票"]]
    )

    with patch(
        "scripts.adhoc.scan_raw_data_gaps.get_gcs_client",
        return_value=_mock_client([blob_1_missing, blob_2_missing]),
    ):
        result_all = scan_raw_data_gaps("bkt", INDUSTRY_RECORDS, min_missing=1)
        result_threshold_2 = scan_raw_data_gaps("bkt", INDUSTRY_RECORDS, min_missing=2)

    assert [r[0] for r in result_all] == ["2026-07-03", "2026-07-01"]  # 缺席多的排前面
    assert [r[0] for r in result_threshold_2] == [
        "2026-07-03"
    ]  # 門檻 2 只留缺 2 檔的那天


if __name__ == "__main__":
    test_scan_raw_data_gaps_excludes_not_yet_listed_stocks()
    test_scan_raw_data_gaps_skips_days_with_no_rows()
    test_scan_raw_data_gaps_respects_min_missing_threshold_and_sorts_desc()
    print("✅ 全部測試通過")
