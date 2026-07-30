import os
from unittest.mock import MagicMock, patch

import shared.utils as shared_utils
from shared.utils import (
    raw_blob_path,
    gcs_uri,
    raw_industry_list_source_name,
    write_raw_partitioned,
    raw_blob_exists_partitioned,
    RAW_TWSE_DAILY,
    RAW_YAHOO_TPEX_HISTORY,
    RAW_FEAR_GREED_HISTORY,
    CLEAN_STOCK_DAILY,
)


def test_raw_blob_path_matches_data_lake_convention():
    """釘住 raw 層路徑樣板,避免之後改動不小心跟寫入端/讀取端的既有慣例對不上。"""
    assert (
        raw_blob_path(RAW_TWSE_DAILY, "dt", "2026-07-09")
        == "raw/twse_daily/dt=2026-07-09/data.json"
    )
    assert (
        raw_blob_path(RAW_YAHOO_TPEX_HISTORY, "stock_id", "7794")
        == "raw/yahoo_tpex_history/stock_id=7794/data.json"
    )
    assert (
        raw_blob_path(RAW_FEAR_GREED_HISTORY, "range", "full")
        == "raw/fear_greed_history/range=full/data.json"
    )


def test_gcs_uri_joins_bucket_and_path():
    assert gcs_uri("my-bucket", CLEAN_STOCK_DAILY) == "gs://my-bucket/clean/stock_daily"


def test_raw_industry_list_source_name_lowercases_market():
    assert raw_industry_list_source_name("TWSE") == "industry_list_twse"
    assert raw_industry_list_source_name("TPEx") == "industry_list_tpex"


def test_write_raw_partitioned_uses_raw_blob_path_convention():
    """
    write_raw_partitioned 改用 raw_blob_path() 之後,實際組出的 blob path/回傳值
    必須跟重構前完全一致(用 mock GCS client 驗證,不需要真的連 GCS)。
    """
    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    result = write_raw_partitioned(
        mock_client, "my-bucket", RAW_TWSE_DAILY, "dt", "2026-07-09", '{"a": 1}'
    )

    mock_client.bucket.assert_called_once_with("my-bucket")
    mock_bucket.blob.assert_called_once_with("raw/twse_daily/dt=2026-07-09/data.json")
    mock_blob.upload_from_string.assert_called_once_with(
        '{"a": 1}', content_type="application/json"
    )
    assert result == "gs://my-bucket/raw/twse_daily/dt=2026-07-09/data.json"


def test_raw_blob_exists_partitioned_checks_expected_path():
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    exists = raw_blob_exists_partitioned(
        mock_client, "my-bucket", RAW_TWSE_DAILY, "dt", "2026-07-09"
    )

    mock_bucket.blob.assert_called_once_with("raw/twse_daily/dt=2026-07-09/data.json")
    assert exists is True


def test_bucket_name_lazy_getattr_raises_when_env_missing():
    """
    BUCKET_NAME 改用 PEP 562 module-level __getattr__ 延遲評估:這支測試釘住
    行為契約——環境變數缺失時,存取 BUCKET_NAME 仍要立刻清楚報錯,而不是靜默
    回傳 None 或殘留上一次成功讀到的值。
    """
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GCP_BUCKET_NAME", None)
        try:
            getattr(shared_utils, "BUCKET_NAME")
            raise AssertionError("環境變數缺失時,應該要 raise RuntimeError")
        except RuntimeError:
            pass


def test_bucket_name_lazy_getattr_returns_env_value_when_present():
    with patch.dict(os.environ, {"GCP_BUCKET_NAME": "test-bucket-123"}):
        assert getattr(shared_utils, "BUCKET_NAME") == "test-bucket-123"


def test_unrelated_module_attribute_raises_attribute_error():
    """
    確保 __getattr__ 只攔截 BUCKET_NAME/SA_KEY_PATH 這兩個名字,其他不存在的
    屬性依然是正常的 AttributeError,不會被誤吞掉。
    """
    try:
        getattr(shared_utils, "THIS_NAME_DOES_NOT_EXIST")
        raise AssertionError("不存在的屬性應該要 raise AttributeError")
    except AttributeError:
        pass


if __name__ == "__main__":
    test_raw_blob_path_matches_data_lake_convention()
    test_gcs_uri_joins_bucket_and_path()
    test_raw_industry_list_source_name_lowercases_market()
    test_write_raw_partitioned_uses_raw_blob_path_convention()
    test_raw_blob_exists_partitioned_checks_expected_path()
    test_bucket_name_lazy_getattr_raises_when_env_missing()
    test_bucket_name_lazy_getattr_returns_env_value_when_present()
    test_unrelated_module_attribute_raises_attribute_error()
    print("✅ 全部測試通過")
