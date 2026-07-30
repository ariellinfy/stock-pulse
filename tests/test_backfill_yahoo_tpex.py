import json
from datetime import date
from unittest.mock import patch, MagicMock, mock_open

from scripts.backfill.backfill_yahoo_tpex import (
    load_tpex_stock_list,
    backfill_yahoo_tpex,
)


def _make_blob(name, content=None):
    blob = MagicMock()
    blob.name = name
    if content is not None:
        blob.download_as_text.return_value = content
    return blob


def test_load_tpex_stock_list_picks_latest_blob_by_name():
    older = _make_blob("raw/industry_list_tpex/dt=2026-07-01/data.json")
    latest_content = json.dumps(
        [
            {"公司代號": "6488", "公司名稱": "環球晶"},
            {"公司代號": "1240", "公司名稱": "茂生農經"},
        ]
    )
    latest = _make_blob(
        "raw/industry_list_tpex/dt=2026-07-09/data.json", latest_content
    )

    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = [older, latest]
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    stock_ids = load_tpex_stock_list(mock_client, "bkt")

    assert stock_ids == ["6488", "1240"]
    mock_bucket.list_blobs.assert_called_once_with(prefix="raw/industry_list_tpex/")
    latest.download_as_text.assert_called_once()
    older.download_as_text.assert_not_called()  # 只該讀最新那份,不是每份都讀


def test_load_tpex_stock_list_raises_when_nothing_found():
    mock_bucket = MagicMock()
    mock_bucket.list_blobs.return_value = []
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    try:
        load_tpex_stock_list(mock_client, "bkt")
        raise AssertionError("找不到清單應該要 raise RuntimeError")
    except RuntimeError:
        pass


def test_backfill_yahoo_tpex_skips_existing_and_writes_only_successful_fetches():
    def fake_exists(client, bucket, source, key, stock_id):
        return stock_id == "1111"  # 只有 1111 視為已存在

    def fake_fetch(stock_id, market, start, end):
        return None if stock_id == "2222" else [{"trade_date": "2026-07-01"}]

    with patch(
        "scripts.backfill.backfill_yahoo_tpex.get_gcs_client", return_value=MagicMock()
    ), patch(
        "scripts.backfill.backfill_yahoo_tpex.load_tpex_stock_list",
        return_value=["1111", "2222", "3333"],
    ), patch(
        "scripts.backfill.backfill_yahoo_tpex.raw_blob_exists_partitioned",
        side_effect=fake_exists,
    ), patch(
        "scripts.backfill.backfill_yahoo_tpex.fetch_yahoo_history",
        side_effect=fake_fetch,
    ), patch(
        "scripts.backfill.backfill_yahoo_tpex.write_raw_partitioned"
    ) as mock_write, patch(
        "scripts.backfill.backfill_yahoo_tpex.time.sleep"
    ), patch(
        "scripts.backfill.backfill_yahoo_tpex.Path"
    ), patch(
        "builtins.open", mock_open()
    ):
        backfill_yahoo_tpex(date(2024, 7, 1), date(2026, 7, 16), delay_seconds=0)

    # 1111 已存在跳過、2222 抓不到資料算失敗,只有 3333 應該真的被寫入
    mock_write.assert_called_once()
    assert mock_write.call_args[0][4] == "3333"


if __name__ == "__main__":
    test_load_tpex_stock_list_picks_latest_blob_by_name()
    test_load_tpex_stock_list_raises_when_nothing_found()
    test_backfill_yahoo_tpex_skips_existing_and_writes_only_successful_fetches()
    print("✅ 全部測試通過")
