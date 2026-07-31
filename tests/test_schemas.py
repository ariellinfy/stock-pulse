from spark.common.schemas import (
    assert_fields_match,
    TWSE_RAW_SCHEMA,
    TPEX_RAW_SCHEMA,
)
from scrapers.twse_client import EXPECTED_FIELDS as TWSE_SCRAPER_FIELDS
from scrapers.tpex_client import EXPECTED_FIELDS as TPEX_SCRAPER_FIELDS


def test_assert_fields_match_passes_silently_when_equal():
    assert_fields_match(["a", "b"], ["a", "b"], "some_source")  # 不 raise 就算過


def test_assert_fields_match_raises_on_mismatch():
    try:
        assert_fields_match(["a", "b"], ["a", "c"], "some_source")
        raise AssertionError("欄位不符應該要 raise ValueError")
    except ValueError as e:
        assert "some_source" in str(e)


def test_twse_schema_field_count_matches_scraper_expected_fields():
    """
    spark/jobs/clean_stock.py::explode_and_flatten 是靠「位置」把
    scraper 抓回來的每一列(list of str)對應到 TWSE_RAW_SCHEMA 的欄位順序,
    兩邊的欄位數量若不一致,資料會整批位移錯位卻不會有任何錯誤訊息。
    這支測試釘住兩邊數量必須一致,欄位定義若之後任一邊調整,這裡會先炸開。
    """
    assert len(TWSE_RAW_SCHEMA.fields) == len(TWSE_SCRAPER_FIELDS)


def test_tpex_schema_field_count_matches_scraper_expected_fields():
    assert len(TPEX_RAW_SCHEMA.fields) == len(TPEX_SCRAPER_FIELDS)


if __name__ == "__main__":
    test_assert_fields_match_passes_silently_when_equal()
    test_assert_fields_match_raises_on_mismatch()
    test_twse_schema_field_count_matches_scraper_expected_fields()
    test_tpex_schema_field_count_matches_scraper_expected_fields()
    print("✅ 全部測試通過")
