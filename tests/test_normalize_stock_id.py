from shared.utils import normalize_stock_id


def test_normalize_stock_id():
    cases = [
        ("2330.TW", ("2330", "TWSE")),
        ("6488.TWO", ("6488", "TPEx")),
        ("2330", ("2330", "UNKNOWN")),
        ("  2330.tw  ", ("2330", "TWSE")),  # 測試空白與大小寫容錯
    ]

    for raw_input, expected in cases:
        result = normalize_stock_id(raw_input)
        status = "✅" if result == expected else "❌"
        print(
            f"{status} normalize_stock_id({raw_input!r}) = {result}  (預期: {expected})"
        )


if __name__ == "__main__":
    test_normalize_stock_id()
