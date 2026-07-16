import yfinance as yf

def explore():
    # 測試一支 TWSE 股票(台積電)跟一支 TPEx 股票(這裡先用我們資料裡看過的代號)
    test_cases = [
        ("2330.TW", "TWSE - 台積電"),
        ("1240.TWO", "TPEx - 茂生農經"),
    ]

    for ticker, label in test_cases:
        print(f"\n=== {label} ({ticker}) ===")
        stock = yf.Ticker(ticker)
        hist = stock.history(start="2026-07-01", end="2026-07-16")
        print(hist)
        print(f"欄位: {list(hist.columns)}")
        print(f"資料型態 (index): {type(hist.index)}")

if __name__ == "__main__":
    explore()