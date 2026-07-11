import requests
import io
import pandas as pd

URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"

def explore():
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()

    print(f"Content-Type: {resp.headers.get('Content-Type')}")
    print(f"回應大小: {len(resp.content)} bytes")

    # 先看原始 bytes 的編碼線索(前 100 bytes)
    print(f"\n原始開頭 bytes: {resp.content[:100]}")

    # 嘗試用 utf-8-sig 解碼(可以自動處理 BOM),失敗就印出錯誤讓我們看
    try:
        text = resp.content.decode("utf-8-sig")
        print("\n✅ utf-8-sig 解碼成功")
    except UnicodeDecodeError as e:
        print(f"\n❌ utf-8-sig 解碼失敗: {e}")
        text = resp.content.decode("big5", errors="replace")
        print("改用 big5 解碼(可能有亂碼,僅供參考)")

    df = pd.read_csv(io.StringIO(text))
    print(f"\n欄位: {list(df.columns)}")
    print(f"筆數: {len(df)}")
    print("\n前 3 筆:")
    print(df.head(3))

if __name__ == "__main__":
    explore()