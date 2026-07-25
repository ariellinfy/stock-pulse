import requests

URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"


def explore():
    params = {
        "response": "json",
        "date": "20260708",
        "type": "ALLBUT0999",
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(URL, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    print(f"stat: {data.get('stat')}")
    tables = data.get("tables", [])
    print(f"共有 {len(tables)} 張表\n")

    for i, table in enumerate(tables):
        if not table:
            print(f"[{i}] (空表)")
            continue
        title = table.get("title", "無標題")
        fields = table.get("fields", [])
        row_count = len(table.get("data", []))
        print(f"[{i}] title={title!r}")
        print(f"    欄位數={len(fields)}, fields={fields}")
        print(f"    資料筆數={row_count}")
        print()


if __name__ == "__main__":
    explore()
