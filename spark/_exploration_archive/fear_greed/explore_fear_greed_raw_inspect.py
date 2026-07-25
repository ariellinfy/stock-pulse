"""
[ARCHIVED] 檢查 GCS 上 fear_greed_history 原始 JSON 的巢狀結構,
並找出時間戳(x)是否有重複值。

狀態: 一次性資料結構探索,非 Spark job。
若要移動此檔案的位置,請確認下方 sys.path.append 的 .parent 層數
與新的資料夾深度一致(目前假設: 專案根目錄/_exploration_archived/fear_greed/)。
"""

import sys
import json
from pathlib import Path
from collections import Counter

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.utils import get_gcs_client, BUCKET_NAME

client = get_gcs_client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob("raw/fear_greed_history/range=full/data.json")
content = json.loads(blob.download_as_text())

print("最外層 keys:", list(content.keys()))
print()
print("fear_and_greed_historical.data 前 2 筆:")
print(content["fear_and_greed_historical"]["data"][:2])
print()
print(
    "fear_and_greed_historical.data 總筆數:",
    len(content["fear_and_greed_historical"]["data"]),
)

x_values = [item["x"] for item in content["fear_and_greed_historical"]["data"]]
duplicates = {k: v for k, v in Counter(x_values).items() if v > 1}
print(f"重複的時間戳數量: {len(duplicates)}")
print(f"範例: {list(duplicates.items())[:5]}")
