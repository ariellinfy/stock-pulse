# _exploration_archived

這個資料夾收納 Spark 清洗 pipeline 開發過程中的 explore / test 檔案。
它們不是正式 pipeline 的一部分,但保留下來作為「當初為什麼這樣設計」的紀錄。

**假設的資料夾位置**：專案根目錄下，與 `spark/`、`shared/` 同層，即：

```
stock-pulse/
├── spark/
├── shared/
├── _exploration_archived/   <- 這個資料夾
│   ├── twse/
│   ├── tpex/
│   ├── yahoo/
│   ├── merge/
│   ├── fear_greed/
│   ├── industry_list/
│   └── infra/
```

⚠️ 每個檔案裡的 `sys.path.append(str(Path(__file__).resolve().parent.parent.parent))`
都是依照上面這個深度（`_exploration_archived/<topic>/<file>.py`，往上三層到專案根目錄）調整過的。
如果你把資料夾放在不同深度，記得同步調整 `.parent` 的數量，否則 `from spark.common...` /
`from shared.utils...` 的 import 會失敗。

## 分類與狀態

| 資料夾 | 檔案 | 內容 | 狀態 |
|---|---|---|---|
| `twse/` | `explore_twse_schema_test.py` | 驗證原始 JSON 套用 `TWSE_RAW_SCHEMA` | 結論已併入 `clean_stock.py` |
| `twse/` | `explore_twse_clean_logic.py` | TWSE 數值清洗 + 漲跌正負號邏輯 | 結論已併入 `clean_stock.py` |
| `twse/` | `explore_twse_live_vs_stored_diff.py` | 即時 API vs GCS 儲存筆數差異稽核 | 一次性稽核,非 pipeline 邏輯 |
| `tpex/` | `explore_tpex_clean_schema.py` | TPEx 清洗 + 官方清單過濾後的 null OHLC 稽核 | 結論已併入 `clean_stock.py` |
| `yahoo/` | `explore_yahoo_history_collision_check.py` | Yahoo 歷史資料清洗/統一 + 分區欄位衝突驗證 | 結論已併入 `clean_stock.py` |
| `merge/` | `explore_merge_three_markets.py` | 三來源 schema 相容性驗證 + 實際 `unionByName` 合併 | `verify_schema_compatibility()` 為實用 debug 工具,可考慮保留；合併邏輯已併入 `clean_stock.py` |
| `fear_greed/` | `explore_fear_greed_raw_inspect.py` | 檢查原始 JSON 巢狀結構、時間戳重複 | 一次性結構探索 |
| `fear_greed/` | `explore_fear_greed_docker_clean_test.py` | Docker 內驗證清洗邏輯(含重複 dt 稽核) | 結論已固定 |
| `fear_greed/` | `explore_fear_greed_write_test.py` | 清洗後寫出至 GCS 的驗證 | 若通過應收斂進正式 job |
| `industry_list/` | `explore_industry_list_gcs_loader.py` | 從 GCS(而非本機檔案)讀取最新產業清單 | 若通過建議搬進 `shared/utils.py` 作為正式函式 |
| `infra/` | `explore_gcs_connector_test.py` | GCS Connector 最小連線驗證 | 環境設定已確認可用 |
| `infra/` | `explore_partition_overwrite_test.py` | 動態分區覆寫 + 先合併再寫出 vs 分開寫入 | 結論(先合併再寫出)已套用到正式邏輯 |
| `infra/` | `explore_spark_session_startup.py` | WSL2 環境 SparkSession 啟動驗證 | 環境已確認可用,純歷史紀錄 |

## 說明

- 每個檔案開頭都補上了 `[ARCHIVED]` 註記與一句話說明目的和目前狀態,方便日後回頭查時不用重新讀完整個檔案。
- 檔名統一改成 `explore_<主題>_<動作>.py`，比原本的 `test_xxx.py` / 無意義檔名更容易搜尋。
- 內容本身沒有改動邏輯，只移除了大量被註解掉的 debug print（那些已經在原檔案裡被你自己註解掉的部分），保留有效程式碼與說明性註解。
