"""
[ARCHIVED] 驗證 SparkSession 能在目前環境(WSL2)正常啟動。
這是階段三所有工作的前提,先確認地基穩固再往下寫邏輯。

狀態: 環境已確認可用,此檔案僅作歷史紀錄保留。
"""

from pyspark.sql import SparkSession


def explore():
    spark = (
        SparkSession.builder.appName("stock-pulse-explore")
        .master("local[*]")  # 本機模式,用所有可用的 CPU 核心
        .getOrCreate()
    )

    print(f"✅ SparkSession 啟動成功")
    print(f"Spark 版本: {spark.version}")

    # 建一個最小的測試 DataFrame,確認基本運算跟顯示都正常
    df = spark.createDataFrame(
        [("2330", 590.0), ("2317", 105.5)], ["stock_id", "close_price"]
    )
    df.show()

    spark.stop()
    print("✅ SparkSession 已正常關閉")


if __name__ == "__main__":
    explore()
