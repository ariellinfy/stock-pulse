"""
本機驗證: 動態分區覆寫 + unionByName 合併寫出,
確認同一個 dt 分區能正確同時容納兩個不同來源(market)的資料,
且不會因為分開寫入而互相覆蓋。
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def explore():
    spark = SparkSession.builder.appName("partition-test").master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    output_path = "/tmp/test_clean_stock_daily"  # 本機暫存路徑,不動到 GCS

    # 第一批: 模擬 TWSE 資料,涵蓋 2 天
    twse_fake = spark.createDataFrame(
        [
            ("2024-07-01", "1101", "TWSE", 23.1),
            ("2024-07-02", "1101", "TWSE", 23.5),
        ],
        ["dt", "stock_id", "market", "close_price"]
    )

    # 第二批: 模擬 Yahoo TPEx 資料,涵蓋同樣 2 天(但不同股票)
    yahoo_fake = spark.createDataFrame(
        [
            ("2024-07-01", "6026", "TPEx", 12.37),
            ("2024-07-02", "6026", "TPEx", 12.45),
        ],
        ["dt", "stock_id", "market", "close_price"]
    )

    # print("=== 情境 A: 分開寫入(驗證是否會互相覆蓋)===")
    # twse_fake.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    # yahoo_fake.write.mode("overwrite").partitionBy("dt").parquet(output_path)  # 第二次寫入,同樣的 dt

    # result_a = spark.read.parquet(output_path)
    # print(f"分開寫入後,總筆數: {result_a.count()}")
    # result_a.orderBy("dt", "market").show()

    print("\n=== 情境 B: 先合併,再一次寫出(正確做法)===")
    combined_fake = twse_fake.unionByName(yahoo_fake)

    output_path_b = "/tmp/test_clean_stock_daily_b"
    combined_fake.write.mode("overwrite").partitionBy("dt").parquet(output_path_b)

    result_b = spark.read.parquet(output_path_b)
    print(f"合併寫入後,總筆數: {result_b.count()}")
    result_b.orderBy("dt", "market").show()

    spark.stop()


if __name__ == "__main__":
    explore()