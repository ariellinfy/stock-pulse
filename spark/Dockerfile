FROM apache/spark:4.1.2-scala2.13-java17-python3-r-ubuntu

USER root

# 基礎 image 已內建 PySpark 4.1.2,不需要重新安裝
# 只需要補上我們自己程式碼需要、但 Spark image 沒有的 Python 套件
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

# GCS Connector JAR,直接放進容器,不依賴本機掛載路徑
COPY spark/jars/gcs-connector-hadoop3-latest.jar /opt/spark/jars/gcs-connector-hadoop3-latest.jar

COPY scrapers /app/scrapers
COPY shared /app/shared
COPY spark /app/spark

ENV PYTHONPATH=/app
WORKDIR /app