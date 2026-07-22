FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-bigquery

COPY dbt /app/dbt

WORKDIR /app/dbt