FROM python:3.12-slim

WORKDIR /app

COPY dbt/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dbt /app/dbt

WORKDIR /app/dbt