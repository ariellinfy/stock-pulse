FROM apache/airflow:3.3.0-python3.12

USER root

# 建立一個 GID 跟宿主機 docker 群組一致的群組,並把 airflow 使用者加進去
# 注意: 用 getent group docker 查實際 GID
ARG DOCKER_GID=1001
RUN groupadd -g ${DOCKER_GID} docker_host || true
RUN usermod -aG docker_host airflow

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt