FROM python:3.12-slim

WORKDIR /app

COPY streamlit_app/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY streamlit_app /app/streamlit_app
# 只需要 shared/env.py 這種零重依賴的小工具(不含 shared/utils.py 需要的
# google-cloud-storage),所以整個 shared/ 都能安全 copy 進來,不用額外裝套件
COPY shared /app/shared

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app/app.py", "--server.address=0.0.0.0"]