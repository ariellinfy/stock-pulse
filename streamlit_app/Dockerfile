FROM python:3.12-slim

WORKDIR /app

COPY streamlit_app/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY streamlit_app /app/streamlit_app

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app/app.py", "--server.address=0.0.0.0"]