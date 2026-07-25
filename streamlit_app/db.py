import os
from dotenv import load_dotenv
from google.cloud import bigquery
import streamlit as st

load_dotenv()


@st.cache_resource
def get_bq_client():
    key_path = os.environ.get("GCP_SA_KEY_PATH", "secrets/gcp-sa-key.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
    project_id = os.environ.get("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)
