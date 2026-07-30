import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from shared.env import resolve_gcp_credentials

load_dotenv()


@st.cache_resource
def get_bq_client():
    resolve_gcp_credentials()
    project_id = os.environ.get("GCP_PROJECT_ID")
    return bigquery.Client(project=project_id)
