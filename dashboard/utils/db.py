import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv

# Path logic 
script_dir = os.path.dirname(os.path.abspath(__file__)) 
root_dir = os.path.dirname(os.path.dirname(script_dir)) 
load_dotenv(os.path.join(root_dir, '.env'))

PROJECT_ID = os.getenv('GCP_PROJECT_ID')

@st.cache_resource
def get_client():
    try:
        key_json = os.getenv("GCP_SA_KEY")
        if key_json:
            info = json.loads(key_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"BigQuery Connection Error: {e}")
        return None

@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    client = get_client()
    if client is None:
        return pd.DataFrame()
    
    try:
        df = client.query(sql).to_dataframe()
        # Clean types for Streamlit/Altair compatibility
        for col in df.columns:
            if str(df[col].dtype) in ["Int64", "int64"]:
                df[col] = df[col].astype(float)
            if "date" in col.lower() or str(df[col].dtype) == "dbdate":
                df[col] = pd.to_datetime(df[col])
        return df
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()