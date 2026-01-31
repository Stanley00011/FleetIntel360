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
    """
    BigQuery client that works in:
    - Streamlit Cloud (uses secrets.toml)
    - Local dev (uses .env with GCP_SA_KEY)
    - GitHub Actions (uses GCP_SA_KEY env var)
    """
    try:
        # STREAMLIT CLOUD: Check if secrets exist AND contain gcp_service_account
        if hasattr(st, 'secrets'):
            try:
                if 'gcp_service_account' in st.secrets:
                    credentials_dict = dict(st.secrets["gcp_service_account"])
                    
                    # CRITICAL FIX: Remove universe_domain added by Streamlit Cloud
                    credentials_dict.pop('universe_domain', None)
                    
                    credentials = service_account.Credentials.from_service_account_info(
                        credentials_dict,
                        scopes=["https://www.googleapis.com/auth/bigquery"]
                    )
                    return bigquery.Client(
                        credentials=credentials, 
                        project=credentials_dict.get('project_id', PROJECT_ID)
                    )
            except Exception:
                # Secrets file doesn't exist or doesn't have gcp_service_account - fall through to env var
                pass
        
        # LOCAL / GITHUB ACTIONS: Use GCP_SA_KEY from environment
        key_json = os.getenv("GCP_SA_KEY")
        if key_json:
            info = json.loads(key_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # FALLBACK: Application Default Credentials (if running on GCP)
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