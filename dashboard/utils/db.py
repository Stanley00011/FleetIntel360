# dashboard/utils/db.py

import duckdb
import streamlit as st
import pandas as pd

@st.cache_resource
def get_connection():
    """
    Creates a persistent connection to the DuckDB database.
    Using read_only=True allows multiple sessions to access the file safely.
    """
    return duckdb.connect("warehouse/analytics/analytics.duckdb", read_only=True)
@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    """
    Executes a SQL query and returns a pandas DataFrame.
    Results are cached for 5 minutes (300 seconds).
    """
    con = get_connection()
    return con.execute(sql).df()


