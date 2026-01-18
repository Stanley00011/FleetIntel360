# dashboard/components/filters.py
import streamlit as st
from datetime import date, timedelta
from typing import List, Optional
from utils.db import run_query

def date_filter(default_days: int = 7, key: str = "date_range"):
    today = date.today()
    start_default = today - timedelta(days=default_days)
    res = st.date_input("Date range", value=(start_default, today), key=key)
    if isinstance(res, (list, tuple)) and len(res) == 2:
        return res[0], res[1]
    return start_default, today

def entity_filter(label: str, entities: List[str], key: str):
    options = ["All"] + sorted(list(set([str(e) for e in entities if e])))
    selected = st.selectbox(label, options=options, key=key)
    return None if selected == "All" else selected

def driver_filter(drivers: Optional[List[str]] = None, key: str = "driver_select"):
    if drivers is None:
        # Fetching from  specific schema
        df = run_query("SELECT DISTINCT driver_id FROM mart.fact_driver_daily_metrics")
        drivers = df["driver_id"].tolist() if not df.empty else []
    return entity_filter("Select Driver ID", drivers, key)

def vehicle_filter():
    # Pull from dim_vehicle to see ALL assets, not just active ones
    query = "SELECT DISTINCT vehicle_id FROM mart.dim_vehicle ORDER BY vehicle_id"
    df = run_query(query)
    return st.selectbox("Select Vehicle", df['vehicle_id'].tolist())