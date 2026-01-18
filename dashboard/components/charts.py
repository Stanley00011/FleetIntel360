# dashboard/components/charts.py

import streamlit as st
import pandas as pd
import altair as alt

from dashboard.utils.formatting import (
    SEVERITY_COLORS,
    format_number
)

# KPI Card
def kpi_card(label: str, value, delta=None):
    """
    Standard KPI metric card.
    """
    st.metric(
        label=label,
        value=format_number(value),
        delta=format_number(delta) if delta is not None else None
    )


# Time series chart
def time_series(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str = None,
    title: str = ""
):
    """
    Generic time series line chart.
    """
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(x, title="Date"),
            y=alt.Y(y, title=y.replace("_", " ").title()),
            color=color
        )
        .properties(title=title)
    )

    st.altair_chart(chart, use_container_width=True)


# Severity-based bar chart
def severity_bar(
    df: pd.DataFrame,
    x: str,
    y: str,
    severity_col: str,
    title: str = "",
    limit: int = 10
):
    """
    Bar chart colored by severity.
    """
    df = df.sort_values(y, ascending=False).head(limit)

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(x, sort="-y", title=x.replace("_", " ").title()),
            y=alt.Y(y, title=y.replace("_", " ").title()),
            color=alt.Color(
                severity_col,
                scale=alt.Scale(
                    domain=list(SEVERITY_COLORS.keys()),
                    range=list(SEVERITY_COLORS.values())
                ),
                legend=alt.Legend(title="Severity")
            ),
            tooltip=list(df.columns)
        )
        .properties(title=title)
    )

    st.altair_chart(chart, use_container_width=True)


# Distribution chart
def distribution(
    df: pd.DataFrame,
    column: str,
    title: str = ""
):
    """
    Histogram for distributions (profit, cost, etc.)
    """
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(column, bin=True, title=column.replace("_", " ").title()),
            y=alt.Y("count()", title="Count")
        )
        .properties(title=title)
    )

    st.altair_chart(chart, use_container_width=True)


# Alert timeline
def alert_timeline(
    df: pd.DataFrame,
    time_col: str,
    entity_col: str,
    severity_col: str,
    title: str = ""
):
    """
    Timeline of alerts over time.
    """
    chart = (
        alt.Chart(df)
        .mark_circle(size=120)
        .encode(
            x=alt.X(time_col, title="Time"),
            y=alt.Y(entity_col, title="Entity"),
            color=alt.Color(
                severity_col,
                scale=alt.Scale(
                    domain=list(SEVERITY_COLORS.keys()),
                    range=list(SEVERITY_COLORS.values())
                )
            ),
            tooltip=[
                entity_col,
                severity_col,
                "metric_name",
                "metric_value",
                "description"
            ]
        )
        .properties(title=title)
    )

    st.altair_chart(chart, use_container_width=True)
