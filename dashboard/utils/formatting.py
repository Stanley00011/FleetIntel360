# dashboard/utils/formatting.py

from typing import Optional

# Severity mappings

SEVERITY_ORDER = {
    "INFO": 0,
    "WARNING": 1,
    "CRITICAL": 2,
}

SEVERITY_COLORS = {
    "INFO": "#4CAF50",       # green
    "WARNING": "#FFC107",    # amber
    "CRITICAL": "#F44336",   # red
}

SEVERITY_EMOJIS = {
    "INFO": "ℹ️",
    "WARNING": "⚠️",
    "CRITICAL": "🚨",
}


def normalize_severity(severity: Optional[str]) -> str:
    """
    Normalize severity values coming from SQL / alerts.
    """
    if severity is None:
        return "INFO"
    return severity.strip().upper()


def severity_badge(severity: str) -> str:
    """
    Return emoji + severity label for tables.
    """
    sev = normalize_severity(severity)
    emoji = SEVERITY_EMOJIS.get(sev, "")
    return f"{emoji} {sev}"


def severity_color(severity: str) -> str:
    """
    Return hex color for severity-based styling.
    """
    sev = normalize_severity(severity)
    return SEVERITY_COLORS.get(sev, "#9E9E9E")


# KPI formatting helpers
def format_percentage(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.{decimals}f}%"


def format_float(value: Optional[float], decimals: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:.{decimals}f}"


def format_int(value: Optional[int]) -> str:
    if value is None:
        return "—"
    return f"{value:,}"


def format_currency(value: Optional[float], currency: str = "$") -> str:
    if value is None:
        return "—"
    return f"{currency}{value:,.2f}"


# Domain-specific helpers

def format_hours(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:.1f} hrs"


def format_temperature(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:.1f}°C"


def format_fatigue_index(value: Optional[float]) -> str:
    """
    Fatigue index is usually 0–1.
    """
    if value is None:
        return "—"
    return f"{value:.2f}"

def format_timestamp(ts) -> str:
    """
    Cleaner for those timestamps we just fixed.
    If it's NaT or NULL, it returns 'NEVER' in a neutral gray.
    """
    if ts is None or pd.isna(ts):
        return "NEVER"
    # Returns 17 Jan 13:15 format
    return ts.strftime("%d %b %H:%M")

def format_activity_date(dt):
    if pd.isna(dt) or dt is None:
        return "NEVER REPORTED"
    return dt.strftime("%Y-%m-%d %H:%M")
