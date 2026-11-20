"""Show which months are processed and which were never downloaded."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

from dashboard.coverage import (
    classify_months,
    coverage_frame,
    measurement_month_dirs,
    partial_month_dirs,
    prepared_observation_days,
    remote_month_status,
)
from dashboard.figures import coverage_heatmap
from dashboard.timeline import load_timeline


def render_coverage_page(root: Path) -> None:
    st.title("Data coverage and quality")
    st.caption(
        "“Not downloaded” means the month is absent locally and the XKDR file list has not confirmed it. "
        "“Available remotely, not downloaded” means GET /v1/files listed it. "
        "“Unavailable from the source” is used only when a retrieved file list omits that month. "
        "AQI_API_KEY is required before the file list can be requested. "
        "March 2026 in the raw tree is outside this CPCB window."
    )
    catalog = load_timeline(root)
    days = prepared_observation_days(catalog.xkdr_daily)
    months = classify_months(
        measurement_month_dirs(root),
        days,
        remote=remote_month_status(root),
        partial=partial_month_dirs(root),
    )
    frame = coverage_frame(months)
    if catalog.xkdr.start is None:
        st.info("No prepared XKDR daily series is stored.")
    else:
        st.markdown(
            f"Prepared XKDR concentrations run from **{catalog.xkdr.start.strftime('%Y-%m-%d')}** "
            f"to **{catalog.xkdr.end.strftime('%Y-%m-%d')}** ({catalog.xkdr.unit}). "
            f"{len(catalog.xkdr.days)} distinct days have a daily aggregate."
        )
    if catalog.gov.start is None:
        st.info("No prepared data.gov.in observations are stored.")
    else:
        missing = int(catalog.gov_observations["avg_value"].isna().sum()) if "avg_value" in catalog.gov_observations else 0
        st.markdown(
            f"Prepared data.gov.in values run from **{catalog.gov.start.strftime('%Y-%m-%d')}** "
            f"to **{catalog.gov.end.strftime('%Y-%m-%d')}** ({catalog.gov.unit}). "
            f"Missing averages: {missing:,} of {len(catalog.gov_observations):,} rows. "
            "Hourly collection cannot reconstruct hours from before the first stored snapshot."
        )
    st.plotly_chart(coverage_heatmap(frame), width="stretch")
    if not frame.empty:
        st.dataframe(
            frame.rename(
                columns={
                    "year": "Year",
                    "month": "Month",
                    "label": "Status",
                    "days_with_observations": "Days with observations",
                    "days_in_month": "Days in month",
                }
            ),
            hide_index=True,
            width="stretch",
        )
    _quality(catalog)


def _quality(catalog: object) -> None:
    measured = catalog.xkdr_measurements
    if measured.empty or "value" not in measured.columns:
        return
    values = pd.to_numeric(measured["value"], errors="coerce")
    stamps = (
        pd.to_datetime(measured["collected_at"], errors="coerce")
        if "collected_at" in measured
        else pd.Series(dtype="datetime64[ns]")
    )
    rows = len(measured)
    missing = int(values.isna().sum())
    negative = int((values < 0).sum())
    numeric = values.to_numpy(dtype="float64", na_value=np.nan)
    infinite = int(np.isinf(numeric).sum()) if rows else 0
    duplicates = _duplicate_rows(measured, ["station_id", "parameter_name", "collected_at"])
    stations = int(measured["station_id"].nunique(dropna=True)) if "station_id" in measured else 0
    st.subheader("Prepared XKDR measurement checks")
    st.caption(
        "Negative, missing, and non-numeric values stay out of the valid-average denominator. "
        "They are not replaced with zero. Candidate spikes are a statistical flag, not a CPCB exceedance. "
        "These counts are the prepared CPCB PM2.5 and PM10 rows, not a selected timeline."
    )
    columns = st.columns(4)
    columns[0].metric("Prepared rows", f"{rows:,}")
    columns[1].metric("Missing values", f"{missing:,} ({_percent(missing, rows)})")
    columns[2].metric("Negative values", f"{negative:,}")
    columns[3].metric("Duplicate timestamp keys", f"{duplicates:,}")
    extra = st.columns(4)
    extra[0].metric("Stations", f"{stations:,}")
    extra[1].metric("Infinite values", f"{infinite:,}")
    extra[2].metric("Distinct timestamps", f"{int(stamps.nunique(dropna=True)):,}")
    latest = None if stamps.dropna().empty else stamps.max()
    extra[3].metric("Latest observation", "not available" if latest is None else str(latest)[:16])
    missing_city = int(measured["city_name"].isna().sum()) if "city_name" in measured else 0
    missing_state = int(measured["state_name"].isna().sum()) if "state_name" in measured else 0
    st.caption(
        f"Rows without a city: {missing_city:,} ({_percent(missing_city, rows)}). "
        f"Rows without a state: {missing_state:,} ({_percent(missing_state, rows)})."
    )
    _named_counts(measured, "parameter_name", "Observations by pollutant")
    _named_counts(measured, "state_name", "Observations by state")
    _named_counts(measured, "city_name", "Observations by city")
    if not stamps.dropna().empty:
        monthly = (
            stamps.dt.to_period("M").value_counts().sort_index().rename_axis("Month").reset_index(name="Observations")
        )
        monthly["Month"] = monthly["Month"].astype(str)
        st.subheader("Monthly observation counts")
        st.caption("A month absent from this table has no prepared CPCB row. It may still be undownloaded.")
        st.dataframe(monthly, hide_index=True, width="stretch")
    _gov_quality(catalog.gov_observations)


def _gov_quality(frame: pd.DataFrame) -> None:
    st.subheader("Prepared data.gov.in checks")
    if frame.empty:
        st.info("No prepared data.gov.in observations are stored.")
        return
    stamps = pd.to_datetime(frame["observed_at"], errors="coerce") if "observed_at" in frame else pd.Series(dtype="datetime64[ns]")
    averages = pd.to_numeric(frame["avg_value"], errors="coerce") if "avg_value" in frame else pd.Series(dtype="float64")
    missing = int(averages.isna().sum()) if len(averages) else 0
    identity = ["state", "city", "station", "pollutant_id"]
    for column in ("last_update_text", "last_update", "observed_at"):
        if column in frame.columns:
            identity.append(column)
            break
    duplicates = _duplicate_rows(frame, identity)
    columns = st.columns(4)
    columns[0].metric("Prepared rows", f"{len(frame):,}")
    columns[1].metric("Missing averages", f"{missing:,} ({_percent(missing, len(frame))})")
    columns[2].metric("Duplicate identities", f"{duplicates:,}")
    columns[3].metric("Distinct API timestamps", f"{int(stamps.nunique(dropna=True)):,}")
    latest = None if stamps.dropna().empty else stamps.max()
    st.caption(
        "Freshness uses the API last_update stored as observed_at. "
        f"Latest API timestamp: {'not available' if latest is None else str(latest)[:16]}. "
        "The local retrieval time is not a measurement time. "
        "The unit is not documented by this API."
    )
    _named_counts(frame, "pollutant_id", "data.gov.in observations by pollutant")
    _named_counts(frame, "state", "data.gov.in observations by state")
    _named_counts(frame, "city", "data.gov.in observations by city")


def _named_counts(frame: pd.DataFrame, column: str, title: str) -> None:
    if column not in frame.columns or frame.empty:
        return
    counts = (
        frame[column]
        .fillna("(missing)")
        .astype(str)
        .replace("", "(missing)")
        .value_counts(dropna=False)
        .rename_axis(column)
        .reset_index(name="Observations")
    )
    st.subheader(title)
    st.dataframe(counts, hide_index=True, width="stretch")


def _duplicate_rows(frame: pd.DataFrame, columns: list[str]) -> int:
    present = [column for column in columns if column in frame.columns]
    if len(present) != len(columns) or frame.empty:
        return 0
    return int(frame.duplicated(present).sum())


def _percent(part: int, whole: int) -> str:
    if whole <= 0:
        return "0%"
    return f"{100.0 * part / whole:.2f}%"
