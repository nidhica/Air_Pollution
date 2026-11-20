"""Recent data.gov.in snapshot page.

The page reads prepared observations. It does not call the API, rescore
measurements, or read the XKDR city series.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.figures import reported_point_map, snapshot_trend
from dashboard.loading import DashboardDataError
from dashboard.recent_data import (
    PROJECT_POLLUTANTS,
    at_timestamp,
    filtered_observations,
    latest_moment,
    latest_retrieval,
    load_pipeline_status,
    load_recent,
    pipeline_status_text,
    trend_frame,
    trend_unavailable_message,
)

_ALL_STATIONS = "All stations"


def render_recent_page(root: Path) -> None:
    st.title("Recent air quality snapshots")
    st.caption(
        "This page shows snapshots retrieved from data.gov.in. "
        "It is not a live stream. Each row keeps the API last_update and the time this project stored it."
    )
    st.caption(pipeline_status_text(load_pipeline_status(root)))
    try:
        recent = load_recent(root)
    except DashboardDataError as exc:
        st.error(str(exc))
        return
    if recent is None:
        st.info(
            "No prepared data.gov.in snapshot is stored yet. "
            "Run `fetch-data-gov` and then `prepare-data-gov`."
        )
        return
    observations = recent.observations
    report = recent.report
    moment = latest_moment(observations)
    retrieval = latest_retrieval(observations)
    left, middle, right = st.columns(3)
    left.metric("Latest API last_update", _clock(moment))
    middle.metric("Latest local retrieval", retrieval or "not available")
    right.metric("Stored readings", f"{len(observations):,}")
    st.caption(
        "The API did not document a measurement unit for min_value, max_value, or avg_value. "
        f"These figures are {report.get('value_kind', 'API-reported values')} "
        f"and the stored unit label is “{report.get('reported_unit', 'not documented by the API')}”. "
        "They are not combined with XKDR concentrations."
    )
    state, city, station, pollutant = _filters(observations)
    selected = filtered_observations(
        observations,
        state=state,
        city=city,
        station=None if station == _ALL_STATIONS else station,
        pollutant=pollutant,
    )
    current = at_timestamp(selected, latest_moment(selected))
    _values(current, station)
    located = current.loc[current["coordinate_status"].eq("valid")] if "coordinate_status" in current.columns else current
    st.plotly_chart(
        reported_point_map(
            located,
            value="avg_value",
            title=f"{pollutant} stations in {city}",
            value_title="API-reported average",
        ),
        width="stretch",
    )
    missing = current.loc[~current["coordinate_status"].eq("valid")] if "coordinate_status" in current.columns else current.iloc[0:0]
    if missing.empty:
        st.caption("Every station in this selection has coordinates inside the valid latitude and longitude range.")
    else:
        st.caption(f"{len(missing):,} rows in this selection do not have a valid coordinate and are not on the map.")
    _trend(selected, station, pollutant)


def _filters(frame: pd.DataFrame) -> tuple[str, str, str, str]:
    states = _choices(frame["state"])
    state = st.selectbox("State", states, key="recent_state")
    cities = _choices(frame.loc[frame["state"].eq(state), "city"])
    city = _select("City", cities, "recent_city")
    stations = [_ALL_STATIONS, *_choices(frame.loc[frame["state"].eq(state) & frame["city"].eq(city), "station"])]
    station = _select("Station", stations, "recent_station")
    pollutant_rows = frame.loc[frame["state"].eq(state) & frame["city"].eq(city)]
    if station != _ALL_STATIONS:
        pollutant_rows = pollutant_rows.loc[pollutant_rows["station"].eq(station)]
    pollutants = [item for item in _choices(pollutant_rows["pollutant_id"]) if item in PROJECT_POLLUTANTS]
    if "PM2.5" in pollutants:
        pollutants = ["PM2.5", *[item for item in pollutants if item != "PM2.5"]]
    pollutant = _select("Pollutant", pollutants, "recent_pollutant")
    return state, city, station, pollutant


def _select(label: str, options: list[str], key: str) -> str:
    if not options:
        st.warning(f"No {label.lower()} is stored for this selection.")
        return ""
    if key in st.session_state and st.session_state[key] not in options:
        st.session_state[key] = options[0]
    return st.selectbox(label, options, key=key)


def _choices(series: pd.Series) -> list[str]:
    return sorted(str(value) for value in series.dropna().unique())


def _values(frame: pd.DataFrame, station: str) -> None:
    st.subheader("Reported values")
    st.caption("Minimum, maximum, and average are the three numbers the API returned. Missing numbers stay blank.")
    if frame.empty:
        st.info("No stored row matches this state, city, station, and pollutant.")
        return
    if station == _ALL_STATIONS:
        display = frame.loc[:, ["station", "min_value", "max_value", "avg_value", "last_update_text"]].rename(
            columns={
                "station": "Station",
                "min_value": "Reported minimum",
                "max_value": "Reported maximum",
                "avg_value": "Reported average",
                "last_update_text": "API last_update",
            }
        )
        st.dataframe(display, hide_index=True, width="stretch")
        return
    row = frame.iloc[0]
    columns = st.columns(3)
    columns[0].metric("Reported minimum", _number(row.get("min_value")))
    columns[1].metric("Reported maximum", _number(row.get("max_value")))
    columns[2].metric("Reported average", _number(row.get("avg_value")))
    st.caption(f"{row.get('station')} · API last_update {row.get('last_update_text')}")


def _trend(frame: pd.DataFrame, station: str, pollutant: str) -> None:
    st.subheader("Stored trend")
    if station == _ALL_STATIONS:
        st.info("Choose one station and pollutant. A trend uses API last_update and needs two different source timestamps.")
        return
    series = trend_frame(frame)
    if series is None:
        st.info(trend_unavailable_message(frame, pollutant))
        return
    st.plotly_chart(snapshot_trend(series, title=f"{station} · {pollutant}"), width="stretch")
    st.caption(
        "The time axis is the API last_update. Minimum, maximum, and average are separate lines. "
        "Missing values stay missing, and fetching the same hour again does not add a point."
    )


def _clock(moment: pd.Timestamp | None) -> str:
    if moment is None or pd.isna(moment):
        return "not available"
    return pd.Timestamp(moment).strftime("%d %b %Y %H:%M")


def _number(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{float(value):,.2f}"
