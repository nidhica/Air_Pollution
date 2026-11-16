"""City Comparison page.

The controls read the cached city cards and city series. Compare does not
start Spark and does not read the annotated measurements.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.cities import build_city_cards
from dashboard.comparison import (
    OPTIONAL_CITY,
    comparison_insights,
    comparison_rows,
    daily_lines,
    display_table,
    dropdown_options,
    hourly_lines,
    metric_frame,
    place_label,
    places_from_cards,
    restore_saved_value,
    selected_places,
    selection_error,
)
from dashboard.figures import grouped_metric_bars, multi_city_lines
from dashboard.loading import AnalyticsBundle, CityPortal


def render_city_comparison(bundle: AnalyticsBundle, portal: CityPortal) -> None:
    cards = build_city_cards(bundle.stations, bundle.cities)
    places = places_from_cards(cards)
    labels = [place_label(city_name, state_name) for city_name, state_name in places]
    st.title("City Comparison")
    window = _saved_window_text(portal.daily)
    st.caption(
        f"Compare two or three cities from the {window} CPCB CAAQM PM2.5 and PM10 records. "
        "Averages stay inside each pollutant. The national daily and hourly tables are not used."
    )
    if len(labels) < 2:
        st.info("At least two cities with a recorded name are required.")
        return

    chosen = _city_controls(labels)
    start, end, full_start, full_end = _date_controls(portal.daily)
    st.session_state["comparison_city_labels"] = chosen
    st.session_state["comparison_dates"] = (start, end)
    st.caption(
        f"The saved measurements cover {window}. The date range is applied to every selected city."
    )
    if st.button("Compare", type="primary"):
        resolved = selected_places(labels, places, chosen)
        error = selection_error(resolved)
        st.session_state.comparison_error = error
        st.session_state.comparison_places = None if error else resolved
    error = st.session_state.get("comparison_error")
    if error:
        st.error(error)
    active = st.session_state.get("comparison_places")
    if not active:
        st.info("Choose two or three cities, then select Compare.")
        return
    _results(portal, bundle.stations, bundle.cities, active, start, end, full_start, full_end)


def _city_controls(labels: list[str]) -> list[str | None]:
    _restore_cities(labels)
    current = [
        st.session_state.get("compare_city_1"),
        st.session_state.get("compare_city_2"),
        st.session_state.get("compare_city_3"),
    ]
    columns = st.columns(3)
    first = columns[0].selectbox(
        "City 1",
        dropdown_options(labels, current, 0),
        key="compare_city_1",
    )
    current[0] = first
    second_options = dropdown_options(labels, current, 1)
    # index 0 is not treated as a widget default, so a restored session value stays quiet.
    second_index = 1 if "compare_city_2" not in st.session_state and len(second_options) > 1 else 0
    second = columns[1].selectbox(
        "City 2",
        second_options,
        index=second_index,
        key="compare_city_2",
    )
    current[1] = second
    third = columns[2].selectbox(
        "City 3",
        dropdown_options(labels, current, 2, optional=True),
        key="compare_city_3",
    )
    return [first, second, third]


def _restore_cities(labels: list[str]) -> None:
    """Keep the last city choices when the page is opened again in this session."""

    saved = st.session_state.get("comparison_city_labels")
    if not isinstance(saved, list):
        return
    allowed = set(labels) | {OPTIONAL_CITY}
    padded = [*saved, None, None, None]
    for key, value in zip(("compare_city_1", "compare_city_2", "compare_city_3"), padded[:3]):
        if value in allowed:
            restore_saved_value(st.session_state, key, value)


def _saved_window_text(daily: pd.DataFrame) -> str:
    """Label the dates actually stored in the city series."""

    start = pd.Timestamp(daily["observation_date"].min()).date()
    end = pd.Timestamp(daily["observation_date"].max()).date()
    if start.year == end.year and start.month == end.month:
        return f"{start.day}–{end.day} {start.strftime('%B %Y')}"
    return f"{start.isoformat()} to {end.isoformat()}"


def _date_controls(daily: pd.DataFrame) -> tuple[object, object, object, object]:
    start_default = pd.Timestamp(daily["observation_date"].min()).date()
    end_default = pd.Timestamp(daily["observation_date"].max()).date()
    saved_dates = st.session_state.get("comparison_dates")
    if isinstance(saved_dates, tuple) and len(saved_dates) == 2:
        restore_saved_value(st.session_state, "compare_dates", saved_dates)
    chosen = st.date_input(
        "Date range",
        value=(start_default, end_default),
        min_value=start_default,
        max_value=end_default,
        key="compare_dates",
    )
    start, end = _date_pair(chosen, start_default, end_default)
    return start, end, start_default, end_default


def _results(
    portal: CityPortal,
    stations: pd.DataFrame,
    city_metrics: pd.DataFrame,
    places: list[tuple[str, str | None]],
    start: object,
    end: object,
    full_start: object,
    full_end: object,
) -> None:
    rows = comparison_rows(portal.daily, places, stations, city_metrics, start, end)
    order = [row["label"] for row in rows]
    st.subheader("Comparison summary")
    st.caption(
        "PM2.5 and PM10 averages are calculated separately. "
        "Monitoring stations are the saved station count for the whole analyzed window. "
        "A missing pollutant is shown as not available."
    )
    st.dataframe(display_table(rows), hide_index=True, width="stretch")
    st.plotly_chart(
        grouped_metric_bars(
            metric_frame(rows, "mean"),
            title="Average pollutant concentration",
            y_title="Mean valid concentration (µg/m³)",
            order=order,
        ),
        width="stretch",
    )
    st.plotly_chart(
        grouped_metric_bars(
            metric_frame(rows, "spike_count"),
            title="Candidate spike counts",
            y_title="Candidate spikes",
            order=order,
        ),
        width="stretch",
    )
    st.plotly_chart(
        grouped_metric_bars(
            metric_frame(rows, "rate"),
            title="Eligible spike rate",
            y_title="Eligible spike rate",
            order=order,
            as_percent=True,
        ),
        width="stretch",
    )
    restore_saved_value(st.session_state, "compare_trend_pollutant", st.session_state.get("comparison_trend"))
    pollutant = st.radio("Trend pollutant", ["PM2.5", "PM10"], horizontal=True, key="compare_trend_pollutant")
    st.session_state["comparison_trend"] = pollutant
    daily = daily_lines(portal.daily, places, str(pollutant), start, end)
    hourly = hourly_lines(
        portal.hourly,
        portal.hour_of_day,
        places,
        str(pollutant),
        start,
        end,
        full_start,
        full_end,
    )
    st.plotly_chart(
        multi_city_lines(
            daily,
            x="observation_date",
            y="mean_valid_value",
            title=f"Daily {pollutant} average",
            x_title="Date",
            y_title="Mean valid concentration (µg/m³)",
            order=order,
        ),
        width="stretch",
    )
    st.plotly_chart(
        multi_city_lines(
            hourly,
            x="observation_hour",
            y="mean_valid_value",
            title=f"Hour-of-day {pollutant} average",
            x_title="Hour of day",
            y_title="Mean valid concentration (µg/m³)",
            order=order,
        ),
        width="stretch",
    )
    st.subheader("Analytical notes")
    for line in comparison_insights(rows):
        st.markdown(f"- {line}")


def _date_pair(value: object, start_default: object, end_default: object) -> tuple[object, object]:
    if isinstance(value, (tuple, list)):
        if len(value) == 2 and value[0] is not None and value[1] is not None:
            return value[0], value[1]
        if len(value) == 1 and value[0] is not None:
            return value[0], value[0]
    return start_default, end_default
