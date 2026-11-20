"""One date range, then the charts each source can support.

Prepared CPCB dates come from the local dataset. A daily XKDR query is sent
only for the most recent 7 days in Asia/Kolkata when those dates are inside
the published archive. Months after that archive are not requested.
data.gov.in stays in its own section. This page does not start Spark, does
not download bulk files, and does not call data.gov.in.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from dashboard.figures import (
    NO_PREPARED_OBSERVATIONS,
    grouped_metric_bars,
    hour_bars,
    missing_observation_periods,
    ranked_bars,
    recent_avg_series,
    reported_point_map,
    time_series,
)
from dashboard.live_daily import (
    HISTORICAL_LABEL,
    LIVE_LABEL,
    LIVE_SECTION_CAPTION,
    ON_DEMAND_HEADING,
    availability_messages,
    kolkata_today,
    period_start_days,
    plan_sources,
    prepared_bounds,
    present_live_section,
    resolve_timeline_sources,
)
from dashboard.xkdr_archive import ArchiveInfo, load_archive
from dashboard.recent_data import (
    at_timestamp,
    latest_moment,
    load_pipeline_status,
    pipeline_status_text,
    pollutant_choices,
    project_pollutants,
    recent_period,
    recent_period_text,
)
from dashboard.timeline import (
    ALL,
    PRESETS,
    TimelineCatalog,
    assert_same_measurement_scale,
    assess_range,
    coverage_text,
    filter_choices,
    gov_observations,
    gov_summary,
    load_timeline,
    ordered_window,
    prepared_fingerprint,
    preset_bounds,
    sources_can_share_scale,
    xkdr_city_bars,
    xkdr_daily_series,
    xkdr_hour_pattern,
    xkdr_spike_events,
    xkdr_station_map_frame,
    xkdr_station_ranking,
    xkdr_summary,
    IncompatibleMeasurements,
)


def render_timeline_page(root: Path) -> None:
    st.title("Timeline")
    st.caption(
        "Prepared CPCB dates come from the local dataset. "
        "The XKDR daily API is used only for the most recent 7 days in Asia/Kolkata "
        "when those dates are inside the published archive. "
        "XKDR concentrations and data.gov.in API values stay separate."
    )
    catalog = _cached_timeline(str(root.resolve()), prepared_fingerprint(root))
    today = kolkata_today()
    archive = load_archive(project_root=root)
    _preset, start, end = _date_controls(today)
    window = ordered_window(start, end)
    prepared_start, prepared_end = prepared_bounds(catalog)
    plan = plan_sources(
        window.start,
        window.end,
        today=today,
        archive_end=archive.archive_end,
        prepared_start=prepared_start,
        prepared_end=prepared_end,
    )
    gov_match = assess_range(catalog, window.start, window.end)[1]
    historical_match = None
    if plan.historical_start is not None and plan.historical_end is not None:
        historical_match, _gov_for_history = assess_range(catalog, plan.historical_start, plan.historical_end)
    st.markdown(
        f"**Selected dates:** {plan.selected_start.isoformat()} to {plan.selected_end.isoformat()}."
    )
    st.caption(archive.detail)
    st.caption(
        f"Recent window: {plan.live_start.isoformat()} to {plan.live_end.isoformat()} (Asia/Kolkata). "
        "A daily query is sent only for the part of this window inside the published archive."
    )
    if plan.future_start is not None and plan.future_end is not None:
        st.info(
            f"Observations from {plan.future_start.isoformat()} to {plan.future_end.isoformat()} "
            "are not yet available. Those dates were not sent to the XKDR API."
        )
    for message in availability_messages(plan, archive):
        st.info(message)
    if historical_match is not None:
        st.markdown(f"**{HISTORICAL_LABEL}**")
        _show_match(historical_match)
    if plan.live_query_start is not None and plan.live_query_end is not None:
        st.markdown(f"**{ON_DEMAND_HEADING}**")
        st.caption(LIVE_LABEL)
        st.caption(
            f"Requested {plan.live_query_start.isoformat()} to {plan.live_query_end.isoformat()}."
        )
    if gov_match.status == "none":
        st.info(coverage_text(gov_match))
    elif gov_match.status == "partial":
        st.warning(coverage_text(gov_match))
    else:
        st.caption(coverage_text(gov_match))
    prepared_status = "none" if historical_match is None else historical_match.status
    archive_only = plan.mode in {"future", "unpublished", "unprepared", "unconfirmed"}
    if plan.live_query_start is None and prepared_status == "none" and gov_match.status == "none":
        if not archive_only:
            st.warning(
                "No prepared observations fall inside this period. "
                "The dates above were not replaced with another period."
            )
        return
    if historical_match is not None and sources_can_share_scale(historical_match, gov_match):
        st.caption("Both sources in this period use the same measurement definition.")
    elif historical_match is not None and historical_match.status != "none" and gov_match.status != "none":
        try:
            assert_same_measurement_scale(
                historical_match.unit,
                gov_match.unit,
                historical_match.value_kind,
                gov_match.value_kind,
            )
        except IncompatibleMeasurements as exc:
            st.info(str(exc))
    place_start = plan.historical_start or plan.live_query_start or plan.selected_start
    place_end = plan.historical_end or plan.live_query_end or plan.selected_end
    state, city, station, pollutant = _place_controls(
        catalog,
        pd.Timestamp(place_start),
        pd.Timestamp(place_end),
        live_pollutants=plan.live_query_start is not None,
    )
    if plan.historical_start is not None and plan.historical_end is not None:
        _xkdr_section(
            catalog,
            pd.Timestamp(plan.historical_start),
            pd.Timestamp(plan.historical_end),
            state,
            city,
            station,
            pollutant,
            excludes_live_from=plan.live_query_start,
        )
    for tail_start, tail_end in plan.historical_tail:
        _xkdr_section(
            catalog,
            pd.Timestamp(tail_start),
            pd.Timestamp(tail_end),
            state,
            city,
            station,
            pollutant,
        )
    if plan.live_query_start is not None and plan.live_query_end is not None:
        _live_daily_section(
            root,
            catalog,
            pd.Timestamp(plan.live_query_start),
            pd.Timestamp(plan.live_query_end),
            state,
            city,
            station,
            pollutant,
            today=today,
            archive=archive,
        )
    if gov_match.status != "none":
        _gov_section(root, catalog, window.start, window.end, state, city, station, pollutant)


@st.cache_data(show_spinner="Loading prepared observations")
def _cached_timeline(root_text: str, fingerprint: tuple[tuple[str, int, int], ...]) -> TimelineCatalog:
    del fingerprint
    return load_timeline(Path(root_text))


def _date_controls(today: date) -> tuple[str, date, date]:
    preset = st.sidebar.radio("Period", PRESETS, key="timeline-preset")
    bounds = preset_bounds(preset, today)
    if bounds is not None:
        st.sidebar.caption(
            "This preset uses the same source split as a custom range. "
            "It includes today and does not expand past that date."
        )
        return preset, bounds[0], bounds[1]
    picked = st.sidebar.date_input(
        "Custom dates",
        value=(today - timedelta(days=6), today),
        key="timeline-custom",
    )
    start, end = _pair(picked, today)
    return preset, start, end


def _place_controls(
    catalog: TimelineCatalog,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    live_pollutants: bool = False,
) -> tuple[str, str, str, str]:
    state_options = filter_choices(catalog, start, end)["state"]
    state = _select("State", state_options, "timeline-state")
    city_options = filter_choices(catalog, start, end, state=state)["city"]
    city = _select("City", city_options, "timeline-city")
    narrowed = filter_choices(catalog, start, end, state=state, city=city)
    station = _select("Station", narrowed["station"], "timeline-station")
    pollutant_options = pollutant_choices(narrowed["pollutant"])
    if live_pollutants:
        pollutant_options = pollutant_choices(_with_live_pollutants(pollutant_options))
    pollutant = _select("Pollutant", pollutant_options, "timeline-pollutant")
    st.sidebar.caption(
        "State, city, and station lists contain places stored inside the selected dates. "
        "The pollutant list offers All, PM2.5, and PM10."
    )
    if live_pollutants:
        st.sidebar.caption("PM2.5 and PM10 can be requested for the recent XKDR daily query.")
    return state, city, station, pollutant


def _select(label: str, options: list[str], key: str) -> str:
    if not options:
        options = [ALL]
    if st.session_state.get(key) not in options:
        st.session_state[key] = options[0]
    return str(st.selectbox(label, options, key=key))


def _show_match(match: object) -> None:
    status = getattr(match, "status", "none")
    text = coverage_text(match)  # type: ignore[arg-type]
    if status == "none":
        st.info(text)
    elif status == "partial":
        st.warning(text)
    else:
        st.caption(text)


def _live_daily_section(
    root: Path,
    catalog: TimelineCatalog,
    start: pd.Timestamp,
    end: pd.Timestamp,
    state: str,
    city: str,
    station: str,
    pollutant: str,
    *,
    today: date,
    archive: ArchiveInfo,
) -> None:
    with st.spinner("Retrieving XKDR daily measurements"):
        resolved = resolve_timeline_sources(
            project_root=root,
            catalog=catalog,
            start=start,
            end=end,
            today=today,
            pollutant=pollutant,
            state=state,
            city=city,
            station=station,
            archive=archive,
        )
    result = resolved.live
    view = present_live_section(result, start, end)
    st.header(ON_DEMAND_HEADING)
    st.caption(LIVE_LABEL)
    st.caption(LIVE_SECTION_CAPTION)
    if view.status == "failed":
        st.error(view.message)
        return
    if view.status == "empty":
        st.info(view.message)
        return
    st.caption(view.message)
    _live_cards(result.rows)
    if not result.rows.empty:
        observed_days = period_start_days(result.rows["period_start"]).dropna()
        if not observed_days.empty:
            first = pd.Timestamp(observed_days.min()).strftime("%Y-%m-%d")
            last = pd.Timestamp(observed_days.max()).strftime("%Y-%m-%d")
            st.caption(f"Returned observation dates: {first} to {last}.")
    if view.show_chart:
        observed = int(result.chart["observations"].sum())
        st.plotly_chart(
            time_series(
                result.chart,
                y="mean_valid_value",
                title=_chart_title("Daily mean of station-level aggregates", start, end, observed),
                y_title=_reported_unit(result.rows),
                source=LIVE_LABEL,
                place=_place_label(state, city, station),
                coverage_window=(start, end),
            ),
            width="stretch",
        )
        if missing_observation_periods(result.chart["observation_date"], start, end):
            st.caption(
                "The daily query did not return a station aggregate for every selected date. "
                "Missing dates were not filled."
            )
    if result.rows.empty:
        st.info(result.detail)
        return
    st.dataframe(_api_table(result.rows), hide_index=True, width="stretch")


def _xkdr_section(
    catalog: TimelineCatalog,
    start: pd.Timestamp,
    end: pd.Timestamp,
    state: str,
    city: str,
    station: str,
    pollutant: str,
    *,
    excludes_live_from: date | None = None,
) -> None:
    st.header(HISTORICAL_LABEL)
    st.caption(f"Selected historical dates: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}.")
    st.caption(
        f"Concentrations are in {catalog.xkdr.unit}. "
        "Candidate spikes, hour-of-day patterns, city comparisons, and maps use prepared local hourly observations. "
        "They are not calculated from daily API aggregates. "
        "The baseline is not recalculated here. "
        "A candidate spike is not an official CPCB exceedance. "
        "A line stops where the next stored day is more than one day away."
    )
    if excludes_live_from is not None:
        st.caption(
            f"These prepared charts stop on {end.strftime('%Y-%m-%d')}. "
            f"They do not include live observations from {excludes_live_from.isoformat()} onward."
        )
    summary = xkdr_summary(
        catalog, start, end, state=state, city=city, station=station, pollutant=pollutant
    )
    _cards(summary, extra=("Candidate spikes", _count(summary["spike_count"])))
    if int(summary["observations"]) == 0:
        st.info("No XKDR rows match this state, city, station, and pollutant inside the selected dates.")
        return
    daily = xkdr_daily_series(
        catalog, start, end, state=state, city=city, station=station, pollutant=pollutant
    )
    place = _place_label(state, city, station)
    observed = int(summary["observations"])
    st.plotly_chart(
        time_series(
            daily,
            y="mean_valid_value",
            title=_chart_title("Daily average of valid concentrations", start, end, observed),
            y_title=catalog.xkdr.unit,
            source=HISTORICAL_LABEL,
            place=place,
            coverage_window=(start, end),
        ),
        width="stretch",
    )
    if "observation_date" in daily.columns and missing_observation_periods(daily["observation_date"], start, end):
        st.caption(NO_PREPARED_OBSERVATIONS)
    st.plotly_chart(
        time_series(
            daily,
            y="spike_count",
            title=_chart_title("Daily candidate spikes", start, end, observed),
            y_title="Stored candidate spikes",
            source=HISTORICAL_LABEL,
            place=place,
        ),
        width="stretch",
    )
    hours = xkdr_hour_pattern(
        catalog, start, end, state=state, city=city, station=station, pollutant=pollutant
    )
    st.plotly_chart(
        hour_bars(
            hours,
            y="mean_valid_value",
            title=_chart_title("Hour-of-day average of valid concentrations", start, end, observed),
            y_title=catalog.xkdr.unit,
        ),
        width="stretch",
    )
    if city == ALL and station == ALL:
        bars = xkdr_city_bars(catalog, start, end, state=state, pollutant=pollutant)
        if not bars.empty:
            order = list(dict.fromkeys(bars["label"].tolist()))
            st.plotly_chart(
                grouped_metric_bars(
                    bars,
                    title=_chart_title("City average concentration", start, end, observed),
                    y_title=catalog.xkdr.unit,
                    order=order,
                ),
                width="stretch",
            )
    ranking = xkdr_station_ranking(catalog, start, end, state=state, city=city, pollutant=pollutant)
    if station != ALL and not ranking.empty:
        ranking = ranking.loc[ranking["station_id"].astype(str).eq(station)]
    if not ranking.empty:
        chart = ranking.copy()
        chart["label"] = chart["station_id"].astype(str) + " · " + chart["parameter_name"].astype(str)
        st.plotly_chart(
            ranked_bars(
                chart,
                label="label",
                value="mean_valid_value",
                title=_chart_title("Highest station mean concentrations", start, end, observed),
                x_title=catalog.xkdr.unit,
            ),
            width="stretch",
        )
        st.caption("Station means use stored concentrations in the selected dates. Spike flags are not rescored.")
    map_pollutants = [pollutant] if pollutant != ALL else ["PM2.5", "PM10"]
    for name in map_pollutants:
        located = xkdr_station_map_frame(catalog, start, end, state=state, city=city, pollutant=name)
        if station != ALL and not located.empty:
            located = located.loc[located["station"].astype(str).eq(station)]
        if located.empty:
            continue
        st.plotly_chart(
            reported_point_map(
                located,
                value="avg_value",
                title=_chart_title(f"{name} stations", start, end, observed),
                value_title=f"Mean {catalog.xkdr.unit}",
            ),
            width="stretch",
        )
    events = xkdr_spike_events(
        catalog, start, end, state=state, city=city, station=station, pollutant=pollutant
    )
    st.subheader("Candidate spikes")
    st.caption(f"{len(events):,} stored spike rows match this period and the current filters.")
    if not events.empty:
        st.dataframe(_spike_table(events), hide_index=True, width="stretch")


def _gov_section(
    root: Path,
    catalog: TimelineCatalog,
    start: pd.Timestamp,
    end: pd.Timestamp,
    state: str,
    city: str,
    station: str,
    pollutant: str,
) -> None:
    st.header(catalog.gov.label)
    st.caption(pipeline_status_text(load_pipeline_status(root)))
    st.caption(
        f"min_value, max_value, and avg_value use the stored unit “{catalog.gov.unit}”. "
        "They are not labelled µg/m³ and they are not averaged with XKDR concentrations. "
        "The time axis is the API last_update."
    )
    rows = project_pollutants(
        gov_observations(
            catalog, start, end, state=state, city=city, station=station, pollutant=pollutant
        )
    )
    summary = gov_summary(rows, unit=catalog.gov.unit, start=start, end=end)
    _cards(summary, extra=("Missing averages", _count(summary["missing_values"])))
    period = recent_period(rows)
    selected_days = int((pd.Timestamp(end).normalize() - pd.Timestamp(start).normalize()).days) + 1
    st.caption(recent_period_text(period, selected_days=selected_days, unit=catalog.gov.unit))
    if rows.empty:
        st.info("No data.gov.in rows match this state, city, station, and pollutant inside the selected dates.")
        return
    if period.kind == "empty":
        st.info(
            "Rows are stored for this selection, but none has a valid avg_value. "
            "Null averages were not replaced with zero, and missing dates were not filled."
        )
        return
    _recent_chart(period)
    chart_pollutant = pollutant if pollutant != ALL else _only_pollutant(rows)
    if chart_pollutant is None:
        st.caption("Choose one pollutant to map stations. A map of mixed pollutants would mix different API fields.")
        return
    current = at_timestamp(rows.loc[rows["pollutant_id"].eq(chart_pollutant)], latest_moment(rows))
    located = current
    if "coordinate_status" in located.columns:
        located = located.loc[located["coordinate_status"].eq("valid")]
    st.plotly_chart(
        reported_point_map(
            located,
            value="avg_value",
            title=_chart_title(
                f"{chart_pollutant} stations at the latest API timestamp in this period",
                start,
                end,
                int(summary["observations"]),
            ),
            value_title="API-reported average",
        ),
        width="stretch",
    )


def _recent_chart(period: object) -> None:
    """Draw one series per pollutant. One timestamp stays a point, not a line."""

    title = "Recent Air Quality Observations — data.gov.in"
    y_title = "Average reported value (API unit not documented)"
    if period.kind == "snapshot":
        st.info(
            "One observation timestamp is stored. "
            "There is insufficient temporal coverage for a trend. "
            "The chart shows that stored point only."
        )
    st.plotly_chart(
        recent_avg_series(period.points, title=title, y_title=y_title, markers_only=period.kind == "snapshot"),
        width="stretch",
    )
    if period.kind == "trend":
        st.caption(
            "Each point is the mean of station-reported avg_value at that observation timestamp. "
            "Each pollutant is its own series. Missing timestamps were not inserted."
        )


def _reported_unit(rows: pd.DataFrame) -> str:
    if "unit" not in rows.columns:
        return "µg/m³"
    units = [str(value) for value in rows["unit"].dropna().unique()]
    if len(units) == 1:
        return units[0]
    return "µg/m³"


def _with_live_pollutants(options: list[str]) -> list[str]:
    names = [item for item in options if item != ALL]
    for pollutant in ("PM2.5", "PM10"):
        if pollutant not in names:
            names.append(pollutant)
    return [ALL, *sorted(set(names))]


def _live_cards(rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    columns = st.columns(4)
    columns[0].metric("Station-day aggregate rows", _count(len(rows)))
    columns[1].metric("Stations", _count(rows["station_id"].nunique(dropna=True)))
    latest = period_start_days(rows["period_start"]).dropna()
    latest_text = "not available" if latest.empty else pd.Timestamp(latest.max()).strftime("%Y-%m-%d")
    columns[2].metric("Latest observation date", latest_text)
    total = pd.to_numeric(rows["n"], errors="coerce").sum()
    columns[3].metric("Hourly observations (sum of n)", "not available" if pd.isna(total) else _count(total))


def _api_table(rows: pd.DataFrame) -> pd.DataFrame:
    ordered = [column for column in ("station_id", "parameter_name", "unit", "period_start", "mean", "min", "max", "n") if column in rows.columns]
    extra = [column for column in rows.columns if column not in ordered]
    return rows.loc[:, ordered + extra]


def _cards(summary: dict[str, object], *, extra: tuple[str, str]) -> None:
    columns = st.columns(4)
    columns[0].metric("Observations", _count(summary["observations"]))
    columns[1].metric("Stations", _count(summary["stations"]))
    latest = summary["latest"]
    columns[2].metric("Latest timestamp", "not available" if latest is None else str(latest)[:16])
    columns[3].metric(extra[0], extra[1])
    st.caption(
        f"Source: {summary['source']}. Unit: {summary['unit']}. "
        f"Selected dates: {pd.Timestamp(summary['selected_start']).strftime('%Y-%m-%d')} to "
        f"{pd.Timestamp(summary['selected_end']).strftime('%Y-%m-%d')}. "
        f"Missing values in this filter: {_count(summary['missing_values'])}."
    )


def _spike_table(events: pd.DataFrame) -> pd.DataFrame:
    display = events.sort_values("collected_at").head(100).copy()
    return pd.DataFrame(
        {
            "Time": pd.to_datetime(display["collected_at"]).dt.strftime("%Y-%m-%d %H:%M"),
            "Station": _field(display, "station_id"),
            "Pollutant": _field(display, "parameter_name"),
            "Value": _field(display, "value"),
            "Unit": _field(display, "unit"),
        }
    )


def _chart_title(name: str, start: object, end: object, observations: int) -> str:
    return (
        f"{name} · {pd.Timestamp(start).strftime('%Y-%m-%d')} to "
        f"{pd.Timestamp(end).strftime('%Y-%m-%d')} · {observations:,} observations"
    )


def _place_label(state: str, city: str, station: str) -> str:
    if station != ALL:
        return f"Station: {station}"
    if city != ALL:
        return f"City: {city}"
    if state != ALL:
        return f"State: {state}"
    return "Place: all stored stations in this filter"


def _field(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([None] * len(frame), index=frame.index)


def _only_pollutant(frame: pd.DataFrame) -> str | None:
    if "pollutant_id" not in frame.columns:
        return None
    names = [str(value) for value in frame["pollutant_id"].dropna().unique()]
    if len(names) == 1:
        return names[0]
    return None


def _pair(value: object, today: date) -> tuple[date, date]:
    if isinstance(value, (tuple, list)):
        if len(value) == 2 and value[0] is not None and value[1] is not None:
            return value[0], value[1]
        if len(value) == 1 and value[0] is not None:
            return value[0], value[0]
    if isinstance(value, date):
        return value, value
    fallback = today - timedelta(days=6)
    return fallback, today


def _count(value: object) -> str:
    return f"{int(value):,}"
