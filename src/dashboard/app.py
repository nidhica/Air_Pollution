"""AEROSCOPE city portal for the saved CAAQM analytics.

Run from the repository root:

    .\\.venv\\Scripts\\python.exe -m streamlit run src/dashboard/app.py

Sign-in checks the demonstration hash in config/portal_users.json. City
trends come from data/processed/city_portal, not from the national daily
or hourly aggregates.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SOURCE = Path(__file__).resolve().parents[1]
if str(_SOURCE) not in sys.path:
    sys.path.insert(0, str(_SOURCE))

import pandas as pd
import streamlit as st

from dashboard.auth import (
    AuthenticationError,
    begin_session,
    clear_city,
    end_session,
    load_users,
    select_city,
    users_path,
    verify_credentials,
)
from dashboard.compare_page import render_city_comparison
from dashboard.cities import (
    build_city_cards,
    city_period,
    city_rows,
    city_station_ids,
    filter_station_ids,
    pollutant_summaries,
    search_cards,
    unassigned_summary,
)
from dashboard.figures import hour_bars, station_bars, station_map, time_series
from dashboard.filters import (
    comparison_columns,
    filter_date_range,
    filter_pollutant,
    located_stations,
    period_summary,
    pool_hours,
    top_by,
    unlocated_stations,
)
from dashboard.loading import (
    AnalyticsBundle,
    CityPortal,
    DashboardDataError,
    load_bundle,
    load_city_portal,
    repository_root,
)
from utils.config_loader import ConfigError, load_config

_COLOR_METRICS = {
    "Eligible spike rate": "spike_rate_of_eligible",
    "Mean concentration": "mean_valid_value",
}


def main() -> None:
    st.set_page_config(page_title="AEROSCOPE", layout="wide")
    _style()
    _ensure_state()
    if not st.session_state.authenticated:
        _login()
        return
    try:
        bundle = _cached_bundle(str(repository_root()))
        portal = _cached_portal(str(repository_root()))
    except DashboardDataError as exc:
        _brand()
        st.error(str(exc))
        _account_sidebar()
        return
    if st.session_state.get("portal_view") == "compare":
        _comparison_sidebar()
        _brand()
        render_city_comparison(bundle, portal)
        return
    if not st.session_state.city_selected:
        _cities(bundle)
        return
    _dashboard(bundle, portal)


@st.cache_data(show_spinner="Loading saved analytics")
def _cached_bundle(root_text: str) -> AnalyticsBundle:
    return load_bundle(Path(root_text))


@st.cache_data(show_spinner="Loading city series")
def _cached_portal(root_text: str) -> CityPortal:
    return load_city_portal(Path(root_text))


def _login() -> None:
    _brand()
    st.title("Air Pollution Analytics Portal")
    st.caption("City exploration for CPCB CAAQM PM2.5 and PM10 spike analytics.")
    left, center, right = st.columns([1, 1.15, 1])
    with center:
        with st.container(border=True):
            st.subheader("Sign in")
            st.markdown(
                "This sign-in is a **demonstration**. It is not production authentication. "
                "The password is checked against a PBKDF2-HMAC-SHA256 hash in `config/portal_users.json`. "
                "The signed-in email stays in this browser tab's Streamlit session only."
            )
            st.markdown(
                "Demonstration account  \n"
                "Email: `analyst@aeroscope.local`  \n"
                "Password: `aeroscope-demo`"
            )
            with st.form("login"):
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Sign in", type="primary", width="stretch")
            if submitted:
                _attempt_login(email, password)


def _attempt_login(email: str, password: str) -> None:
    try:
        users = load_users(users_path(repository_root()))
    except (AuthenticationError, OSError, ValueError) as exc:
        st.error(str(exc))
        return
    if verify_credentials(email, password, users):
        begin_session(st.session_state, email)
        st.rerun()
    st.error("The email or password is not valid.")


def _cities(bundle: AnalyticsBundle) -> None:
    _account_sidebar()
    _brand()
    st.title("Choose a city")
    st.caption(
        "Cities come from the saved station and city aggregates. "
        "A station without one city name is listed separately and is not assigned to a city."
    )
    cards = build_city_cards(bundle.stations, bundle.cities)
    query = st.text_input("Search cities", placeholder="Search by city or state")
    matched = search_cards(cards, query)
    st.markdown(f"**{len(matched)}** of **{len(cards)}** cities with a recorded city name.")
    if not matched:
        st.info("No recorded city matches that search.")
    else:
        columns = st.columns(3)
        for index, card in enumerate(matched):
            with columns[index % 3]:
                _city_card(card, index)
    unassigned = unassigned_summary(bundle.stations)
    st.subheader("Stations without a city")
    st.caption("These stations stay out of the city cards. Their missing city is not replaced.")
    if unassigned.empty:
        st.markdown("Every station in the saved table has one city name.")
    else:
        st.markdown(f"**{len(unassigned)}** stations have no single recorded city.")
        st.dataframe(unassigned, hide_index=True, width="stretch")


def _city_card(card: dict[str, object], index: int) -> None:
    state = card["state_name"] or "State not recorded"
    pollutants = ", ".join(card["pollutants"]) if card["pollutants"] else "No pollutant series"
    with st.container(border=True):
        st.caption(str(state))
        st.markdown(f"**{card['city_name']}**")
        st.markdown(
            f"{_format_count(card['stations'])} monitoring stations  \n"
            f"Pollutants: {pollutants}  \n"
            f"{_format_count(card['observations'])} observations"
        )
        if st.button("Open city", key=f"open-{index}-{card['city_name']}-{card['state_name']}", width="stretch"):
            select_city(st.session_state, str(card["city_name"]), card["state_name"])
            st.rerun()


def _dashboard(bundle: AnalyticsBundle, portal: CityPortal) -> None:
    city_name = str(st.session_state.city_name)
    state_name = st.session_state.state_name
    state_label = state_name or "State not recorded"
    station_ids = city_station_ids(bundle.stations, city_name, state_name)
    daily = city_rows(portal.daily, city_name, state_name)
    selection = _city_sidebar(bundle, daily, city_name, state_label)
    _brand()
    st.title(city_name)
    st.caption(f"{state_label}. Station charts and the map use this city's saved station IDs.")
    if daily.empty:
        st.warning("This city has no dated city-series rows.")
        return
    selected_daily = city_period(portal.daily, city_name, state_name, selection["start"], selection["end"])
    _summary(selected_daily, station_ids, selection)
    temporal, stations, geographic, quality = st.tabs(
        ["Temporal", "Stations", "Geographic", "Data quality"]
    )
    with temporal:
        _temporal(portal, city_name, state_name, selection)
    with stations:
        _stations(bundle, station_ids, selection)
    with geographic:
        _geographic(bundle, station_ids, selection)
    with quality:
        _quality(bundle, portal, city_name, state_name, station_ids, selection)


def _summary(selected_daily: pd.DataFrame, station_ids: list[str], selection: dict[str, object]) -> None:
    summaries = pollutant_summaries(selected_daily)
    combined = period_summary(selected_daily)
    top = st.columns(3)
    bottom = st.columns(3)
    top[0].metric("PM2.5 average", _metric_mean(summaries.get("PM2.5")))
    top[1].metric("PM10 average", _metric_mean(summaries.get("PM10")))
    top[2].metric("Monitoring stations", _format_count(len(station_ids)))
    bottom[0].metric("Observations", _format_count(combined["observations"]))
    bottom[1].metric("Candidate spikes", _format_count(combined["spike_count"]))
    bottom[2].metric("Eligible spike rate", _format_rate(combined["spike_rate_of_eligible"]))
    st.caption(
        "Averages, observations, candidate spikes, and the eligible spike rate follow the selected dates. "
        "The station count is the number of saved station IDs for this city over the full analyzed window. "
        f"Selected dates: {selection['start']} to {selection['end']}."
    )


def _temporal(
    portal: CityPortal,
    city_name: str,
    state_name: str | None,
    selection: dict[str, object],
) -> None:
    daily = city_period(portal.daily, city_name, state_name, selection["start"], selection["end"])
    st.caption(
        "These series are aggregated from the annotated measurements for this city's stations. "
        "The national daily and hourly tables are not used here."
    )
    st.plotly_chart(
        time_series(daily, y="mean_valid_value", title="Daily average concentration", y_title="Mean valid value"),
        width="stretch",
    )
    st.plotly_chart(
        time_series(daily, y="spike_count", title="Daily candidate spikes", y_title="Candidate spikes"),
        width="stretch",
    )
    st.plotly_chart(
        time_series(
            daily,
            y="spike_rate_of_eligible",
            title="Daily eligible spike rate",
            y_title="Spikes / eligible observations",
        ),
        width="stretch",
    )
    hours = _city_hours(portal, city_name, state_name, selection)
    st.plotly_chart(
        hour_bars(hours, y="mean_valid_value", title="Hour-of-day average concentration", y_title="Mean valid value"),
        width="stretch",
    )
    st.plotly_chart(
        hour_bars(
            hours,
            y="spike_rate_of_eligible",
            title="Hour-of-day eligible spike rate",
            y_title="Spikes / eligible observations",
        ),
        width="stretch",
    )


def _stations(bundle: AnalyticsBundle, station_ids: list[str], selection: dict[str, object]) -> None:
    pollutant = str(selection["pollutant"])
    rows = filter_pollutant(filter_station_ids(bundle.stations, station_ids), pollutant)
    st.caption(
        f"{pollutant} rankings use the stored station aggregates for this city's station IDs. "
        "Spike flags are not recalculated. Ranks below are the order inside this city."
    )
    if rows.empty:
        st.info(f"No {pollutant} station series is saved for this city.")
        return
    _rank_chart(rows, "spike_count", f"Highest {pollutant} candidate spike counts", "Candidate spikes")
    _rank_chart(rows, "spike_rate_of_eligible", f"Highest {pollutant} eligible spike rates", "Eligible spike rate")
    _rank_chart(rows, "mean_valid_value", f"Highest {pollutant} mean concentrations", "Mean valid value")
    st.subheader("Denominators")
    st.dataframe(_station_table(rows), hide_index=True, width="stretch")
    st.subheader("Station comparison")
    comparison = filter_station_ids(bundle.comparison, station_ids)
    if comparison.empty:
        st.info("No comparison row is saved for these station IDs.")
        return
    choice = st.selectbox("Station", sorted(comparison["station_id"].astype(str).unique()))
    _comparison(comparison, choice)
    events = filter_station_ids(bundle.spike_events, [choice])
    events = filter_pollutant(events, pollutant)
    events = filter_date_range(events, "collected_at", selection["start"], selection["end"])
    st.markdown(
        f"**{_format_count(len(events))}** stored candidate spike rows for this station, pollutant, and date range."
    )
    if not events.empty:
        st.dataframe(_event_table(events), hide_index=True, width="stretch")


def _geographic(bundle: AnalyticsBundle, station_ids: list[str], selection: dict[str, object]) -> None:
    pollutant = str(selection["pollutant"])
    metric_label = st.radio("Map colour", list(_COLOR_METRICS), horizontal=True)
    metric = _COLOR_METRICS[str(metric_label)]
    rows = filter_pollutant(filter_station_ids(bundle.stations, station_ids), pollutant)
    located = located_stations(rows)
    missing = unlocated_stations(rows)
    event = st.plotly_chart(
        station_map(
            located,
            color=metric,
            color_title=str(metric_label),
            title=f"{pollutant} stations",
        ),
        width="stretch",
        on_select="rerun",
        selection_mode="points",
        key=f"city-map-{pollutant}-{metric}",
    )
    st.caption(
        f"{_format_count(located['station_id'].nunique()) if not located.empty else 0} "
        f"{pollutant} stations have coordinates. "
        "Stations without a latitude or longitude are listed below and are not given a point on the map."
    )
    picked = _point_station(event)
    if picked is None:
        picked = st.session_state.get("selected_station")
    else:
        st.session_state.selected_station = picked
    if isinstance(picked, str) and picked in set(station_ids):
        _comparison(filter_station_ids(bundle.comparison, station_ids), picked)
    if missing.empty:
        st.markdown("Every station series in this city and pollutant has coordinates.")
    else:
        st.subheader("Stations without coordinates")
        st.dataframe(_station_table(missing), hide_index=True, width="stretch")


def _quality(
    bundle: AnalyticsBundle,
    portal: CityPortal,
    city_name: str,
    state_name: str | None,
    station_ids: list[str],
    selection: dict[str, object],
) -> None:
    city_stations = filter_station_ids(bundle.stations, station_ids)
    missing_coordinates = unlocated_stations(city_stations)
    full = period_summary(city_rows(portal.daily, city_name, state_name))
    selected = period_summary(city_period(portal.daily, city_name, state_name, selection["start"], selection["end"]))
    unassigned = unassigned_summary(bundle.stations)
    coverage = bundle.report["coverage"]
    left, right = st.columns(2)
    with left:
        st.subheader("This city")
        st.markdown(
            f"- Station series without coordinates: **{_format_count(len(missing_coordinates))}**\n"
            f"- Invalid measurements in the dated city series: **{_format_count(_gap(full, 'observations', 'valid_observations'))}**\n"
            f"- Dated observations without an established baseline: **{_format_count(_gap(full, 'observations', 'eligible_observations'))}**\n"
            f"- Eligible observations: **{_format_count(full['eligible_observations'])}**\n"
            f"- Selected-date observations without a baseline: **{_format_count(_gap(selected, 'observations', 'eligible_observations'))}**"
        )
    with right:
        st.subheader("Outside every city")
        st.markdown(
            f"- Stations with no single city name: **{_format_count(len(unassigned))}**\n"
            f"- Saved stations without a city: **{_format_count(coverage['stations_without_city'])}**\n"
            f"- Saved stations without a state: **{_format_count(coverage['stations_without_state'])}**\n"
            f"- Saved stations without coordinates: **{_format_count(coverage['stations_without_coordinates'])}**\n"
            f"- Rows without a city: **{_format_count(coverage['rows_without_city'])}**"
        )
    st.caption(str(coverage["note"]))
    st.subheader("Count and rate")
    st.markdown(
        "The candidate spike count is the number of stored spike flags. "
        "The eligible spike rate divides that count by the eligible observations. "
        "A short series can have a high rate and a small count. "
        "A long series can have a high count and a moderate rate."
    )
    _method(bundle)


def _method(bundle: AnalyticsBundle) -> None:
    parameters = bundle.report["parameters"]
    settings = _spike_settings()
    window = settings[0] if settings is not None else "the configured"
    sigmas = settings[1] if settings is not None else "the configured"
    st.subheader("How a candidate spike is defined")
    st.markdown(
        f"Each station and pollutant was scored before these tables were saved. "
        f"Rows are ordered by `collected_at`. The baseline is the previous {window} valid observations, "
        f"excluding the current observation. A row is eligible when it is valid, its timestamp order is defined, "
        f"and at least {parameters['minimum_baseline']} earlier valid observations are available. "
        f"It is a candidate spike when its value is strictly greater than the baseline sample mean plus "
        f"{sigmas} sample standard deviations. The stored eligibility rule is: {parameters['eligible_rule']}. "
        "The portal does not rescore measurements."
    )
    st.markdown(
        "National unusual-date flags are not copied onto the city series. "
        "Those flags compare a date with every station in the country, so they are not a city trend."
    )


def _city_sidebar(
    bundle: AnalyticsBundle,
    daily: pd.DataFrame,
    city_name: str,
    state_label: str,
) -> dict[str, object]:
    _account_sidebar()
    if st.sidebar.button("Back to cities", width="stretch"):
        clear_city(st.session_state)
        st.rerun()
    st.sidebar.markdown(f"**{city_name}**  \n{state_label}")
    start_default = pd.Timestamp(daily["observation_date"].min()).date()
    end_default = pd.Timestamp(daily["observation_date"].max()).date()
    chosen = st.sidebar.date_input(
        "Date range",
        value=(start_default, end_default),
        min_value=start_default,
        max_value=end_default,
        key=f"dates-{city_name}-{state_label}",
    )
    start, end = _date_pair(chosen, start_default, end_default)
    pollutant = st.sidebar.radio("Pollutant", ["PM2.5", "PM10"], horizontal=True, key=f"pollutant-{city_name}")
    st.sidebar.caption(
        "The date range filters this city's daily and hourly series and the summary cards. "
        "Station rankings and the map use the full window stored for each station. "
        "The pollutant choice applies to stations and the map. Daily charts show PM2.5 and PM10 together."
    )
    st.sidebar.caption(f"Analytics report {bundle.report['generated_at']}.")
    return {
        "pollutant": pollutant,
        "start": start,
        "end": end,
        "full_start": start_default,
        "full_end": end_default,
    }


def _comparison_sidebar() -> None:
    _account_sidebar(show_compare=False)
    if st.session_state.city_selected:
        if st.sidebar.button(f"Return to {st.session_state.city_name}", width="stretch"):
            st.session_state.portal_view = "explore"
            st.rerun()
    if st.sidebar.button("City list", width="stretch"):
        clear_city(st.session_state)
        st.session_state.portal_view = "explore"
        st.rerun()


def _account_sidebar(*, show_compare: bool = True) -> None:
    st.sidebar.markdown("**AEROSCOPE**")
    email = st.session_state.user_email or ""
    if email:
        st.sidebar.caption(email)
    if show_compare and st.sidebar.button("City comparison", width="stretch"):
        st.session_state.portal_view = "compare"
        st.rerun()
    if st.sidebar.button("Log out", width="stretch"):
        end_session(st.session_state)
        st.rerun()


def _city_hours(
    portal: CityPortal,
    city_name: str,
    state_name: str | None,
    selection: dict[str, object],
) -> pd.DataFrame:
    if selection["start"] == selection["full_start"] and selection["end"] == selection["full_end"]:
        return city_rows(portal.hour_of_day, city_name, state_name)
    hourly = city_rows(portal.hourly, city_name, state_name)
    hourly = filter_date_range(hourly, "observation_date", selection["start"], selection["end"])
    return pool_hours(hourly)


def _rank_chart(frame: pd.DataFrame, column: str, title: str, y_title: str) -> None:
    st.plotly_chart(
        station_bars(top_by(frame, column, limit=12), y=column, title=title, y_title=y_title),
        width="stretch",
    )


def _comparison(comparison: pd.DataFrame, station_id: str) -> None:
    rows = comparison.loc[comparison["station_id"].astype(str).eq(station_id)]
    if rows.empty:
        st.info("This station is not in the saved comparison table.")
        return
    row = rows.iloc[0]
    columns = st.columns(2)
    for column, pollutant in zip(columns, ("PM2.5", "PM10")):
        fields = comparison_columns(pollutant)
        mean = row[fields[0]] if fields[0] in row.index else None
        if mean is None or pd.isna(mean):
            column.markdown(f"**{pollutant}**  \nNo series in the saved comparison.")
            continue
        column.markdown(
            f"**{pollutant}**  \n"
            f"Mean {_format_number(mean)}  \n"
            f"Valid observations {_format_count(row[fields[1]])}  \n"
            f"Spikes {_format_count(row[fields[2]])}  \n"
            f"Eligible observations {_format_count(row[fields[3]])}  \n"
            f"Eligible spike rate {_format_rate(row[fields[4]])}"
        )


def _station_table(frame: pd.DataFrame) -> pd.DataFrame:
    view = frame.sort_values(["spike_count", "station_id"], ascending=[False, True]).copy()
    return pd.DataFrame(
        {
            "Station": view["station_id"],
            "Observations": view["observations"].astype(int),
            "Valid observations": view["valid_observations"].astype(int),
            "Mean": view["mean_valid_value"].map(_format_number),
            "Candidate spikes": view["spike_count"].astype(int),
            "Eligible observations": view["eligible_observations"].astype(int),
            "Eligible spike rate": view["spike_rate_of_eligible"].map(_format_rate),
        }
    )


def _event_table(events: pd.DataFrame) -> pd.DataFrame:
    display = events.sort_values("collected_at").copy()
    display["Time"] = pd.to_datetime(display["collected_at"]).dt.strftime("%Y-%m-%d %H:%M")
    return display.rename(
        columns={
            "value": "Value",
            "baseline_count": "Baseline count",
            "rolling_mean": "Baseline mean",
            "rolling_stddev": "Baseline standard deviation",
            "spike_threshold": "Threshold",
        }
    )[
        [
            "Time",
            "Value",
            "Baseline count",
            "Baseline mean",
            "Baseline standard deviation",
            "Threshold",
        ]
    ]


def _point_station(event: object) -> str | None:
    if event is None:
        return None
    selection = event["selection"] if isinstance(event, dict) else getattr(event, "selection", None)
    if selection is None:
        return None
    points = selection["points"] if isinstance(selection, dict) else getattr(selection, "points", [])
    if not points:
        return None
    point = points[0]
    custom = point["customdata"] if isinstance(point, dict) else getattr(point, "customdata", None)
    if isinstance(custom, (list, tuple)) and custom:
        return str(custom[0])
    return None


def _metric_mean(summary: dict[str, float | int | None] | None) -> str:
    if summary is None:
        return "No series"
    return _format_number(summary["mean_valid_value"])


def _gap(summary: dict[str, float | int | None], whole: str, part: str) -> int:
    return int(summary[whole] or 0) - int(summary[part] or 0)


def _date_pair(value: object, start_default: object, end_default: object) -> tuple[object, object]:
    if isinstance(value, (tuple, list)):
        if len(value) == 2 and value[0] is not None and value[1] is not None:
            return value[0], value[1]
        if len(value) == 1 and value[0] is not None:
            return value[0], value[0]
    return start_default, end_default


def _spike_settings() -> tuple[int, float] | None:
    try:
        settings = load_config().spikes
    except ConfigError:
        return None
    return settings.baseline_window, settings.threshold_sigmas


def _format_count(value: object) -> str:
    return f"{int(value):,}"


def _format_number(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{float(value):,.2f}"


def _format_rate(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{float(value):.2%}"


def _ensure_state() -> None:
    defaults = {
        "authenticated": False,
        "user_email": None,
        "city_selected": False,
        "city_name": None,
        "state_name": None,
        "portal_view": "explore",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _brand() -> None:
    st.markdown('<p class="aero-brand">AEROSCOPE</p>', unsafe_allow_html=True)


def _style() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.1rem; padding-bottom: 2rem;}
        .aero-brand {letter-spacing: 0.22em; color: #7FDBDA; font-weight: 700; margin-bottom: 0;}
        div[data-testid="stMetric"] {
            background: #123047;
            border: 1px solid #1E4D63;
            border-radius: 14px;
            padding: 0.75rem 0.9rem;
        }
        div[data-testid="stMetricValue"] {font-variant-numeric: tabular-nums;}
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
