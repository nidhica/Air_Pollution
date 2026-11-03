"""City cards and city filters for the saved analytics tables.

A city is a city name plus a state name from the saved aggregates. A missing
city stays missing. These functions do not read measurements and do not
invent a place name.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from dashboard.filters import filter_date_range, filter_pollutant, period_summary, unlocated_stations


def build_city_cards(stations: pd.DataFrame, city_metrics: pd.DataFrame) -> list[dict[str, Any]]:
    """One card per saved city and state, with station counts and pollutant means."""

    assigned = _assigned(stations)
    cards: list[dict[str, Any]] = []
    if city_metrics.empty:
        return cards
    grouped = city_metrics.groupby(["city_name", "state_name"], dropna=False, sort=False)
    for (city_name, state_name), part in grouped:
        if pd.isna(city_name):
            continue
        city_text = str(city_name)
        state_value = None if pd.isna(state_name) else str(state_name)
        station_rows = _match_place(assigned, city_text, state_value)
        pollutants = sorted(str(value) for value in part["parameter_name"].dropna().unique())
        means = {
            str(row.parameter_name): None if pd.isna(row.mean_valid_value) else float(row.mean_valid_value)
            for row in part.itertuples(index=False)
        }
        cards.append(
            {
                "city_name": city_text,
                "state_name": state_value,
                "stations": int(station_rows["station_id"].nunique()) if not station_rows.empty else 0,
                "stations_with_coordinates": int(
                    station_rows.loc[
                        station_rows["latitude"].notna() & station_rows["longitude"].notna(),
                        "station_id",
                    ].nunique()
                )
                if not station_rows.empty
                else 0,
                "pollutants": pollutants,
                "observations": int(part["observations"].sum()),
                "spike_count": int(part["spike_count"].sum()),
                "eligible_observations": int(part["eligible_observations"].sum()),
                "means": means,
            }
        )
    cards.sort(key=lambda item: (item["city_name"].casefold(), item["state_name"] or ""))
    return cards


def search_cards(cards: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """Keep cards whose city or state contains the query. Blank queries keep every card."""

    text = query.strip().casefold()
    if not text:
        return list(cards)
    found = []
    for card in cards:
        state = card["state_name"] or ""
        if text in f"{card['city_name']} {state}".casefold():
            found.append(card)
    return found


def unassigned_stations(stations: pd.DataFrame) -> pd.DataFrame:
    """Stations that have no single city name. Their city is not filled in."""

    missing = stations["city_name"].isna()
    conflict = stations["city_conflict"].fillna(False) if "city_conflict" in stations.columns else False
    return stations.loc[missing | conflict].copy()


def unassigned_summary(stations: pd.DataFrame) -> pd.DataFrame:
    """One row per station that cannot be placed in a city card."""

    rows = unassigned_stations(stations)
    if rows.empty:
        return pd.DataFrame(
            columns=[
                "station_id",
                "reason",
                "pollutants",
                "observations",
                "latitude",
                "longitude",
            ]
        )
    records = []
    for station_id, part in rows.groupby("station_id", dropna=False, sort=True):
        conflict = bool(part["city_conflict"].fillna(False).any()) if "city_conflict" in part.columns else False
        pollutants = sorted(str(value) for value in part["parameter_name"].dropna().unique())
        latitude = part["latitude"].dropna()
        longitude = part["longitude"].dropna()
        records.append(
            {
                "station_id": station_id,
                "reason": "conflicting city names" if conflict else "city not recorded",
                "pollutants": ", ".join(pollutants),
                "observations": int(part["observations"].sum()) if "observations" in part.columns else len(part),
                "latitude": None if latitude.empty else float(latitude.iloc[0]),
                "longitude": None if longitude.empty else float(longitude.iloc[0]),
            }
        )
    return pd.DataFrame(records)


def city_station_ids(stations: pd.DataFrame, city_name: str, state_name: str | None) -> list[str]:
    """Station IDs saved for this city. Stations without that city are excluded."""

    rows = _match_place(_assigned(stations), city_name, state_name)
    return sorted(str(value) for value in rows["station_id"].dropna().unique())


def filter_station_ids(frame: pd.DataFrame, station_ids: list[str] | set[str]) -> pd.DataFrame:
    """Keep rows whose station ID is in the city's saved station list."""

    if "station_id" not in frame.columns:
        return frame.iloc[0:0].copy()
    wanted = {str(value) for value in station_ids}
    return frame.loc[frame["station_id"].astype(str).isin(wanted)].copy()


def city_rows(frame: pd.DataFrame, city_name: str, state_name: str | None) -> pd.DataFrame:
    """Keep one city. Rows from every other city are removed."""

    return _match_place(frame, city_name, state_name)


def city_period(
    daily: pd.DataFrame,
    city_name: str,
    state_name: str | None,
    start: object,
    end: object,
    pollutant: str | None = None,
) -> pd.DataFrame:
    """City daily rows inside an inclusive date range, optionally for one pollutant."""

    rows = city_rows(daily, city_name, state_name)
    rows = filter_date_range(rows, "observation_date", start, end)
    return filter_pollutant(rows, pollutant)


def pollutant_summaries(daily_rows: pd.DataFrame) -> dict[str, dict[str, float | int | None]]:
    """Weighted summaries for each pollutant present in a city date slice."""

    if daily_rows.empty or "parameter_name" not in daily_rows.columns:
        return {}
    summaries = {}
    for pollutant in sorted(str(value) for value in daily_rows["parameter_name"].dropna().unique()):
        summaries[pollutant] = period_summary(filter_pollutant(daily_rows, pollutant))
    return summaries


def _assigned(stations: pd.DataFrame) -> pd.DataFrame:
    named = stations.loc[stations["city_name"].notna()].copy()
    if "city_conflict" in named.columns:
        named = named.loc[~named["city_conflict"].fillna(False)]
    return named


def _match_place(frame: pd.DataFrame, city_name: str, state_name: str | None) -> pd.DataFrame:
    if frame.empty or "city_name" not in frame.columns:
        return frame.iloc[0:0].copy()
    rows = frame.loc[frame["city_name"].eq(city_name)]
    if "state_name" not in rows.columns:
        return rows.copy()
    if state_name is None:
        return rows.loc[rows["state_name"].isna()].copy()
    return rows.loc[rows["state_name"].eq(state_name)].copy()
